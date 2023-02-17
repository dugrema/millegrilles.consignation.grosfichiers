const debug = require('debug')('backupSftp')
const fs = require('fs')
const path = require('path')
const { exec } = require('child_process')
const readline = require('readline')
const axios = require('axios')
const https = require('https')

const SftpDao = require('./sftpDao')

const INTERVALLE_BACKUP = 8 * 3_600_000

const _httpsAgent = new https.Agent({
    rejectUnauthorized: false,
})

function BackupSftp(mq, storeConsignation) {
    this.amqpdao = mq
    this.storeConsignation = storeConsignation

    this.queueItems = new Set()
    this.timerBackup = null
    this.backupEnCours = false

    this.sftpDao = new SftpDao()
}

BackupSftp.prototype.threadBackup = async function() {
    try {
        debug("Run threadBackup")
        if(this.timerBackup) clearTimeout(this.timerBackup)
        this.timerBackup = null
        this.backupEnCours = new Date()

        const configuration = await this.storeConsignation.chargerConfiguration()
        if(configuration.type_backup === 'sftp') {
            // Run backup
            await this.sftpDao.init(configuration)
            await this.runBackup(configuration)
        } else {
            debug("Type backup !== sftp, pas de backup")
        }

    } catch(err) {
        console.error(new Date() + ' TransfertPrimaire.threadBackup Erreur execution cycle : %O', err)
    } finally {
        this.backupEnCours = false
        debug("threadBackup Fin execution cycle, attente %s ms", INTERVALLE_BACKUP)
        // Redemarrer apres intervalle
        this.setTimeout()
        this.sftpDao.fermer()
            .catch(err=>console.error("Erreur fermeture sftpDao pour backup ", err))
    }

}

BackupSftp.prototype.setTimeout = function(delai) {
    const attente = delai || INTERVALLE_BACKUP
    if(this.timerBackup) clearTimeout(this.timerBackup)
    this.timerBackup = setTimeout(()=>{
        this.timerBackup = null
        this.threadBackup()
            .catch(err=>console.error(new Date() + " BackupSftp Erreur run threadBackup: %O", err))
    }, attente)
}

BackupSftp.prototype.runBackup = async function(configuration) {
    debug("runBackup Debut")

    // S'assurer que le path remote existe
    await this.sftpDao.mkdir(configuration.remote_path_sftp_backup)

    // Detecter differences dans les listes de fichier
    await this.genererListeFuuids(configuration)

    // Transferer fichiers
    await this.transfererFuuids(configuration)

    debug("runBackup Fin backup")
}

BackupSftp.prototype.genererListeFuuids = async function(configuration) {
    const pathStaging = this.storeConsignation.getPathStaging()
    const pathFichiers = path.join(pathStaging, 'liste')
    const fuuidsBackupMissing = path.join(pathFichiers, '/fuuidsBackupMissing.txt')

    {
        const fuuidsBackup = path.join(pathFichiers, '/fuuidsBackup.txt')
        const fuuidsActifs = path.join(pathFichiers, '/fuuidsActifs.txt')
        const fuuidsBackupWork = path.join(pathFichiers, '/fuuidsBackup.txt.work')

        const writeFuuidsBackupStream = fs.createWriteStream(fuuidsBackupWork)
        const fuuidsBackupCb = info => {
            debug("Fuuids backup present info ", info)
            writeFuuidsBackupStream.write(info.filename + '\n')
        }
        const pathRemoteBackup = path.join(configuration.remote_path_sftp_backup, 'consignation')
        await this.sftpDao.parcourirFichiersRecursif(pathRemoteBackup, fuuidsBackupCb)
        writeFuuidsBackupStream.close()

        await new Promise((resolve, reject)=>{
            exec(`sort -o ${fuuidsBackup} ${fuuidsBackupWork}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })

        await new Promise((resolve, reject)=>{
            exec(`comm -3 ${fuuidsActifs} ${fuuidsBackup} > ${fuuidsBackupMissing}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })

        const readStreamFichiers = fs.createReadStream(fuuidsBackupMissing)
        const rlFichiers = readline.createInterface({input: readStreamFichiers, crlfDelay: Infinity})
        for await (const line of rlFichiers) {
            // Detecter fichiers manquants localement par espaces vide au debut de la ligne
            if( ! (line.startsWith('	') || line.startsWith('\t')) ) {
                // Manquant
                const fuuid = line.trim()
                debug('Transferer fuuid manquant du backup ', fuuid)
                this.queueItems.add(fuuid)
            }
        }
    
    }
}

BackupSftp.prototype.transfererFuuids = async function(configuration) {

    const pathSftpRemote = configuration.remote_path_sftp_backup,
          pathConsignationRemote = path.join(pathSftpRemote, 'consignation'),
          pathWorkRemote = path.join(pathSftpRemote, 'work')

    await this.sftpDao.mkdir(pathConsignationRemote)
    await this.sftpDao.mkdir(pathWorkRemote)

    while(true) {
        // Recuperer un fuuid a partir du Set
        let fuuid = null
        for(fuuid of this.queueItems.values()) break
        if(!fuuid) break
        this.queueItems.delete(fuuid)

        try {
            await this.uploadFuuidBackup(pathConsignationRemote, pathWorkRemote, fuuid)
        } catch(err) {
            console.error(new Date() + " backupSftp.transfererFuuids Erreur upload %s vers backup %O", fuuid, err)
        }
    }
}

BackupSftp.prototype.uploadFuuidBackup = async function(pathConsignationRemote, pathWorkRemote, fuuid) {
    const infoFichier = await this.storeConsignation.getInfoFichier(fuuid)
    debug("uploadFuuidBackup ", infoFichier)
    const pathFichierWorkRemote = path.join(pathWorkRemote, fuuid),
          pathFichierRemote = path.join(pathConsignationRemote, fuuid)

    let stream = null
    if(infoFichier.fileRedirect) {
        const reponseFichier = await axios({
            method: 'GET', 
            url: infoFichier.fileRedirect, 
            responseType: 'stream',
            httpsAgent: _httpsAgent,
        })
        if(reponseFichier.status !== 200) throw new Error(`Erreur axios fuuid ${fuuid}, status ${reponseFichier.status}`)
        stream = reponseFichier.data
    } else {
        // Acces direct local
        const pathFichierLocal = path.join(infoFichier.directory, infoFichier.name)
        debug("Path fichier local pour upload backup ", pathFichierLocal)
        throw new Error('not implemented')
    }

    await this.sftpDao.writeFileStream(pathFichierWorkRemote, stream)
    await this.sftpDao.rename(pathFichierWorkRemote, pathFichierRemote)
}

module.exports = BackupSftp
