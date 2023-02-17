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
    this.fichiersBackupItem = new Set()
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
            // Mapper champs sftp pour reutiliser modules
            const configurationCustom = {
                ...configuration,
                hostname_sftp: configuration.hostname_sftp_backup, 
                username_sftp: configuration.username_sftp_backup, 
                port_sftp: configuration.port_sftp_backup, 
                key_type_sftp: configuration.key_type_sftp_backup,
            }

            // Run backup
            await this.sftpDao.init(configurationCustom)
            await this.runBackup(configurationCustom)
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

    // Detecter differences dans les listes de fichier et transferer
    await this.genererListeFuuids(configuration)
    await this.transfererFuuids(configuration)

    // Detecter differences dans liste de backups et transferer
    await this.genererListeBackups(configuration)
    await this.transfererFichiersBackup(configuration)

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
        const pathRemoteConsignation = path.join(configuration.remote_path_sftp_backup, 'consignation')
        await this.sftpDao.mkdir(pathRemoteConsignation)
        await this.sftpDao.parcourirFichiersRecursif(pathRemoteConsignation, fuuidsBackupCb)
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

    debug("transfererFuuids Fin")
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
        // const pathFichierLocal = path.join(infoFichier.directory, infoFichier.name)
        const pathFichierLocal = infoFichier.filePath
        debug("Path fichier local pour upload backup ", pathFichierLocal)
        stream = fs.createReadStream(pathFichierLocal)
    }

    await this.sftpDao.writeFileStream(pathFichierWorkRemote, stream)
    await this.sftpDao.rename(pathFichierWorkRemote, pathFichierRemote)
}

BackupSftp.prototype.genererListeBackups = async function(configuration) {
    const pathStaging = this.storeConsignation.getPathStaging()
    const pathFichiers = path.join(pathStaging, 'liste')
    const fichiersBackupMissing = path.join(pathFichiers, '/fichiersBackupMissing.txt')

    {
        const fichiersBackupLocal = path.join(pathFichiers, '/fichiersBackupLocal.txt')
        const fichiersBackupLocalWork = path.join(pathFichiers, '/fichiersBackupLocal.txt.work')
        const fichiersBackupSftp = path.join(pathFichiers, '/fichiersBackupSftp.txt')
        const fichiersBackupSftpWork = path.join(pathFichiers, '/fichiersBackupSftp.txt.work')

        const pathRemoteBackup = path.join(configuration.remote_path_sftp_backup, 'backup'),
              pathRemoteTransactions = path.join(pathRemoteBackup, 'transactions')

        debug("Creation repertoires remote %s", pathRemoteTransactions)
        await this.sftpDao.mkdir(pathRemoteBackup)
        await this.sftpDao.mkdir(pathRemoteTransactions)

        // Conserver backups locaux
        const writeStreamBackupLocal = fs.createWriteStream(fichiersBackupLocalWork)
        const cbBackupLocal = info => {
            if(!info) return  // Callback vide (generalement dernier)
            debug("genererListeBackups Fichier backup local ", info)
            const domaine = info.directory.split('/').pop()
            writeStreamBackupLocal.write(path.join(domaine, info.filename) + '\n')
        }
        await this.storeConsignation.parcourirBackup(cbBackupLocal)

        const writeFuuidsBackupStream = fs.createWriteStream(fichiersBackupSftpWork)
        const fichierBackupCb = info => {
            debug("Fichiers backup present info ", info)
            const domaine = info.directory.split('/').pop()
            writeFuuidsBackupStream.write(path.join(domaine, info.filename) + '\n')
        }

        await this.sftpDao.parcourirFichiersRecursif(pathRemoteTransactions, fichierBackupCb)
        writeFuuidsBackupStream.close()

        await new Promise((resolve, reject)=>{
            exec(`sort -o ${fichiersBackupSftp} ${fichiersBackupSftpWork}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })

        await new Promise((resolve, reject)=>{
            exec(`sort -o ${fichiersBackupLocal} ${fichiersBackupLocalWork}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })

        await new Promise((resolve, reject)=>{
            exec(`comm -3 ${fichiersBackupLocal} ${fichiersBackupSftp} > ${fichiersBackupMissing}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })

        const readStreamFichiers = fs.createReadStream(fichiersBackupMissing)
        const rlFichiers = readline.createInterface({input: readStreamFichiers, crlfDelay: Infinity})
        for await (const line of rlFichiers) {
            // Detecter fichiers manquants localement par espaces vide au debut de la ligne
            if( ! (line.startsWith('	') || line.startsWith('\t')) ) {
                // Manquant
                const fuuid = line.trim()
                debug('Transferer fichier transactions manquant du backup ', fuuid)
                this.fichiersBackupItem.add(fuuid)
            }
        }
    
    }    
}

BackupSftp.prototype.transfererFichiersBackup = async function(configuration) {

    const pathSftpRemote = configuration.remote_path_sftp_backup,
          pathBackup = path.join(pathSftpRemote, 'backup'),
          pathBackupTransactions = path.join(pathBackup, 'transactions'),
          pathWorkRemote = path.join(pathSftpRemote, 'work')

    await this.sftpDao.mkdir(pathBackupTransactions)
    await this.sftpDao.mkdir(pathWorkRemote)

    while(true) {
        // Recuperer un fuuid a partir du Set
        let fichier = null
        for(fichier of this.fichiersBackupItem.values()) break
        if(!fichier) break
        this.fichiersBackupItem.delete(fichier)

        try {
            debug("transfererFichiersBackup ", fichier)
            await this.uploadFicherBackup(fichier, pathBackupTransactions, pathWorkRemote)
        } catch(err) {
            console.error(new Date() + " backupSftp.transfererFichiersBackup Erreur upload %s vers backup %O", fichier, err)
        }
    }

    debug("transfererFichiersBackup Fin")
}

BackupSftp.prototype.uploadFicherBackup = async function(fichierBackup, pathBackupTransactions, pathWorkRemote) {
    const fichierParsed = path.parse(fichierBackup)
    debug('uploadFuuidBackup fichierBackup %s, parsed %O', fichierBackup, fichierParsed)
    const pathWorkFichier = path.join(pathWorkRemote, fichierParsed.base)
    const domaine = fichierParsed.dir
    const pathDestinationDomaine = path.join(pathBackupTransactions, domaine)
    const pathDestinationFichier = path.join(pathDestinationDomaine, fichierParsed.base)

    debug("Creer dir domaine ", pathDestinationDomaine)
    await this.sftpDao.mkdir(pathDestinationDomaine)

    debug("Transfert fichier backup vers de %O vers %O", fichierBackup, pathWorkFichier)
    const streamFichier = await this.storeConsignation.getBackupTransactionStream(fichierBackup)

    await this.sftpDao.writeFileStream(pathWorkFichier, streamFichier)

    debug("Rename fichier backup vers de %O vers %O", pathWorkFichier, pathDestinationFichier)
    await this.sftpDao.rename(pathWorkFichier, pathDestinationFichier)
}

module.exports = BackupSftp
