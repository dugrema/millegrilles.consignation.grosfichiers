const debugLib = require('debug')
const debug = debugLib('consignation:store:sftp')
const path = require('path')
const readdirp = require('readdirp')
const { Readable } = require('stream')
const lzma = require('lzma-native')

const { Hacheur, VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')
const { chargerConfiguration, modifierConfiguration } = require('./storeConsignationLocal')
const SftpDao = require('./sftpDao')

const CONSIGNATION_PATH = process.env.MG_CONSIGNATION_PATH || '/var/opt/millegrilles/consignation'
const PATH_BACKUP_TRANSACTIONS_DIR = path.join(CONSIGNATION_PATH, 'backup', 'transactions')

const CONST_EXPIRATION_ORPHELINS = 86_400_000 * 3

const _sftpDao = new SftpDao()

let _urlDownload = null,
    _remotePath = '.'

async function init(params) {
    params = params || {}

    // const {hostname_sftp, username_sftp, url_download, port_sftp, remote_path_sftp, key_type_sftp} = params

    const { remote_path_sftp, url_download } = params
    if(!url_download) throw new Error("Parametre urlDownload manquant")

    _urlDownload = new URL(''+url_download).href
    _remotePath = remote_path_sftp || _remotePath

    _sftpDao.fermer()  // Si connexion deja ouverte (config change)
    return _sftpDao.init(params)
        .then(()=>{
            _sftpDao.mkdir(getPathWork())
            _sftpDao.mkdir(getRemotePathFichiers())
            _sftpDao.mkdir(getRemotePathOrphelins())
            _sftpDao.mkdir(getRemotePathArchives())
        })
}

function fermer() {
    return _sftpDao.fermer()
}

function getPathFichier(fuuid) {
    // Split path en 2 derniers caracteres
    const pathConsignation = getRemotePathFichiers()
    const last2 = fuuid.slice(fuuid.length-2)
    return path.join(pathConsignation, last2, fuuid)
}

function getPathFichierOrphelins(fuuid) {
    // Split path en 2 derniers caracteres
    const pathOrphelins = getRemotePathOrphelins()
    const last2 = fuuid.slice(fuuid.length-2)
    return path.join(pathOrphelins, last2, fuuid)
}

function getPathFichierArchives(fuuid) {
    const pathArchives = getRemotePathArchives()
    const last2 = fuuid.slice(fuuid.length-2)
    return path.join(pathArchives, last2, fuuid)
}

// function getPathFichier(fuuid) {
//     // Split path en 4 derniers caracteres
//     const last2 = fuuid.slice(fuuid.length-2)
//     // const sub1 = last2[1],
//     //       sub2 = last2[0]
//     const pathFichiers = getRemotePathFichiers()
//     return path.join(pathFichiers, last2, fuuid)
// }

// function getPathFichierCorbeille(fuuid) {
//     const last2 = fuuid.slice(fuuid.length-2)
//     // const sub1 = last2[1],
//     //       sub2 = last2[0]
//     const pathCorbeille = getRemotePathCorbeille()
//     return path.join(pathCorbeille, last2, fuuid)
// }

function getPathWork(fuuid) {
    if(fuuid) {
        return path.join(_remotePath, 'work', fuuid)
    } else {
        return path.join(_remotePath, 'work')
    }
}

async function getInfoFichier(fuuid) {
    debug("getInfoFichier %s", fuuid)
    const url = new URL(_urlDownload)
    const last2 = fuuid.slice(fuuid.length-2)
    url.pathname = path.join(url.pathname, last2, fuuid)
    const fileRedirect = url.href
    debug("File redirect : %O", fileRedirect)
    return { fileRedirect }
}

async function reactiverFichier(fuuid) {
    debug("reactiverFichier %s", fuuid)
    const filePath = getPathFichier(fuuid)
    const filePathDir = path.dir(filePath)
    await _sftpDao.mkdir(filePathDir)
    try {
        const filePathCorbeille = getPathFichierCorbeille(fuuid)
        await _sftpDao.stat(filePathCorbeille)
        await _sftpDao.rename(filePathCorbeille, filePath)
        const statInfo = await _sftpDao.stat(filePath)
        return { stat: statInfo, filePath }
    } catch(err) {
        try {
            const filePathArchives = getPathFichierArchives(fuuid)
            await _sftpDao.stat(filePathArchives)
            await _sftpDao.rename(filePathArchives, filePath)
            const statInfo = await _sftpDao.stat(filePath)
            return { stat: statInfo, filePath }
        } catch(err) {
            debug("Erreur reactiverFichier %s : %O", fuuid, err)
            throw err
        }
    } finally {
        try {
            debug("reactiverFichier fermer sftp apres %s", fuuid)
        } catch(err) {
            debug("ERR sftp.end: %O", err)
        }
    }
}

async function consignerFichier(pathFichierStaging, fuuid) {
    debug("Consigner fichier fuuid %s (path %s)", fuuid, pathFichierStaging)
    // let sftp = await sftpClient()
    await _sftpDao.mkdir(getPathWork())

    const pathFichier = getPathWork(fuuid)

    // Creer hacheur en sha2-256 pour verifier avec sha256sums via ssh apres l'upload
    const hacheur = new Hacheur({encoding: 'base16', hash: 'sha2-256'})
    // Verifier contenu local uploade
    const verificateurHachage = new VerificateurHachage(fuuid)

    let positionVerif = 0
    const callbackVerif = async (chunk, position) => {
        if(positionVerif === position) {  // Handle retry
            await hacheur.update(chunk)
            await verificateurHachage.update(chunk)
            positionVerif += chunk.length
        } else if(positionVerif > position) {
            debug("Verification, skip position previous (retry?): %d < %d", position, positionVerif)
        } else {
            debug("Erreur hachage, position out of sync (%d != %d)", position, positionVerif)
        }
    }

    try {
        var writeHandle = await _sftpDao.open(pathFichier, 'w', 0o644)

        const entry = { position: 0, fullPath: path.join(pathFichierStaging, fuuid) }
        debug("Consigner fichier vers ftp %O", entry)
        
        let retryCount = 1
        while(retryCount++ < 3) {
            try {
                const { total } = await _sftpDao.putFile(fuuid, entry, writeHandle, callbackVerif)

                // Succes, break boucle retry
                fileSize = total  // Total est la position finale a l'ecriture
                retryCount = 0
                break
            } catch(err) {
                if(retryCount < 3) {
                    debug("consignerFichier Erreur putfile %s, reessayer. Detail : %O", fuuid, err)
                    // if(!_connexionSsh) {
                    //     // Reconnecter, restaurer etat ecriture
                    //     await connecterSSH()
                    //     sftp = await sftpClient()
                        writeHandle = await _sftpDao.open(pathFichier, 'r+', 0o644)
                    // }
                }
            }
        }

        // await writer.close()
        const multiHachageSha256 = await hacheur.finalize()
        const hachageSha256 = multiHachageSha256.slice(5)  // Retirer 5 premiers chars (f multibase, multihash)
        debug("Fichier %s sftp upload, sha-256 local = %s, taille %s", fuuid, hachageSha256, fileSize)
        await verificateurHachage.verify()
        debug("Fichier %s transfere avec succes vers consignation sftp, sha-256 local = %s", fuuid, hachageSha256)

        // Attendre que l'ecriture du fichier soit terminee (fs sync)
        // const handle = await open(pathFichier, 'r')
        let infoFichier = null
        debug("Attente sync du fichier %s", pathFichier)

        debug("Execution fstat sur %s", fuuid)
        infoFichier = await _sftpDao.fstat(writeHandle)
        debug("Stat fichier initial avant attente : %O", infoFichier)

        debug("Info fichier : %O", infoFichier)

        if(infoFichier.size !== fileSize) {
            const err = new Error(`Taille du fichier est differente sur sftp : ${infoFichier.size} != ${fileSize}`)
            debug("Erreur taille fichier: %O", err)
            throw err
        }

        debug("Information fichier sftp : %O", infoFichier)

        // S'assurer d'avoir le path ./local
        const pathLocal = getRemotePathFichiers()
        await _sftpDao.mkdir(pathLocal)

        const pathFichierRemote = getPathFichier(fuuid)
        const dirFichier = path.dirname(pathFichierRemote)
        await _sftpDao.mkdir(dirFichier)

        // Renommer de work/fuuid a local/fuuid
        await _sftpDao.rename(pathFichier, pathFichierRemote)

    } catch(err) {
        try {
            await _sftpDao.unlink(pathFichier)
        } catch(err) {
            console.error("Erreur unlink fichier sftp %s : %O", pathFichier, err)
        }
        throw err
    } finally {
        try { await _sftpDao.close(writeHandle) }
        catch(err) { debug("Erreur fermeture writeHandle %s : %O", fuuid, err)}
        try { 
            debug("consignerFichier fermer sftp apres %s", fuuid)
            // sftp.end(err=>debug("Fermeture sftp : %O", err)) 
        }
        catch(err) { debug("ERR fermerture sftp : %O", err)}
    }

}

async function marquerOrphelin(fuuid) {
    const pathFichier = getPathFichier(fuuid)
    const pathOrphelin = getPathFichierOrphelins(fuuid)
    const pathFichierOrphelinDir = path.dir(pathOrphelin)
    await _sftpDao.mkdir(pathFichierOrphelinDir)
    await _sftpDao.rename(pathFichier, pathOrphelin)
}

async function archiverFichier(fuuid) {
    const pathFichier = getPathFichier(fuuid)
    const pathArchive = getPathFichierArchives(fuuid)
    const pathFichieArchivesDir = path.dir(pathArchive)
    await _sftpDao.mkdir(pathFichieArchivesDir)
    await _sftpDao.rename(pathFichier, pathArchive)
}

async function parcourirFichiers(callback, opts) {
    debug("Parcourir fichiers")
    try {
        // var sftp = await sftpClient()
        const pathLocal = getRemotePathFichiers()
        debug("sftp.parcourirFichiers Path local %s", pathLocal)
        await _sftpDao.parcourirFichiersRecursif(pathLocal, callback, opts)
        await callback()  // Dernier appel avec aucune valeur (fin traitement)
    } finally {
        debug("parcourirFichiers fermer sftp")
        // sftp.end(err=>debug("SFTP end : %O", err))
    }
}

async function purgerOrphelinsExpires() {
    const expire = Math.floor((new Date().getTime() - CONST_EXPIRATION_ORPHELINS) / 1000)
    const pathOrphelins = getRemotePathOrphelins()
    debug("sftp.purgerOrphelinsExpires Path orphelins %s", pathOrphelins)
    const callback = async info => {
        if(!info) return  // C'est le dernier callback (vide)
        debug("Info fichier orphelin : ", info)
        if(info.modified < expire) {
            const filePath = path.join(info.directory, info.filename)
            debug("purgerOrphelinsExpires Retirer orphelin expire ", filePath)
            await _sftpDao.unlink(filePath)
        }
    }
    await _sftpDao.parcourirFichiersRecursif(pathOrphelins, callback)
    await callback()  // Dernier appel avec aucune valeur (fin traitement)
}

function getFichierStream(fuuid) {
    const pathFichier = getPathFichier(fuuid)
    return _sftpDao.createReadStream(pathFichier)
}

function getRemotePathFichiers() {
    return path.join(_remotePath, 'local')
}

function getRemotePathOrphelins() {
    return path.join(_remotePath, 'orphelins')
}

function getRemotePathArchives() {
    return path.join(_remotePath, 'archives')
}

function getRemotePathBackup() {
    return path.join(_remotePath, 'backup')
}

function getRemotePathBackupTransactions() {
    return path.join(_remotePath, 'backup', 'transactions')
}

async function parcourirBackup(callback, opts) {
    debug("Parcourir backup")
    
    try {

        // var sftp = await sftpClient()
        const pathLocal = getRemotePathBackup()

        // Init repertoires backup
        await _sftpDao.mkdir(pathLocal)
        await _sftpDao.mkdir(getRemotePathBackupTransactions())

        await _sftpDao.parcourirFichiersRecursif(pathLocal, callback, opts)
        await callback()  // Dernier appel avec aucune valeur (fin traitement)
    } catch(err) {
        if(err.code === 2) {
            debug("Repertoire backup n'existe pas (OK)")
            await callback()
        } else {
            throw err
        }
    } finally {
        debug("parcourirBackup fermer sftp")
        // sftp.end(err=>debug("SFTP end : %O", err))
    }
}

async function sauvegarderBackupTransactions(message) {

    const { domaine, partition, date_transactions_fin } = message
    const { uuid_transaction } = message['en-tete']

    const dateFinBackup = new Date(date_transactions_fin * 1000)
    debug("Sauvegarde du backup %s date %O", domaine, dateFinBackup)

    // Formatter le nom du fichier avec domaine_partition_DATE
    const dateFinString = dateFinBackup.toISOString().replaceAll('-', '').replaceAll(':', '')
    const nomFichierList = [domaine]
    if(partition) nomFichierList.push(partition)
    nomFichierList.push(dateFinString)
    nomFichierList.push(uuid_transaction.slice(0,8))  // Ajouter valeur "random" avec uuid_transaction
    
    const nomFichier = nomFichierList.join('_') + '.json.xz'
    const pathFichier = path.join(domaine, nomFichier)

    // Compresser en lzma et conserver
    const messageCompresse = await lzma.compress(JSON.stringify(message), 9)

    await pipeBackupTransactionStream(pathFichier, toStream(messageCompresse))

    debug("Backup %s date %O sauvegarde sous %O", domaine, dateFinBackup, pathFichier)
}

function toStream(bytes) {
    const byteReader = new Readable()
    byteReader._read = () => {}
    
    byteReader.push(bytes)
    byteReader.push(null)

    return byteReader
}

async function rotationBackupTransactions(message) {

    const { domaine, partition } = message
    debug("rotationBackupTransactions", domaine, partition)

    const maxArchives = 1

    const pathDomaine = path.join(getRemotePathBackupTransactions(), domaine)
    // const sftp = await sftpClient()
    await _sftpDao.rmdir(pathDomaine)

    console.warn(" !!! NOT IMPLEMENTED : storeConsignationSftp.rotationBackupTransactions !!! ")
}

async function getFichiersBackupTransactionsCourant(mq, replyTo) {
    throw new Error('not implemented')

    // Parcourir repertoire
    const promiseReaddirp = readdirp(PATH_BACKUP_TRANSACTIONS_DIR, {
        type: 'files',
        fileFilter: '*.json.xz',
        depth: 2,
    })

    let clesAccumulees = {}
    let countCles = 0

    for await (const entry of promiseReaddirp) {
        debug("Fichier backup transactions : %O", entry)
        const nomFichier = entry.path
        let contenu = await fsPromises.readFile(entry.fullPath)
        contenu = await lzma.decompress(contenu)
        debug("Contenu archive str : %O", contenu)
        contenu = JSON.parse(contenu)
        debug("Contenu archive : %O", contenu)

        const cle = contenu.cle
        clesAccumulees[nomFichier] = cle
        countCles++

        if(countCles >= 1000) {
            debug("Emettre message %d cles (batch)", countCles)
            await emettreMessageCles(mq, replyTo, clesAccumulees, false)

            // Clear
            clesAccumulees = {}
            countCles = 0
        }
    }

    debug("Emettre message %d cles (final)", countCles)
    await emettreMessageCles(mq, replyTo, clesAccumulees, true)

    return {ok: true}
}

async function emettreMessageCles(mq, replyTo, cles, complet) {
    const reponse = { ok: true, cles, complet }
    await mq.transmettreReponse(reponse, replyTo, 'cles')
}

async function getBackupTransaction(pathBackupTransaction) {
    const readStream = getRemotePathBackupTransactions(pathBackupTransaction)
    contenu = await lzma.decompress(readStream)

    debug("Contenu archive str : %O", contenu)
    contenu = JSON.parse(contenu)

    debug("Contenu archive : %O", contenu)
    return contenu
}

async function getBackupTransactionStream(pathBackupTransaction) {
    const pathDomaine = path.join(getRemotePathBackupTransactions(), pathBackupTransaction)
    // const sftp = await sftpClient()
    return _sftpDao.createReadStream(pathDomaine)
}

async function pipeBackupTransactionStream(pathFichier, stream) {
    
    // const sftp = await sftpClient()

    const pathFichierParsed = path.parse(pathFichier)
    debug("pipeBackupTransactionStream ", pathFichierParsed)
    const dirBackup = getRemotePathBackupTransactions()
    await _sftpDao.mkdir(dirBackup)
    const dirFichier = path.join(dirBackup, pathFichierParsed.dir)
    await _sftpDao.mkdir(dirFichier)
    
    const pathFichierComplet = path.join(dirBackup, pathFichier)
    
    const writeHandle = await _sftpDao.open(pathFichierComplet, 'w', 0o644)
    const resultat = await _sftpDao.writeStream(stream, writeHandle)

    return resultat
}

async function deleteBackupTransaction(pathBackupTransaction) {
    const dirBackup = getRemotePathBackupTransactions()
    const pathFichier = path.join(dirBackup, pathBackupTransaction)
    // const sftp = await sftpClient()
    await _sftpDao.unlink(pathFichier)
}

module.exports = {
    init, fermer,
    chargerConfiguration, modifierConfiguration,

    getInfoFichier, getFichierStream, 
    consignerFichier, marquerOrphelin, purgerOrphelinsExpires, archiverFichier, reactiverFichier,
    parcourirFichiers,

    // Backup
    parcourirBackup,
    sauvegarderBackupTransactions, rotationBackupTransactions,
    getFichiersBackupTransactionsCourant, getBackupTransaction,
    getBackupTransactionStream, pipeBackupTransactionStream, deleteBackupTransaction,
}
