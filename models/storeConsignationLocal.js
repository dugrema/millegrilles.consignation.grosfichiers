const debug = require('debug')('consignation:store:local')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const readdirp = require('readdirp')
const lzma = require('lzma-native')

const CONSIGNATION_PATH = process.env.MG_CONSIGNATION_PATH || '/var/opt/millegrilles/consignation'
const PATH_CONFIG_DIR = path.join(CONSIGNATION_PATH, 'config')
const PATH_CONFIG_FICHIER = path.join(PATH_CONFIG_DIR, 'store.json')
const PATH_BACKUP_TRANSACTIONS_DIR = path.join(CONSIGNATION_PATH, 'backup', 'transactions')
// const PATH_ARCHIVES_DIR = path.join(CONSIGNATION_PATH, 'archives')

const DEFAULT_URL_CONSIGNATION = 'https://fichiers:443',
      CONST_EXPIRATION_ORPHELINS = 86_400_000 * 3,
      ERROR_CODE_FICHIER_INCONNU = 1

let _pathConsignation = path.join(CONSIGNATION_PATH, 'local'),
    _pathOrphelins = path.join(CONSIGNATION_PATH, 'orphelins'),
    _pathArchives = path.join(CONSIGNATION_PATH, 'archives')

function init(params) {
    params = params || {}
    _pathConsignation = params.pathConsignation || _pathConsignation
}

async function chargerConfiguration() {

    // S'assurer que le repertoire existe
    await fsPromises.mkdir(PATH_CONFIG_DIR, {recursive: true})

    let config = null
    try {
        const fichierConfiguration = await fsPromises.readFile(PATH_CONFIG_FICHIER, 'utf-8')
        config = JSON.parse(fichierConfiguration)
    } catch(err) {
        if(err.errno === -2) {
            config = {type_store: 'millegrille', consignation_url: DEFAULT_URL_CONSIGNATION}
            await fsPromises.writeFile(PATH_CONFIG_FICHIER, JSON.stringify(config))
        } else {
            console.error("storeConsignationLocal.chargerConfiguration ERROR Erreur chargement fichier configuration : %O", err)
            throw new err
        }
    }

    return config
}

async function modifierConfiguration(params, opts) {
    opts = opts || {}
    let config

    // Retirer le data dechiffre si present
    if(opts.override !== true) {
        let configCourante = await chargerConfiguration()
        config = {...configCourante, ...params, data_dechiffre: undefined}
    } else {
        config = {...params, data_dechiffre: undefined}
    }

    await fsPromises.writeFile(PATH_CONFIG_FICHIER, JSON.stringify(config))
}

function getPathFichier(fuuid) {
    // Split path en 2 derniers caracteres
    const last2 = fuuid.slice(fuuid.length-2)
    return path.join(_pathConsignation, last2, fuuid)
}

function getPathFichierOrphelins(fuuid) {
    // Split path en 2 derniers caracteres
    const last2 = fuuid.slice(fuuid.length-2)
    return path.join(_pathOrphelins, last2, fuuid)
}

function getPathFichierArchives(fuuid) {
    const last2 = fuuid.slice(fuuid.length-2)
    return path.join(_pathArchives, last2, fuuid)
}

/**
 * Retourne un readStream pour le fichier local.
 * @param {} fuuid 
 * @returns ReadStream
 */
async function getFichierStream(fuuid, opts) {
    const pathFichier = getPathFichier(fuuid)
    try {
        return fs.createReadStream(pathFichier)
    } catch(err) {
        if(err.errno === -2) {
            if(opts.supporteArchive === true) {
                const pathFichier = getPathFichierArchives(fuuid)
                try {
                    return fs.createReadStream(pathFichier)
                } catch(err) {
                    // Rien a faire, return null
                }
            }
            return null
        }
        else throw err
    }
}

async function getInfoFichier(fuuid, opts) {
    opts = opts || {}
    try {
        const filePath = getPathFichier(fuuid)
        const stat = await fsPromises.stat(filePath)
        return { stat, filePath }
    } catch(err) {
        if(err.code === 'ENOENT' && opts.supporteArchives === true) {
            const filePath = getPathFichierArchives(fuuid)
            const stat = await fsPromises.stat(filePath)
            return { stat, filePath }
        }
        throw err  // Echec ou pas archive
    }
}

async function consignerFichier(pathFichierStaging, fuuid, opts) {
    opts = opts || {}
    const pathSource = path.join(pathFichierStaging, fuuid)

    let pathFichier = null 
    if(opts.archive === true) {
        pathFichier = getPathFichierArchives(fuuid)
    } else {
        pathFichier = getPathFichier(fuuid)
    }
    const dirFichier = path.dirname(pathFichier)
    await fsPromises.mkdir(dirFichier, {recursive: true})

    try {
        // Methode simple, rename (move)
        await fsPromises.rename(pathSource, pathFichier)
    } catch(err) {
        if(err.code === 'ENOENT') {
            console.error(new Date() + " storeConsignationLocal.consignerFichier Fichier %s introuvable ", pathSource)
            throw err
        }
        debug("consignerFichier Erreur rename, tenter copy ", err)
        const reader = fs.createReadStream(pathSource)
        const writer = fs.createWriteStream(pathFichier)
        const promise = new Promise((resolve, reject)=>{
            writer.on('close', resolve)
            writer.on('error', reject)
        })
        reader.pipe(writer)
        await promise
        fsPromises.rm(pathFichierStaging, {recursive: true})
            .catch(err=>console.error("Erreur suppression fichier : %O", err))
    }

}

async function marquerOrphelin(fuuid) {
    const pathFichier = getPathFichier(fuuid)
    const pathFichierOrphelins = getPathFichierOrphelins(fuuid)
    const dirPathOrphelins = path.dirname(pathFichierOrphelins)
    await fsPromises.mkdir(dirPathOrphelins, {recursive: true})
    return await fsPromises.rename(pathFichier, pathFichierOrphelins)
}

async function purgerOrphelinsExpires(opts) {
    opts = opts || {}
    const expiration = opts.expiration || CONST_EXPIRATION_ORPHELINS

    const expirationDate = new Date().getTime() - expiration

    const repertoireOrphelins = _pathOrphelins
    const settingsReaddirp = { type: 'files', alwaysStat: true, depth: 1 }
    for await (const entry of readdirp(repertoireOrphelins, settingsReaddirp)) {
        debug("Marquer orphelin entry : ", entry.path)
        if( entry.stats.ctimeMs < expirationDate ) {
            try {
                debug("Supprimer fichier orphelin : ", entry.basename)
                await fsPromises.rm(entry.fullPath)
            } catch(err) {
                console.warn(new Date() + " WARN Echec suppression fichier orphelin expire ", err)
            }
        }
    }
}

/** Deplace un fichier actif vers le dossier archives */
async function archiverFichier(fuuid) {
    const pathFichierActif = getPathFichier(fuuid),
          pathFichierArchives = getPathFichierArchives(fuuid),
          pathDirFichierArchives = path.dirname(pathFichierArchives)

    debug("Archiver fichier %s vers %s", fuuid, pathFichierArchives)

    try {
        await fsPromises.mkdir(pathDirFichierArchives, {recursive: true})
    } catch(err) {
        debug("Erreur creation repertoire archives %s : %O", pathDirFichierArchives, err)
        throw err
    }
    await fsPromises.rename(pathFichierActif, pathFichierArchives)
}

/** Remet un fichier qui est sous orphelins ou archive dans le repertoire actif (e.g. local) */
async function reactiverFichier(fuuid) {
    const pathFichierActif = getPathFichier(fuuid),
          pathDirFichierActif = path.dirname(pathFichierActif)
    const pathFichierOrphelins = getPathFichierOrphelins(fuuid)
    const pathFichierArchives = getPathFichierArchives(fuuid)

    // Verifier si le fichier existe deja dans le repertoire actifs
    try {
        await fsPromises.stat(pathFichierActif)
        return  // Ok, fichier existe
    } catch(err) {
        if(err.code === 'ENOENT') {
            // Fichier n'existe pas, on continue
        } else {
            throw err  // Autre type d'erreur
        }
    }

    await fsPromises.mkdir(pathDirFichierActif, {recursive: true})

    try {
        await fsPromises.rename(pathFichierOrphelins, pathFichierActif)
    } catch(err) {
        if(err.code === 'ENOENT') {
            try {
                await fsPromises.rename(pathFichierArchives, pathFichierActif)
            } catch(err) {
                if(err.code === 'ENOENT') {
                    // Fichier inconnu (pas dans orphelins ni archives)
                    const error = new Error(`Fichier ${fuuid} inconnu`)
                    error.cause = err
                    error.fuuid = fuuid
                    error.code = ERROR_CODE_FICHIER_INCONNU
                    throw error
                }
            }
        } else {
            throw err
        }
    }
}

async function parcourirFichiers(callback, opts) {
    opts = opts || {}
    await parcourirFichiersRecursif(_pathConsignation, callback, {...opts, depth: 3})
    await callback()  // Dernier appel avec aucune valeur (fin traitement)
}

async function parcourirBackup(callback, opts) {
    await parcourirFichiersRecursif(PATH_BACKUP_TRANSACTIONS_DIR, callback, opts)
    await callback()  // Dernier appel avec aucune valeur (fin traitement)
}

async function parcourirArchives(callback, opts) {
    await parcourirFichiersRecursif(_pathArchives, callback, opts)
    await callback()  // Dernier appel avec aucune valeur (fin traitement)
}

async function parcourirOrphelins(callback, opts) {
    await parcourirFichiersRecursif(_pathOrphelins, callback, opts)
    await callback()  // Dernier appel avec aucune valeur (fin traitement)
}

async function parcourirFichiersRecursif(repertoire, callback, opts) {
    opts = opts || {}
    const depth = opts.depth || 1
    debug("parcourirFichiers %s", repertoire)
  
    const settingsReaddirp = { type: 'files', alwaysStat: true, depth }

    for await (const entry of readdirp(repertoire, settingsReaddirp)) {
        debug("Fichier : %O", entry.fullPath)
        const { basename, fullPath, stats } = entry
        const { mtimeMs, size } = stats
        const repertoire = path.dirname(fullPath)
        const data = { filename: basename, directory: repertoire, modified: mtimeMs, size }

        let utiliserFichier = true
        if(opts.filtre) utiliserFichier = opts.filtre(data)

        if(utiliserFichier) {
            await callback(data)
        }
    }
}

async function sauvegarderBackupTransactions(message) {

    const contenu = JSON.parse(message.contenu)
    const { domaine, partition, date_transactions_fin } = contenu
    const uuid_transaction = message.id

    const dateFinBackup = new Date(date_transactions_fin * 1000)
    debug("Sauvegarde du backup %s date %O", domaine, dateFinBackup)

    // Creer repertoire de backup
    const dirBackup = path.join(PATH_BACKUP_TRANSACTIONS_DIR, domaine)
    await fsPromises.mkdir(dirBackup, {recursive: true})

    // Formatter le nom du fichier avec domaine_partition_DATE
    const dateFinString = dateFinBackup.toISOString().replaceAll('-', '').replaceAll(':', '')
    const nomFichierList = [domaine]
    if(partition) nomFichierList.push(partition)
    nomFichierList.push(dateFinString)
    nomFichierList.push(uuid_transaction.slice(0,8))  // Ajouter valeur "random" avec uuid_transaction
    
    const nomFichier = nomFichierList.join('_') + '.json.xz'
    const pathFichier = path.join(dirBackup, nomFichier)

    // Compresser en lzma et conserver
    const messageCompresse = await lzma.compress(JSON.stringify(message), 9)
    await fsPromises.writeFile(pathFichier + '.new', messageCompresse)
    await fsPromises.rename(pathFichier + '.new', pathFichier)
    
    debug("Backup %s date %O sauvegarde sous %O", domaine, dateFinBackup, pathFichier)
}

async function rotationBackupTransactions() {

    const maxArchives = 3

    // try {
    //     await fsPromises.mkdir(PATH_BACKUP_TRANSACTIONS_ARCHIVES_DIR)
    // } catch(err) {
    //     // EEXIST est ok
    //     if(err.code !== 'EEXIST') throw err
    // }

    const dirBackup = path.join(PATH_BACKUP_TRANSACTIONS_DIR)
    const dirArchives = path.join(PATH_BACKUP_TRANSACTIONS_DIR + '.1')

    try {
        await fsPromises.stat(dirBackup)
    } catch (err) {
        // Le code ENOENT (inexistant) indique qu'il n'y a rien a faire
        if(err.code === 'ENOENT') return
    }

    const dirArchiveDernier = path.join(PATH_BACKUP_TRANSACTIONS_DIR + '.' + maxArchives)
    try {
        await fsPromises.rm(dirArchiveDernier, {recursive: true})
    } catch(err) {
        // Le code ENOENT (inexistant) est OK
        if(err.code != 'ENOENT') throw err
    }

    try {
        await pushRotateArchive(1, 2)
    } catch(err) {
        // Le code ENOENT (inexistant) est OK
        if(err.code != 'ENOENT') throw err
    }

    debug("Deplacer repertoire %s vers archive %s", dirBackup, dirArchives)
    await fsPromises.rename(dirBackup, dirArchives)
}

async function pushRotateArchive(idxFrom) {
    const dirArchivesScr = path.join(PATH_BACKUP_TRANSACTIONS_DIR + '.' + idxFrom)
    const dirArchivesDst = path.join(PATH_BACKUP_TRANSACTIONS_DIR + '.' + (idxFrom+1))

    debug("pushRotateArchive idxFrom %d", idxFrom)

    try {
        await fsPromises.stat(dirArchivesDst)
        // Le repertoire existe, on le deplace en premier (recursivement)
        await pushRotateArchive(idxFrom+1)
    } catch (err) {
        // Le code ENOENT (inexistant) est OK
        if(err.code != 'ENOENT') return  // Le code ENOENT (inexistant) est OK, rien a faire
    }

    await fsPromises.rename(dirArchivesScr, dirArchivesDst)
}

async function getFichiersBackupTransactionsCourant(mq, replyTo) {
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

    const pathFichier = path.join(PATH_BACKUP_TRANSACTIONS_DIR, pathBackupTransaction)

    let contenu = await fsPromises.readFile(pathFichier)
    contenu = await lzma.decompress(contenu)

    debug("Contenu archive str : %O", contenu)
    contenu = JSON.parse(contenu)

    debug("Contenu archive : %O", contenu)
    return contenu
}

async function getBackupTransactionStream(pathBackupTransaction) {
    const pathFichier = path.join(PATH_BACKUP_TRANSACTIONS_DIR, pathBackupTransaction)
    return fs.createReadStream(pathFichier)
}

async function pipeBackupTransactionStream(pathFichier, stream) {
    const pathFichierParsed = path.parse(pathFichier)
    debug("pipeBackupTransactionStream ", pathFichierParsed)
    const dirFichier = path.join(PATH_BACKUP_TRANSACTIONS_DIR, pathFichierParsed.dir)
    const pathFichierComplet = path.join(PATH_BACKUP_TRANSACTIONS_DIR, pathFichier)
    await fsPromises.mkdir(dirFichier, {recursive: true})
    
    const writeStream = fs.createWriteStream(pathFichierComplet)
    await new Promise((resolve, reject)=>{
        writeStream.on('close', resolve)
        writeStream.on('error', reject)
        stream.pipe(writeStream)
    })
}

async function deleteBackupTransaction(pathBackupTransaction) {
    const pathFichier = path.join(PATH_BACKUP_TRANSACTIONS_DIR, pathBackupTransaction)
    await fsPromises.unlink(pathFichier)
}

module.exports = {
    PATH_CONFIG_DIR, PATH_CONFIG_FICHIER, DEFAULT_URL_CONSIGNATION, 

    init, chargerConfiguration, modifierConfiguration,
    
    getInfoFichier, getFichierStream, 
    consignerFichier, marquerOrphelin, purgerOrphelinsExpires, archiverFichier, reactiverFichier,
    parcourirFichiers,

    parcourirArchives, parcourirOrphelins,
    
    // Backup
    parcourirBackup, 
    sauvegarderBackupTransactions, rotationBackupTransactions,
    getFichiersBackupTransactionsCourant, getBackupTransaction,
    getBackupTransactionStream, pipeBackupTransactionStream, deleteBackupTransaction,
}
