const debug = require('debug')('consignation:store:local')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const readdirp = require('readdirp')
const lzma = require('lzma-native')

const CONSIGNATION_PATH = process.env.MG_CONSIGNATION_PATH || '/var/opt/millegrilles/consignation'
const PATH_CONFIG_DIR = path.join(CONSIGNATION_PATH, 'config')
const PATH_CONFIG_FICHIER = path.join(PATH_CONFIG_DIR, 'store.json')
const PATH_BACKUP_CONSIGNATION_DIR = path.join(CONSIGNATION_PATH, 'backupConsignation')
// const PATH_ARCHIVES_DIR = path.join(CONSIGNATION_PATH, 'archives')

const DEFAULT_URL_CONSIGNATION = 'https://fichiers:443',
      CONST_EXPIRATION_ORPHELINS = 86_400_000 * 7,
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
            console.error("storeConsignationLocal.chargerConfiguration ERROR Erreur chargement fichier configuration. On RESET le fichier\n%O", err)
            config = {type_store: 'millegrille', consignation_url: DEFAULT_URL_CONSIGNATION}
            await fsPromises.writeFile(PATH_CONFIG_FICHIER, JSON.stringify(config))
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

function getPathFichierBackup(pathFichier) {
    return path.join(PATH_BACKUP_CONSIGNATION_DIR, pathFichier)
}

/**
 * Retourne un readStream pour le fichier local.
 * @param {} idFichier 
 * @returns ReadStream
 */
async function getFichierStream(idFichier, opts) {
    opts = opts || {}
    const { archive, backup, supporteArchives } = opts

    let pathFichier = null
    if(backup) {
        pathFichier = getPathFichierBackup(idFichier)
    } else if(archive) {
        pathFichier = getPathFichierArchives(idFichier)
    } else {
        pathFichier = getPathFichier(idFichier)
    }

    try {
        return fs.createReadStream(pathFichier)
    } catch(err) {
        if(err.errno === -2) {
            if(!archive && supporteArchives === true) {
                // Fallback sur archive
                const pathFichier = getPathFichierArchives(idFichier)
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

async function getInfoFichier(idFichier, opts) {
    opts = opts || {}
    const { archive, backup, supporteArchives } = opts

    let filePath = null
    if(backup) {
        filePath = getPathFichierBackup(idFichier)
    } else if(archive) {
        filePath = getPathFichierArchives(idFichier)
    } else {
        filePath = getPathFichier(idFichier)
    }

    try {
        // const filePath = getPathFichier(idFichier, opts)
        const stat = await fsPromises.stat(filePath)
        return { stat, filePath }
    } catch(err) {
        if(!archive && supporteArchives === true && err.code === 'ENOENT') {
            const filePath = getPathFichierArchives(idFichier)
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
    opts = opts || {}
    const { uuid_backup, domaine } = opts
    let pathBackup = PATH_BACKUP_CONSIGNATION_DIR
    if(uuid_backup) {
        pathBackup = path.join(pathBackup, uuid_backup)
        if(domaine) {
            pathBackup = path.join(pathBackup, domaine)
        }
    }
    await parcourirFichiersRecursif(pathBackup, callback, opts)
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
    const depth = opts.depth || 1,
          types = opts.types || 'files'
    debug("parcourirFichiers %s", repertoire)
  
    const settingsReaddirp = { type: types, alwaysStat: true, depth }

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

async function sauvegarderBackupTransactions(uuid_backup, domaine, srcPath) {

    const nomfichier = path.basename(srcPath)

    const dirpathDestination = path.join(PATH_BACKUP_CONSIGNATION_DIR, uuid_backup, domaine)
    await fsPromises.mkdir(dirpathDestination, {recursive: true})

    const destPath = path.join(dirpathDestination, nomfichier)
    await deplacerFichier(srcPath, destPath)
}

async function rotationBackupTransactions(uuid_backups_courants) {
    debug("rotationBackupTransactions Rotation backups, conserver : %O", uuid_backups_courants)
    if(!uuid_backups_courants || uuid_backups_courants.length <= 1) {
        debug("rotationBackupTransactions Skip, liste de backups courants est trop petite : %O", uuid_backups_courants)
        return
    }

    const uuidBackupsSet = new Set(uuid_backups_courants)

    const listeSuppression = []
    const promiseReaddirp = readdirp(PATH_BACKUP_CONSIGNATION_DIR, {
        type: 'directories',
        depth: 0,
    })
    for await (const entry of promiseReaddirp) {
        debug("rotationBackupTransactions Repertoire d'un backup set : %O", entry)
        const uuid_backup = entry.path
        if(!uuidBackupsSet.has(uuid_backup)) {
            listeSuppression.push(entry.fullPath)
        }
    }

    debug("rotationBackupTransactions Supprimer vieux backup sets : %O", listeSuppression)

    for await (const pathBackupSet of listeSuppression) {
        await fsPromises.rm(pathBackupSet, {recursive: true})
            .catch(err=>console.error("Erreur suppression vieux backup %s : %O", pathBackupSet, err))
    }
}

// async function getFichiersBackupTransactionsCourant(mq, replyTo) {
//     throw new Error('fix me')
//     // // Parcourir repertoire
//     // const promiseReaddirp = readdirp(PATH_BACKUP_TRANSACTIONS_DIR, {
//     //     type: 'files',
//     //     fileFilter: '*.json.xz',
//     //     depth: 2,
//     // })

//     // let clesAccumulees = {}
//     // let countCles = 0

//     // for await (const entry of promiseReaddirp) {
//     //     debug("Fichier backup transactions : %O", entry)
//     //     const nomFichier = entry.path
//     //     let contenu = await fsPromises.readFile(entry.fullPath)
//     //     contenu = await lzma.decompress(contenu)
//     //     debug("Contenu archive str : %O", contenu)
//     //     contenu = JSON.parse(contenu)
//     //     debug("Contenu archive : %O", contenu)
        
//     //     // Extraire le message du catalogue d'archive
//     //     const contenuMessage = JSON.parse(contenu.contenu)

//     //     const cle = contenuMessage.cle
//     //     clesAccumulees[nomFichier] = cle
//     //     countCles++

//     //     if(countCles >= 1000) {
//     //         debug("Emettre message %d cles (batch)", countCles)
//     //         await emettreMessageCles(mq, replyTo, clesAccumulees, false)

//     //         // Clear
//     //         clesAccumulees = {}
//     //         countCles = 0
//     //     }
//     // }

//     // debug("Emettre message %d cles (final) : ", countCles, clesAccumulees)
//     // await emettreMessageCles(mq, replyTo, clesAccumulees, true)

//     // return {ok: true}
// }

// async function emettreMessageCles(mq, replyTo, cles, complet) {
//     const reponse = { ok: true, cles, complet }
//     await mq.transmettreReponse(reponse, replyTo, 'cles')
// }

// async function getBackupTransaction(pathBackupTransaction) {
//     throw new Error('fix me')
//     // const pathFichier = path.join(PATH_BACKUP_TRANSACTIONS_DIR, pathBackupTransaction)

//     // let contenu = await fsPromises.readFile(pathFichier)
//     // contenu = await lzma.decompress(contenu)

//     // debug("Contenu archive str : %O", contenu)
//     // contenu = JSON.parse(contenu)

//     // debug("Contenu archive : %O", contenu)
//     // return contenu
// }

async function getBackupTransactionStream(pathBackupTransaction) {
    const pathFichier = path.join(PATH_BACKUP_CONSIGNATION_DIR, pathBackupTransaction)
    return fs.createReadStream(pathFichier)
}

// async function pipeBackupTransactionStream(pathFichier, stream) {
//     throw new Error('obsolete')
//     // const pathFichierParsed = path.parse(pathFichier)
//     // debug("pipeBackupTransactionStream ", pathFichierParsed)
//     // const dirFichier = path.join(PATH_BACKUP_TRANSACTIONS_DIR, pathFichierParsed.dir)
//     // const pathFichierComplet = path.join(PATH_BACKUP_TRANSACTIONS_DIR, pathFichier)
//     // await fsPromises.mkdir(dirFichier, {recursive: true})
    
//     // const writeStream = fs.createWriteStream(pathFichierComplet)
//     // await new Promise((resolve, reject)=>{
//     //     writeStream.on('close', resolve)
//     //     writeStream.on('error', reject)
//     //     stream.pipe(writeStream)
//     // })
// }

// async function deleteBackupTransaction(pathBackupTransaction) {
//     throw new Error('obsolete')
//     // const pathFichier = path.join(PATH_BACKUP_TRANSACTIONS_DIR, pathBackupTransaction)
//     // await fsPromises.unlink(pathFichier)
// }

async function deplacerFichier(pathSource, pathDestination) {
    const dirFichier = path.dirname(pathDestination)
    await fsPromises.mkdir(dirFichier, {recursive: true})

    try {
        // Methode simple, rename (move)
        await fsPromises.rename(pathSource, pathDestination)
    } catch(err) {
        if(err.code === 'ENOENT') {
            console.error(new Date() + " deplacerFichier Fichier %s introuvable ", pathSource)
            throw err
        }
        debug("deplacerFichier Erreur rename, tenter copy ", err)
        const reader = fs.createReadStream(pathSource)
        const writer = fs.createWriteStream(pathDestination)
        await new Promise((resolve, reject)=>{
            writer.on('close', resolve)
            writer.on('error', err => {
                fsPromises.unlink(pathDestination)
                    .catch(err=>console.warn("deplacerFichier Erreur suppression fichier incomplet %s : %O", pathDestination, err))
                reject(err)
            })
            reader.pipe(writer)
        })
        fsPromises.rm(pathSource).catch(err=>console.error("deplacerFichier Erreur suppression fichier : %O", err))
    }

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
    sauvegarderBackupTransactions, rotationBackupTransactions, getBackupTransactionStream, 
    //getFichiersBackupTransactionsCourant, getBackupTransaction, pipeBackupTransactionStream, deleteBackupTransaction,
}
