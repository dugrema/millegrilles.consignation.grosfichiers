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
const PATH_BACKUP_TRANSACTIONS_ARCHIVES_DIR = path.join(CONSIGNATION_PATH, 'backup', 'transactions_archives')

const DEFAULT_URL_CONSIGNATION = 'https://fichiers:443'

let _pathConsignation = path.join(CONSIGNATION_PATH, 'local'),
    _pathCorbeille = path.join(CONSIGNATION_PATH, 'corbeille')

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
    // Split path en 4 derniers caracteres
    const last2 = fuuid.slice(fuuid.length-2)
    // const sub1 = last2[1],
    //       sub2 = last2[0]
    return path.join(_pathConsignation, last2, fuuid)
}

function getPathFichierCorbeille(fuuid) {
    const last2 = fuuid.slice(fuuid.length-2)
    // const sub1 = last2[1],
    //       sub2 = last2[0]
    return path.join(_pathCorbeille, last2, fuuid)
}

/**
 * Retourne un readStream pour le fichier local.
 * @param {} fuuid 
 * @returns ReadStream
 */
async function getFichier(fuuid) {
    const pathFichier = getPathFichier(fuuid)
    try {
        const stream = fs.createReadStream(pathFichier)
    } catch(err) {
        if(err.errno === -2) return null
        else throw err
    }
}

async function getInfoFichier(fuuid, opts) {
    opts = opts || {}
    const filePath = getPathFichier(fuuid)
    try {
        const stat = await fsPromises.stat(filePath)
        return { stat, filePath }
    } catch(err) {
        if(err.errno === -2) {
            if(opts.recover === true)  {
                // Verifier si le fichier est encore disponible
                return recoverFichierSupprime(fuuid)
            }
            return null
        }
        else throw err
    }
}

async function recoverFichierSupprime(fuuid) {
    const filePathCorbeille = getPathFichierCorbeille(fuuid)
    const filePath = getPathFichier(fuuid)
    const dirDestPath = path.dirname(filePath)
    try {
        await fsPromises.stat(filePathCorbeille)
        await fsPromises.mkdir(dirDestPath, {recursive: true})
        await fsPromises.rename(filePathCorbeille, filePath)
        const stat = await fsPromises.stat(filePath)
        return { stat, filePath }
    } catch(err) {
        debug("Erreur recoverFichierSupprime %s : %O", fuuid, err)
        return null
    }
}

async function consignerFichier(pathFichierStaging, fuuid) {
    const pathSource = path.join(pathFichierStaging, fuuid)
    const pathFichier = getPathFichier(fuuid)
    const dirFichier = path.dirname(pathFichier)
    await fsPromises.mkdir(dirFichier, {recursive: true})

    try {
        // Methode simple, rename (move)
        await fsPromises.rename(pathSource, pathFichier)
    } catch(err) {
        // if(err.code === 'ENOENT') {
        //     console.error(new Date() + " storeConsignationLocal.consignerFichier Fichier %s introuvable ", pathSource)
        //     return
        // }
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

async function marquerSupprime(fuuid) {
    const pathFichier = getPathFichier(fuuid)
    const pathFichierCorbeille = getPathFichierCorbeille(fuuid)
    const dirPathCorbeille = path.dirname(pathFichierCorbeille)
    // const pathFichierSupprime = pathFichier + '.corbeille'
    await fsPromises.mkdir(dirPathCorbeille, {recursive: true})
    return await fsPromises.rename(pathFichier, pathFichierCorbeille)
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

async function parcourirFichiersRecursif(repertoire, callback, opts) {
    opts = opts || {}
    const depth = opts.depth || 1
    debug("parcourirFichiers %s", repertoire)
  
    const settingsReaddirp = { type: 'files', alwaysStat: true, depth }

    for await (const entry of readdirp(repertoire, settingsReaddirp)) {
        debug("Fichier : %O", entry)
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

    const { domaine, partition, date_transactions_fin } = message
    const { uuid_transaction } = message['en-tete']

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

async function rotationBackupTransactions(message) {

    const { domaine, partition } = message

    const maxArchives = 3

    try {
        await fsPromises.mkdir(PATH_BACKUP_TRANSACTIONS_ARCHIVES_DIR)
    } catch(err) {
        // EEXIST est ok
        if(err.code !== 'EEXIST') throw err
    }

    const dirBackup = path.join(PATH_BACKUP_TRANSACTIONS_DIR, domaine)
    const dirArchives = path.join(PATH_BACKUP_TRANSACTIONS_ARCHIVES_DIR, domaine + '.1')

    try {
        await fsPromises.stat(dirBackup)
    } catch (err) {
        // Le code ENOENT (inexistant) indique qu'il n'y a rien a faire
        if(err.code === 'ENOENT') return
    }

    const dirArchiveDernier = path.join(PATH_BACKUP_TRANSACTIONS_ARCHIVES_DIR, domaine + '.' + maxArchives)
    try {
        await fsPromises.rm(dirArchiveDernier, {recursive: true})
    } catch(err) {
        // Le code ENOENT (inexistant) est OK
        if(err.code != 'ENOENT') throw err
    }

    try {
        await pushRotateArchive(domaine, partition, 1, 2)
    } catch(err) {
        // Le code ENOENT (inexistant) est OK
        if(err.code != 'ENOENT') throw err
    }

    debug("Deplacer repertoire %s vers archive %s", dirBackup, dirArchives)
    await fsPromises.rename(dirBackup, dirArchives)
}

async function pushRotateArchive(domaine, partition, idxFrom) {
    const dirArchivesScr = path.join(PATH_BACKUP_TRANSACTIONS_ARCHIVES_DIR, domaine + '.' + idxFrom)
    const dirArchivesDst = path.join(PATH_BACKUP_TRANSACTIONS_ARCHIVES_DIR, domaine + '.' + (idxFrom+1))

    debug("pushRotateArchive domaine %s, partition %s, idxFrom %d", domaine, partition, idxFrom)

    try {
        await fsPromises.stat(dirArchivesDst)
        // Le repertoire existe, on le deplace en premier (recursivement)
        await pushRotateArchive(domaine, partition, idxFrom+1)
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
    getInfoFichier, consignerFichier,
    marquerSupprime, recoverFichierSupprime, 
    parcourirFichiers, parcourirBackup,
    sauvegarderBackupTransactions, rotationBackupTransactions,
    getFichiersBackupTransactionsCourant, getBackupTransaction,
    getBackupTransactionStream, pipeBackupTransactionStream, deleteBackupTransaction,
}
