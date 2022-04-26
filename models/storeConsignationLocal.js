const debug = require('debug')('consignation:store:local')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const readdirp = require('readdirp')

const { VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')

const CONSIGNATION_PATH = process.env.MG_CONSIGNATION_PATH || '/var/opt/millegrilles/consignation'
const PATH_CONFIG_DIR = path.join(CONSIGNATION_PATH, 'config')
const PATH_CONFIG_FICHIER = path.join(PATH_CONFIG_DIR, 'store.json')

let _pathConsignation = path.join(CONSIGNATION_PATH, 'local')

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
            config = {typeStore: 'local'}
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
    if(opts.override !== true) {
        let configCourante = await chargerConfiguration()
        config = {...configCourante, ...params}
    } else {
        config = {...params}
    }
    await fsPromises.writeFile(PATH_CONFIG_FICHIER, JSON.stringify(config))
}

function getPathFichier(fuuid) {
    return path.join(_pathConsignation, fuuid)
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
    const filePath = getPathFichier(fuuid)
    const filePathCorbeille = filePath + '.corbeille'
    try {
        await fsPromises.stat(filePathCorbeille)
        await fsPromises.rename(filePathCorbeille, filePath)
        const stat = await fsPromises.stat(filePath)
        return { stat, filePath }
    } catch(err) {
        debug("Erreur recoverFichierSupprime %s : %O", fuuid, err)
        return null
    }
}

async function consignerFichier(pathFichierStaging, fuuid) {
    const pathFichier = getPathFichier(fuuid)

    // Lire toutes les parts et combiner dans la destination
    const dirFichier = path.dirname(pathFichier)
    await fsPromises.mkdir(dirFichier, {recursive: true})
    const writer = fs.createWriteStream(pathFichier)

    const promiseReaddirp = readdirp(pathFichierStaging, {
        type: 'files',
        fileFilter: '*.part',
        depth: 1,
    })

    const verificateurHachage = new VerificateurHachage(fuuid)
    try {
        const listeParts = []
        for await (const entry of promiseReaddirp) {
            const fichierPart = entry.basename
            const position = Number(fichierPart.split('.').shift())
            listeParts.push({position, fullPath: entry.fullPath})
        }
        listeParts.sort((a,b)=>{return a.position-b.position})
        for await (const entry of listeParts) {
            const {position, fullPath} = entry
            // debug("Entry path : %O", entry);
            // const fichierPart = entry.basename
            // const position = Number(fichierPart.split('.').shift())
            debug("Traiter consignation pour item %s position %d", fuuid, position)
            const streamReader = fs.createReadStream(fullPath)
            
            let total = 0
            streamReader.on('data', chunk=>{
            // Verifier hachage
            verificateurHachage.update(chunk)
            writer.write(chunk)
            total += chunk.length
            })

            const promise = new Promise((resolve, reject)=>{
                streamReader.on('end', _=>resolve())
                streamReader.on('error', err=>reject(err))
            })

            await promise
            debug("Taille fichier %s : %d", pathFichier, total)
        }

        await verificateurHachage.verify()
        debug("Fichier %s transfere avec succes vers consignation locale", fuuid)
    } catch(err) {
        fsPromises.rm(pathFichier).catch(err=>console.error("Erreur suppression fichier : %O", err))
        throw err
    }

}

function marquerSupprime(fuuid) {
    const pathFichier = getPathFichier(fuuid)
    const pathFichierSupprime = pathFichier + '.corbeille'
    return fsPromises.rename(pathFichier, pathFichierSupprime)
}

async function parourirFichiers(callback, opts) {
    await parourirFichiersRecursif(_pathConsignation, callback, opts)
    await callback()  // Dernier appel avec aucune valeur (fin traitement)
}

async function parourirFichiersRecursif(repertoire, callback, opts) {
    opts = opts || {}
    debug("parourirFichiers %s", repertoire)
  
    const settingsReaddirp = { type: 'files', alwaysStat: true, depth: 1 }

    // const pathFichiersActifs = pathConsignation.consignationPathLocal
    // let fichiersVerifier = {}
    // let compteur = 0
    for await (const entry of readdirp(repertoire, settingsReaddirp)) {
        debug("Fichier : %O", entry)
        const { basename, fullPath, stats } = entry
        const { mtimeMs, size } = stats
        const repertoire = path.dirname(fullPath)
        const data = { filename: basename, directory: repertoire, modified: mtimeMs, size }

        // let utiliserFichier = true
        if(opts.filtre) utiliserFichier = opts.filtre(data)

        if(utiliserFichier) {
            debug("!!! FICHIER ITEM : \n%O", entry)
            await callback(data)
        }

    //     fichiersVerifier[basename] = fullPath
    //     if(++compteur >= 1000) {
    //         // Executer une batch de verification
    //         await traiterBatch(mq, pathConsignation, fichiersVerifier)

    //         // Reset
    //         fichiersVerifier = {}
    //         compteur = 0
    //     }
    }

    // if(compteur > 0) {
    //     // Derniere batch
    //     await traiterBatch(mq, pathConsignation, fichiersVerifier)
    // }

    // const handleDir = await opendir(repertoire)
    // try {
    //     let liste = await readdir(handleDir)
    //     while(liste) {
    //         // Filtrer la liste, conserver uniquement les fuuids (fichiers, enlever extension)
    //         // Appeler callback sur chaque item
    //         var infoFichiers = liste.filter(item=>{
    //             let isFile = item.attrs.isFile()
    //             if(opts.filtre) return isFile && opts.filtre(item)
    //             return isFile
    //         })
    //         if(infoFichiers && infoFichiers.length > 0) {
    //             for(let fichier of infoFichiers) {
    //                 const data = { filename: fichier.filename, directory: repertoire, modified: fichier.attrs.mtime }
    //                 await callback(data)
    //             }
    //         }
        
    //         // Parcourir recursivement tous les repertoires
    //         const directories = liste.filter(item=>item.attrs.isDirectory())
    //         for await (const directory of directories) {
    //             const subDirectory = path.join(repertoire, directory.filename)
    //             await parourirFichiersRecursif(subDirectory, callback, opts)
    //         }

    //         try {
    //             liste = await readdir(handleDir)
    //         } catch (err) {
    //             liste = false
    //         }
    //     }
    // } finally {
    //     close(handleDir)
    // }
}

module.exports = {
    init, chargerConfiguration, modifierConfiguration,
    getFichier, getInfoFichier, consignerFichier,
    marquerSupprime, parourirFichiers,
}
