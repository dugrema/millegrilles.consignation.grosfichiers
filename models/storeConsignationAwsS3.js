const debugLib = require('debug')
const debug = debugLib('consignation:store:s3')
const path = require('path')
const { Readable } = require('stream')
const lzma = require('lzma-native')

const { Upload } = require("@aws-sdk/lib-storage")
const { 
    S3Client, ListObjectsCommand, GetObjectCommand,
    AbortMultipartUploadCommand, 
    ListMultipartUploadsCommand,
    CopyObjectCommand, HeadObjectCommand, DeleteObjectCommand, DeleteObjectsCommand, 
} = require('@aws-sdk/client-s3')
const fs = require('fs')

const { Hacheur, VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')
const { chargerConfiguration, modifierConfiguration } = require('./storeConsignationLocal')

// Creer un pool de connexions a reutiliser
const AWS_API_VERSION = '2006-03-01',
      MAX_PAGES = 50_000,
      BATCH_SIZE_MIN = 1024 * 1024 * 5,
      BATCH_SIZE_MAX = 1024 * 1024 * 200,
      BATCH_SIZE_RECOMMENDED = 1024 * 1024 * 30,
      INTERVALLE_CLEANUP_MULTIPART_UPLOAD = 1000 * 60 * 60 * 12,
      CONST_EXPIRATION_ORPHELINS = 86_400_000 * 3
      
let _s3_client = null,
    _s3_bucket = null,
    _s3_bucket_backup = null,
    _urlDownload = null,
    _upload_en_cours = false,
    _dernier_cleanup_multipart = 0

function toStream(bytes) {
    const byteReader = new Readable()
    byteReader._read = () => {}
    
    byteReader.push(bytes)
    byteReader.push(null)

    return byteReader
}

async function init(params) {
    params = params || {}
    const {
        url_download,
        s3_access_key_id,
        s3_bucket,
        s3_bucket_backup,
        s3_endpoint,
        s3_region,
        data_dechiffre,
    } = params
    const { s3_secret_access_key } = data_dechiffre

    // Validation presence des parametres
    if(!url_download) throw new Error("Parametre url_download manquant")
    if(!s3_access_key_id) throw new Error("Parametre s3_access_key_id manquant")
    if(!s3_secret_access_key) throw new Error("Parametre s3_secret_access_key manquant")
    if(!s3_bucket) throw new Error("Parametre s3_bucket manquant")
    // if(!s3_bucket_backup) throw new Error("Parametre s3_bucket_backup manquant")
    if(!s3_endpoint && !s3_region) throw new Error("Parametre s3_endpoint ou s3_region manquant")

    // Conserver params de configuration
    _urlDownload = new URL(''+url_download).href
    _s3_bucket = s3_bucket
    _s3_bucket_backup = s3_bucket_backup || s3_bucket

    const configurationAws = {
        apiVersion: AWS_API_VERSION,
        credentials: {
          accessKeyId: s3_access_key_id,
          secretAccessKey: s3_secret_access_key,
        },
    }
    if(s3_region) configurationAws.region = s3_region
    if(s3_endpoint) configurationAws.endpoint = s3_endpoint
    
    _s3_client = new S3Client(configurationAws)

    // Tester connexion en nettoyant uploads incomplets
    await abortMultipartUploads()
}

function fermer() {
}

/** Supprimer tous les uploads incomplets. */
async function abortMultipartUploads(opts) {
    opts = opts || {}
    const bucket = opts.bucket || _s3_bucket

    debug("abortMultipartUploads Debut")

    const command = new ListMultipartUploadsCommand({
        Bucket: bucket,
    })
    const reponseListe = await _s3_client.send(command)

    const uploads = reponseListe.Uploads
    debug("abortMultipartUploads uploads ", uploads)
    if(uploads) {
        for await(let u of uploads) {
            const command = new AbortMultipartUploadCommand({
                Bucket: bucket,
                Key: u.Key,
                UploadId: u.UploadId,
            })
            debug("abortMultipartUploads command ", command)
            const response = await _s3_client.send(command)
            debug("abortMultipartUpload Retirer upload incomplet de ", response.Key)
        }
    }

    debug("abortMultipartUploads Complete")
}

async function getInfoFichier(fuuid, opts) {
    opts = opts || {}
    const prefix = opts.prefix || 'c/'
    const bucket = opts.bucket || _s3_bucket
    const keyPath = path.join(prefix, fuuid)

    const url = new URL(_urlDownload)
    url.pathname = path.join(url.pathname, fuuid)
    const fileRedirect = url.href

    try {
        const command = new HeadObjectCommand({
            Bucket: bucket,
            Key: keyPath,
        })

        const reponse = await _s3_client.send(command)
        debug("getInfoFichier Reponse ", reponse)
        const stat = {
            mTimeMs: reponse.LastModified.getTime(),
            mtime: reponse.LastModified,
            size: reponse.ContentLength,
        }

        return { stat, remoteFilePath: keyPath, fileRedirect }
    } catch(err) {
        throw err
        // const metadata = err['$metadata']
        // if(!metadata) throw err
        // const httpStatusCode = metadata.httpStatusCode
        // if(httpStatusCode === 404) {
        //     // if(opts.recover === true)  {
        //     //     // Verifier si le fichier est encore disponible
        //     //     return recoverFichierSupprime(fuuid)
        //     // }
        //     return null
        // }
        // else throw err
    }
}

async function renameFichier(fichierOld, fichierNew, opts) {
    opts = opts || {}
    const bucket = opts.bucket || _s3_bucket
    const commandeCopy = new CopyObjectCommand({
        Bucket: bucket,
        Key: fichierNew,
        ACL: 'public-read',
        CopySource: path.join(bucket, fichierOld),
    })
    const resultatCopie = await _s3_client.send(commandeCopy)
    debug("renameFichier Resultat copie ", resultatCopie)

    const commandeDeleteOld = new DeleteObjectCommand({
        Bucket: bucket,
        Key: fichierOld,
    })
    const resultatDelete = await _s3_client.send(commandeDeleteOld)
    debug("renameFichier Resultat cleanup ", resultatDelete)
}

async function reactiverFichier(fuuid) {
    debug("reactiverFichier %s", fuuid)
    const keyFile = path.join('c', fuuid),
          keyFileCorbeille = path.join('o', fuuid),
          keyFileArchives = path.join('a', fuuid)

    try {
        const info = await getInfoFichier(fuuid)
        debug("Reactivation %s info %O", fuuid, info) 
        return info
    } catch(err) {
        // Ok, le fichier n'existe pas deja dans c/
    }
    
    try {
        await renameFichier(keyFileCorbeille, keyFile)
        const info = await getInfoFichier(fuuid)
        debug("Reactivation %s info %O", fuuid, info) 
        return info
    } catch(err) {
        try {
            await renameFichier(keyFileArchives, keyFile)
            return await getInfoFichier(fuuid)
        } catch(err) {
          throw err
        }
    }
}

async function purgerOrphelinsExpires() {
    const expire = new Date().getTime() - CONST_EXPIRATION_ORPHELINS
    const bucketOrphelins = _s3_bucket
    const bucketParams = {
        Bucket: bucketOrphelins,
        MaxKeys: 1000,
        Prefix: 'o/',  // Path des orphelins
    }
    const callback = async info => {
        if(!info) return  // Dernier callback (vide, termine)

        debug("Info orphelin : ", info)
        if(info.modified < expire) {
            debug("Supprimer orphelin %s (expire)", info.Key)
            const commandeDeleteOld = new DeleteObjectCommand({
                Bucket: bucketOrphelins,
                Key: info.Key,
            })
            await _s3_client.send(commandeDeleteOld)
        }
    }
    await _parcourir(bucketParams, callback)
}

async function consignerFichier(pathFichierStaging, fuuid) {
    debug("Consigner fichier fuuid %s", fuuid)

    try {
        const pathFichierLocal = path.join(pathFichierStaging, fuuid)

        // Charger information fichier pur obtenir la taille
        _upload_en_cours = true
        debug("consignerFichier Upload simple vers AWS pour %s", fuuid)
        await uploadSimple(fuuid, pathFichierLocal, _s3_bucket, {prefix: 'c/'})

        // Verifier si on split les fichiers (>200M)
        // if(tailleFichier <= BATCH_SIZE_MAX) {
        //     debug("consignerFichier Upload simple vers AWS pour %s", fuuid)
        //     await uploadSimple(fuuid, pathFichierLocal, _s3_bucket, {prefix: 'c/'})
        // } else {
        //     debug("consignerFichier Upload multipart vers AWS pour %s", fuuid)
        //     await uploadMultipart(fuuid, pathFichierLocal, _s3_bucket, {prefix: 'c/'})
        // }
    } finally {
        _upload_en_cours = false
    }

}

async function uploadSimple(fuuid, pathFichier, bucket, opts) {
    opts = opts || {}
    const keyPrefix = opts.prefix || 'c/'
    const fileStream = fs.createReadStream(pathFichier)

    const pathKey = path.join(keyPrefix, fuuid)

    const parallelUploads3 = new Upload({
        client: _s3_client,
        params: { 
            Bucket: bucket, 
            Key: pathKey, 
            Body: fileStream,
            ACL: 'public-read',
        },
        tags: [
            /*...*/
        ], // optional tags
        queueSize: 1, // optional concurrency configuration
        partSize: BATCH_SIZE_RECOMMENDED,
        leavePartsOnError: false, // optional manually handle dropped parts
    })
    
    parallelUploads3.on("httpUploadProgress", (progress) => {
        debug(progress)
    });
    
    await parallelUploads3.done()
}

// async function uploadMultipart(fuuid, listeParts, bucket, opts) {
//     const keyPrefix = opts.prefix || 'c/'
//     const pathKey = path.join(keyPrefix, fuuid)

//     const localPartStat = await fsPromises.stat(listeParts[0].fullPath)
//     debug("Local part stat : ", localPartStat)
//     const localPartSize = localPartStat.size
//     if(localPartSize < BATCH_SIZE_MIN) {
//         // Combiner parts en multistreams si individuellement
//         const numPartsStream = Math.ceil(BATCH_SIZE_MIN / localPartSize)
//         debug("Combiner parts de %d bytes en groupes de %d parts (min %d)", localPartSize, numPartsStream, BATCH_SIZE_MIN)
//         const partsOriginal = listeParts
//         listeParts = []
//         while(partsOriginal.length > 0) {
//             let subpart = []
//             listeParts.push(subpart)
//             for(let i=0; i<numPartsStream; i++) {
//                 const nextPart = partsOriginal.shift()
//                 if(!nextPart) break
//                 subpart.push(nextPart)
//             }
//         }
//     } else if(localPartSize > BATCH_SIZE_MAX) {
//         throw new Error("part > MAX_PART_SIZE. Not supported")
//     }

//     debug("Parts a uploader : ", listeParts)

//     const command = new CreateMultipartUploadCommand({
//         Bucket: bucket,
//         Key: pathKey,
//     })
//     const response = await _s3_client.send(command)
//     debug("multipartUploadFile ", response)

//     const etags = []
//     const uploadId = response.UploadId
//     let partNumberCount = 1
//     let tmpFolder = null,
//         tmpFile = null

//     try {
//         for await (const p of listeParts) {
//             let partNumber = partNumberCount++
//             let stream = null
//             if(Array.isArray(p)) {
//                 debug("Multistream upload de ", p)
//                 const ms = new MultiStream(p.map(item=>fs.createReadStream(item.fullPath)))
//                 if(!tmpFolder) {
//                     tmpFolder = await tmpPromises.dir()
//                     tmpFile = path.join(tmpFolder.path, 'tmp.work')
//                     debug("TMP Folder : %s, tmpFile %s", tmpFolder, tmpFile)
//                 }
//                 const writer = fs.createWriteStream(tmpFile)
//                 await new Promise((resolve, reject) => {
//                     writer.on('close', resolve)
//                     writer.on('error', reject)
//                     ms.pipe(writer)
//                 })
//                 stream = fs.createReadStream(tmpFile)
//             } else {
//                 debug("Single part upload de ", p)
//                 stream = fs.createReadStream(p.fullPath)
//             }

//             const commandUpload = new UploadPartCommand({
//                 Bucket: bucket,
//                 Key: pathKey,
//                 Body: stream,
//                 UploadId: uploadId,
//                 PartNumber: partNumber,
//             })

//             const responseUpload = await _s3_client.send(commandUpload)
//             etags.push({ETag: responseUpload.ETag, PartNumber: partNumber})
//             debug("multipartUploadFile part %d: %O", partNumber, responseUpload)
//         }
//     } catch(err) {
//         console.error("Erreur upload multiparts ", err)
//     } finally {
//         if(tmpFile !== null) await fsPromises.unlink(tmpFile)
//         if(tmpFolder !== null) tmpFolder.cleanup()
//     }

//     debug("Etags : ", etags)

//     const commandeComplete = new CompleteMultipartUploadCommand({
//         Bucket: bucket,
//         Key: pathKey,
//         UploadId: uploadId,
//         MultipartUpload: {
//             Parts: etags,
//         },
//     })
//     const responseComplete = await _s3_client.send(commandeComplete)
//     debug("multipartUploadFile Complete ", responseComplete)

//     const commandeAcl = new PutObjectAclCommand({
//         Bucket: bucket,
//         Key: pathKey,
//         ACL: 'public-read'
//     })
//     const responseAcl = await _s3_client.send(commandeAcl)
//     debug("multipartUploadFile ACL ", responseAcl)
// }

async function getFichierStream(fuuid, opts) {
    opts = opts || {}
    debug('getFichierStream ', fuuid)

    const prefix = opts.prefix || 'c/'
    const bucket = opts.bucket || _s3_bucket

    try {
        const keyPath = path.join(prefix, fuuid)
        const command = new GetObjectCommand({
            Bucket: bucket,
            Key: keyPath
        })
        const response = await _s3_client.send(command)
        try {
            return response.Body
        } catch(err) {
            console.error(new Date() + " getFichierStream ERROR Download error, destroy stream")
            await response.Body.destroy()
            throw err
        }
    } catch(err) {
        console.error(new Date() + " getFichierStream ERROR Error get test file", err)
    }
}

async function marquerOrphelin(fuuid) {
    const keyFile = path.join('c/', fuuid)
    const orphelinKeyFile = path.join('o/', fuuid)
    await renameFichier(keyFile, orphelinKeyFile)
}

async function archiverFichier(fuuid) {
    const keyFile = path.join('c/', fuuid)
    const orphelinKeyFile = path.join('a/', fuuid)
    await renameFichier(keyFile, orphelinKeyFile)
}


async function _parcourir(bucketParams, callback, opts) {
    debug("_parcourir: Faire lecture AWS S3 sous %s", bucketParams.Bucket)
    let isTruncated = true
    let count = 0
    while(isTruncated === true && count++ < MAX_PAGES) {
        const command = new ListObjectsCommand(bucketParams)
        const response = await _s3_client.send(command)

        const contents = response.Contents
        if(contents) {
            debug("_parcourir Response %d items", contents.length)
            for await (let f of response.Contents) {
                const pathFichier = path.parse(f.Key)
                const data = { 
                    Key: f.Key,
                    filename: pathFichier.base, 
                    directory: pathFichier.dir, 
                    modified: f.LastModified.getTime(), 
                    size: f.Size
                }
                await callback(data)
            }
        } else {
            debug("_parcourir No contents")
        }
        isTruncated = response.IsTruncated
        bucketParams.Marker = response.NextMarker
    }
    
    await callback()  // Dernier call, vide
}

async function parcourirFichiers(callback, opts) {
    debug("Parcourir fichiers")
    
    const bucketParams = {
        Bucket: _s3_bucket,
        MaxKeys: 1000,
        Prefix: 'c/',  // Path de consignation
    }

    return _parcourir(bucketParams, callback, opts)
}

async function parcourirBackup(callback, opts) {
    debug("Parcourir backup")
    opts = opts || {}
    const prefix = opts.prefix || 'b/transactions'
    const bucketParams = {
        Bucket: _s3_bucket,
        MaxKeys: 1000,
        Prefix: prefix,
    }

    return _parcourir(bucketParams, callback, opts)
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

async function rotationBackupTransactions() {
    debug("rotationBackupTransactions")

    const maxArchives = 2  // transaction.IDX, valeurs superieures vont etre supprimees

    const regex = /^b\/transactions(\.([0-9]+))?(.*)$/
    const dictFichiersExistants = {}
    await parcourirBackup(item=>{
        if(!item) return  // Ok, dernier item
        debug("rotationBackupTransactions Backup item : ", item)
        const key = item.Key
        const result = key.match(regex)
        console.debug("Result ", result)
        const groupe = Number.parseInt(result[2]) || 0
        const subPath = result[3]
        const dictGroupe = dictFichiersExistants[''+groupe] || {groupe, liste: []}
        dictGroupe.liste.push({groupe, item, key, subPath})
        dictFichiersExistants[''+groupe] = dictGroupe
    }, {prefix: 'b/transactions'})

    // Faire une liste flat, trier par ordre inverse de groupe
    const listeFichiers = Object.values(dictFichiersExistants).reduce((acc, groupe)=>acc.concat(groupe.liste), [])
    listeFichiers.sort((a,b)=>b.groupe-a.groupe)

    // Supprimer archives
    if(listeFichiers.length > 0) {
        
        // Supprimer les archives expirees
        const supprimer = []
        for await (const fichier of listeFichiers) {
            const {groupe, item, key, subPath} = fichier
            if(groupe >= maxArchives) {
                supprimer.push({Key: key})
            }
        }
        if(supprimer.length > 0) {
            const params = {
                Bucket: _s3_bucket_backup,
                Delete: { Objects: supprimer }
            }
            debug("rotationBackupTransactions Params ", params)
            const command = new DeleteObjectsCommand(params)
            const reponse = await _s3_client.send(command)
            debug("rotationBackupTransactions Reponse delete backups ", reponse)
        }

        // Faire rotation des archives en ordre decroissant
        for await (const fichier of listeFichiers) {
            const {groupe, item, key, subPath} = fichier
            if(groupe < maxArchives) {
                const nouvelleKey = path.join(`b/transactions.${''+(groupe+1)}`, subPath)
                debug("Renommer archive %s -> %s", key, nouvelleKey)
                await renameFichier(key, nouvelleKey, {bucket: _s3_bucket_backup})
            }
        }

    } else {
        debug("rotationBackupTransactions Aucuns fichiers de backup courant, on conserve archives (si presentes)")
    }
}

async function getFichiersBackupTransactionsCourant(mq, replyTo) {
    throw new Error('not implemented')
}

async function getBackupTransaction(pathBackupTransaction) {
    const readStream = await getBackupTransactionStream(pathBackupTransaction)
    try {
        let contenu = await lzma.decompress(readStream)

        debug("Contenu archive str : %O", contenu)
        contenu = JSON.parse(contenu)

        debug("Contenu archive : %O", contenu)
        return contenu
    } catch(err) {
        readStream.destroy()
        throw err
    }
}

async function getBackupTransactionStream(pathBackupTransaction) {
    debug('getBackupTransactionStream path ', pathBackupTransaction)
    try {
        const keyPath = path.join('b/transactions', pathBackupTransaction)
        const command = new GetObjectCommand({
            Bucket: _s3_bucket_backup,
            Key: keyPath
        })
        const response = await _s3_client.send(command)
        try {
            debug("getBackupTransactionStream response : ", response)
            // const writer = fs.createWriteStream('./output.txt')
            // response.Body.pipe(writer)
            return response.Body
        } catch(err) {
            console.error("Download error, destroy stream")
            await response.Body.destroy()
            throw err
        }
    } catch(err) {
        console.error("Error get test file", err)
    }
}

async function pipeBackupTransactionStream(pathFichier, stream, opts) {
    opts = opts || {}
    const keyPrefix = opts.prefix || 'b/transactions'
    const pathKey = path.join(keyPrefix, pathFichier)

    const parallelUploads3 = new Upload({
        client: _s3_client,
        params: { 
            Bucket: _s3_bucket_backup, 
            Key: pathKey, 
            Body: stream,
        },
        tags: [
            /*...*/
        ], // optional tags
        queueSize: 1, // optional concurrency configuration
        partSize: BATCH_SIZE_RECOMMENDED,
        leavePartsOnError: false, // optional manually handle dropped parts
    })
    
    parallelUploads3.on("httpUploadProgress", (progress) => {
        debug(progress)
    });
    
    await parallelUploads3.done()
}

async function deleteBackupTransaction(pathBackupTransaction) {
    const commande = new DeleteObjectCommand({
        Bucket: _s3_bucket_backup,
        Key: path.join('b', pathBackupTransaction),
    })
    const resultatDelete = await _s3_client.send(commande)
    debug("deleteBackupTransaction Resultat cleanup ", resultatDelete)
}

async function entretien() {
    debug("Entretien debut")

    const now = new Date().getTime()

    if(!_upload_en_cours && now > _dernier_cleanup_multipart + INTERVALLE_CLEANUP_MULTIPART_UPLOAD) {
        debug("Entretien abortMultipartUploads")
        _dernier_cleanup_multipart = now
        try {
            await abortMultipartUploads()
        } catch(err) {
            console.error(new Date() + ' Erreur abort multiparts ', err)
        }
    }

    debug("Entretien fin")
}

module.exports = {
    init, fermer,
    chargerConfiguration, modifierConfiguration,

    getInfoFichier, getFichierStream, 
    consignerFichier, marquerOrphelin, purgerOrphelinsExpires, archiverFichier, reactiverFichier,
    parcourirFichiers,

    entretien,

    // Backup
    parcourirBackup,
    sauvegarderBackupTransactions, rotationBackupTransactions,
    getFichiersBackupTransactionsCourant, getBackupTransaction,
    getBackupTransactionStream, pipeBackupTransactionStream, deleteBackupTransaction,
}
