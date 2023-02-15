const debugLib = require('debug')
const debug = debugLib('consignation:store:s3')
const debugTrace = debugLib('consignation:store:s3Trace')
const path = require('path')
const { Readable } = require('stream')
const lzma = require('lzma-native')
const readdirp = require('readdirp')

// const S3 = require('aws-sdk/clients/s3')
const { Upload } = require("@aws-sdk/lib-storage")
const { 
    S3Client, ListObjectsCommand, GetObjectCommand,
    CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand, 
    ListMultipartUploadsCommand, PutObjectAclCommand,
} = require('@aws-sdk/client-s3')
const fs = require('fs')
const fsPromises = require('fs/promises')
const MultiStream = require('multistream')
const tmpPromises = require('tmp-promise')

const { Hacheur, VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')
const { chargerConfiguration, modifierConfiguration } = require('./storeConsignationLocal')

// Creer un pool de connexions a reutiliser
const AWS_API_VERSION = '2006-03-01',
      MAX_PAGES = 50_000,
      BATCH_SIZE_MIN = 1024 * 1024 * 5,
      BATCH_SIZE_MAX = 1024 * 1024 * 200,
      BATCH_SIZE_RECOMMENDED = 1024 * 1024 * 30
      
let _s3_client = null,
    _s3_bucket = null,
    _s3_bucket_backup = null,
    _urlDownload = null

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
}

function fermer() {
}

async function getInfoFichier(fuuid) {
    debug("getInfoFichier %s", fuuid)
    const url = new URL(_urlDownload)
    url.pathname = path.join(url.pathname, fuuid)
    const fileRedirect = url.href
    debug("File redirect : %O", fileRedirect)
    return { fileRedirect }
}

async function recoverFichierSupprime(fuuid) {
    debug("recoverFichierSupprime %s", fuuid)
    throw new Error('not implemented')
}

async function consignerFichier(pathFichierStaging, fuuid) {
    debug("Consigner fichier fuuid %s", fuuid)

    // Determiner si on a une seule .part ou plusieurs
    const promiseReaddirp = readdirp(pathFichierStaging, {
        type: 'files',
        fileFilter: '*.part',
        depth: 1,
    })
    const listeParts = []
    for await (const entry of promiseReaddirp) {
        const fichierPart = entry.basename
        const position = Number(fichierPart.split('.').shift())
        listeParts.push({position, fullPath: entry.fullPath})
    }
    listeParts.sort((a,b)=>{return a.position-b.position})

    if(listeParts.length === 1) {
        debug("consignerFichier Upload simple vers AWS pour %s", fuuid)
        await uploadSimple(fuuid, listeParts[0].fullPath, _s3_bucket, {prefix: 'c/'})
    } else {
        debug("consignerFichier Upload multipart vers AWS pour %s", fuuid)
        await uploadMultipart(fuuid, listeParts, _s3_bucket, {prefix: 'c/'})
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
        console.log(progress)
    });
    
    await parallelUploads3.done()
}

async function uploadMultipart(fuuid, listeParts, bucket, opts) {
    const keyPrefix = opts.prefix || 'c/'
    const pathKey = path.join(keyPrefix, fuuid)

    const localPartStat = await fsPromises.stat(listeParts[0].fullPath)
    debug("Local part stat : ", localPartStat)
    const localPartSize = localPartStat.size
    if(localPartSize < BATCH_SIZE_MIN) {
        // Combiner parts en multistreams si individuellement
        const numPartsStream = Math.ceil(MIN_PART_SIZE / localPartSize)
        debug("Combiner parts de %d bytes en groupes de %d parts (min %d)", localPartSize, numPartsStream, MIN_PART_SIZE)
        const partsOriginal = listeParts
        listeParts = []
        while(partsOriginal.length > 0) {
            let subpart = []
            listeParts.push(subpart)
            for(let i=0; i<numPartsStream; i++) {
                const nextPart = partsOriginal.shift()
                if(!nextPart) break
                subpart.push(nextPart)
            }
        }
    } else if(localPartSize > BATCH_SIZE_MAX) {
        throw new Error("part > MAX_PART_SIZE. Not supported")
    }

    debug("Parts a uploader : ", listeParts)

    const command = new CreateMultipartUploadCommand({
        Bucket: bucket,
        Key: pathKey,
    })
    const response = await _s3_client.send(command)
    debug("multipartUploadFile ", response)

    const etags = []
    const uploadId = response.UploadId
    let partNumberCount = 1
    let tmpFolder = null,
        tmpFile = null

    try {
        for await (const p of listeParts) {
            let partNumber = partNumberCount++
            let stream = null
            if(Array.isArray(p)) {
                debug("Multistream upload de ", p)
                const ms = new MultiStream(p.map(item=>fs.createReadStream(item.fullPath)))
                if(!tmpFolder) {
                    tmpFolder = await tmpPromises.dir()
                    tmpFile = path.join(tmpFolder.path, 'tmp.work')
                    debug("TMP Folder : %s, tmpFile %s", tmpFolder, tmpFile)
                }
                const writer = fs.createWriteStream(tmpFile)
                await new Promise((resolve, reject) => {
                    writer.on('close', resolve)
                    writer.on('error', reject)
                    ms.pipe(writer)
                })
                stream = fs.createReadStream(tmpFile)
            } else {
                debug("Single part upload de ", p)
                stream = fs.createReadStream(p.fullPath)
            }

            const commandUpload = new UploadPartCommand({
                Bucket: bucket,
                Key: pathKey,
                Body: stream,
                UploadId: uploadId,
                PartNumber: partNumber,
            })

            const responseUpload = await _s3_client.send(commandUpload)
            etags.push({ETag: responseUpload.ETag, PartNumber: partNumber})
            debug("multipartUploadFile part %d: %O", partNumber, responseUpload)
        }
    } catch(err) {
        console.error("Erreur upload multiparts ", err)
    } finally {
        if(tmpFile !== null) await fsPromises.unlink(tmpFile)
        if(tmpFolder !== null) tmpFolder.cleanup()
    }

    debug("Etags : ", etags)

    const commandeComplete = new CompleteMultipartUploadCommand({
        Bucket: bucket,
        Key: pathKey,
        UploadId: uploadId,
        MultipartUpload: {
            Parts: etags,
        },
    })
    const responseComplete = await _s3_client.send(commandeComplete)
    debug("multipartUploadFile Complete ", responseComplete)

    const commandeAcl = new PutObjectAclCommand({
        Bucket: bucket,
        Key: pathKey,
        ACL: 'public-read'
    })
    const responseAcl = await _s3_client.send(commandeAcl)
    debug("multipartUploadFile ACL ", responseAcl)
}

async function marquerSupprime(fuuid) {
    throw new Error('not implemented')
}

async function _parcourir(bucketParams, callback, opts) {
    debug("_parcourir: Faire lecture AWS S3 sous %s", bucketParams.Bucket)
    let isTruncated = true
    let count = 0
    while(isTruncated === true && count++ < MAX_PAGES) {
        const command = new ListObjectsCommand(bucketParams)
        const response = await _s3_client.send(command)

        debug("_parcourir Response ", response)
        const contents = response.Contents
        if(contents) {
            for await (let f of response.Contents) {
                const pathFichier = path.parse(f.Key)
                const data = { 
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
    const bucketParams = {
        Bucket: _s3_bucket,
        MaxKeys: 1000,
        Prefix: 'b/',  // Path de consignation
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

async function rotationBackupTransactions(message) {
    const { domaine, partition } = message
    debug("rotationBackupTransactions", domaine, partition)
    throw new Error('not implemented')
}

async function getFichiersBackupTransactionsCourant(mq, replyTo) {
    throw new Error('not implemented')
}

async function getBackupTransaction(pathBackupTransaction) {
    throw new Error('not implemented')
}

async function getBackupTransactionStream(pathBackupTransaction) {
    throw new Error('not implemented')
}

async function pipeBackupTransactionStream(pathFichier, stream) {
    throw new Error('not implemented')
}

async function deleteBackupTransaction(pathBackupTransaction) {
    throw new Error('not implemented')
}

module.exports = {
    init, fermer,
    chargerConfiguration, modifierConfiguration,
    getInfoFichier, consignerFichier, 
    marquerSupprime, recoverFichierSupprime,
    parcourirFichiers,

    // Backup
    parcourirBackup,
    sauvegarderBackupTransactions, rotationBackupTransactions,
    getFichiersBackupTransactionsCourant, getBackupTransaction,
    getBackupTransactionStream, pipeBackupTransactionStream, deleteBackupTransaction,
}
