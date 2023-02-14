const debugLib = require('debug')
const debug = debugLib('consignation:store:s3')
const debugTrace = debugLib('consignation:store:s3Trace')
const path = require('path')
const { Readable } = require('stream')
const lzma = require('lzma-native')

const S3 = require('aws-sdk/clients/s3')

const { Hacheur, VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')
const { chargerConfiguration, modifierConfiguration } = require('./storeConsignationLocal')

// Creer un pool de connexions a reutiliser
const AWS_API_VERSION = '2006-03-01'
// const CONNEXION_TIMEOUT = 10 * 60 * 1000  // 10 minutes

let _s3 = null,
    _s3_bucket = null,
    _s3_bucket_backup = 'backup',
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
    _s3_bucket_backup = s3_bucket_backup || 'backup'

    const configurationAws = {
        apiVersion: AWS_API_VERSION,
        credentials: {
          accessKeyId: s3_access_key_id,
          secretAccessKey: s3_secret_access_key,
        },
    }
    if(s3_region) configurationAws.region = s3_region
    if(s3_endpoint) configurationAws.endpoint = s3_endpoint
    
    _s3 = new S3(configurationAws)    
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
    throw new Error('not implemented')
}

async function marquerSupprime(fuuid) {
    throw new Error('not implemented')
}

async function parcourirFichiers(callback, opts) {
    debug("Parcourir fichiers")
    
    const paramsListing = {
        Bucket: bucketName,
        MaxKeys: 1000,
        Prefix: repertoire,
    }
    if(opts.ContinuationToken) paramsListing.ContinuationToken = opts.ContinuationToken

    debug("awss3.listerConsignation: Faire lecture AWS S3 sous %s / %s", bucketName, repertoire)
    const data = await new Promise ((resolve, reject) => {
        _s3.listObjectsV2(paramsListing, async (err, data)=>{
            if(err) {
                console.error("awss3.listerConsignation: Erreur demande liste fichiers")
                return reject(err)
            }
            resolve(data)
        })
    })

    // console.log("Listing fichiers bucket " + paramsListing.Bucket);
    for await (const contents of data.Contents) {
    // debug("Contents : %O", contents)
    const pathFichier = path.parse(contents.Key)
    // debug("Path fichier : %s", pathFichier)
    const fuuid = pathFichier.name
    // debug("Fuuid : %s", fuuid)
    const contentFichier = {
        ...contents,
        fuuid,
    }
    if(res) {
        res.write(JSON.stringify(contentFichier) + '\n')
    }
    }

    if(data.IsTruncated) {
    // debug("Continuer listing");
    opts.ContinuationToken = data.NextContinuationToken
    await listerConsignation(s3, bucketName, repertoire, opts)
    }    
}

async function parcourirBackup(callback, opts) {
    debug("Parcourir backup")
    throw new Error('not implemented')
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
