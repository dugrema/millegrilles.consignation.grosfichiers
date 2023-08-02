const debug = require('debug')('routes:backup')
const path = require('path')
const express = require('express')
const fs = require('fs')
const fsPromises = require('fs/promises')
const zlib = require('zlib')

const forgecommon = require('@dugrema/millegrilles.utiljs/src/forgecommon')

const PATH_BACKUP = '/var/opt/millegrilles/consignation/backupConsignation',
      PATH_STAGING = '/var/opt/millegrilles/consignation/staging/backupConsignation'

function init(mq, consignationManager, opts) {
  opts = opts || {}

  const route = express.Router()

  route.use((req, res, next)=>{
    req.consignationManager = consignationManager
    next()
  })

  // Fichiers sync primaire
  route.use(headersNoCache)
  route.post('/verifierFichiers', express.json(), verifierBackup)
  route.put('/upload/:uuid_backup/:domaine/:nomfichier', recevoirFichier)
  route.get('/download/:uuid_backup/:domaine/:nomfichier', downloaderFichier)

  debug("Route /fichiers_transfert/backup initialisee")

  fsPromises.mkdir(PATH_STAGING, {recursive: true})
    .catch(err=>console.error("Erreur creation path staging %s : %O", PATH_STAGING, err))

  return route
}

function headersNoCache(req, res, next) {
    res.setHeader('Cache-Control', 'no-store')
    next()
}

async function verifierBackup(req, res) {
    const body = req.body
    debug("verifierBackup verifier %s", body)

    const uuidBackup = body.uuid_backup,
          domaine = body.domaine

    const reponse = body.fichiers.reduce((acc, item)=>{
        acc[item] = false
        return acc
    }, {})
    const callback = fichier => {
        if(!fichier) return  // Termine
        debug("VerifierBackup callback %O", fichier)
        if(reponse[fichier.filename]===false) {
            reponse[fichier.filename] = true
        }
    }

    await req.consignationManager.parcourirBackup(callback, {uuid_backup: uuidBackup, domaine})

    return res.status(200).send(reponse)
}

async function recevoirFichier(req, res) {

    const consignationManager = req.consignationManager
    const params = req.params
    debug("recevoirFichier Backup %s", params)
    const { nomfichier, domaine, uuid_backup } = params
    if(!nomfichier || !domaine || !uuid_backup ) {
        console.warn("recevoirFichier Mauvais requete, params manquants (recu: %O)", params)
        return res.status(400).send('Parametres manquants')
    }

    // Verifier si le fichier existe deja
    try {
        await consignationManager.getInfoFichier(`${uuid_backup}/${domaine}/${nomfichier}`, {backup: true})
        return res.sendStatus(409)
    } catch(err) {
        if(err.code === 'ENOENT') {
            // Ok, le fichier n'existe pas
        } else {
            throw err
        }
    }

    // Recevoir le fichier dans staging
    const pathReception = path.join(PATH_STAGING, params.nomfichier)
    try {
        const writeStream = fs.createWriteStream(pathReception)
        await new Promise((resolve, reject)=>{
            writeStream.on('error', reject)
            writeStream.on('close', resolve)
            try { req.pipe(writeStream) } catch(err) { reject(err) }
        })

        // Valider le catalogue recu
        try {
            let readStream = fs.createReadStream(pathReception)
            let contenu = ''
            await new Promise((resolve, reject)=>{
                readStream.on('error', reject)

                if(nomfichier.endsWith('.gz')) {
                    readStream = readStream.pipe(zlib.createGunzip())
                    readStream.on('error', reject)
                }

                readStream.on('close', resolve)
                readStream.on('data', chunk => { contenu += chunk })
                readStream.read()
            })

            const catalogue = JSON.parse(contenu)
            const pki = req.amqpdao.pki
            try { 
                const resultat = await pki.verifierMessage(catalogue) 
                const extensions = forgecommon.extraireExtensionsMillegrille(resultat.certificat)
                if(!extensions.niveauxSecurite || extensions.niveauxSecurite.length === 0) {
                    debug("recevoirFichier ERROR catalogue invalide (certificat n'est pas systeme)")
                    return res.status(400).send("Catalogue invalide (certificat n'est pas systeme)")
                }
            } catch(err) {
                debug("recevoirFichier ERROR catalogue invalide (signature ou hachage)")
                return res.status(400).send('Catalogue invalide (signature ou hachage)')
            }

            contenu = JSON.parse(catalogue['contenu'])
            if(contenu.domaine !== domaine) {
                debug("recevoirFichier Mauvais domaine pour catalogue %s", nomfichier)
                return res.status(400).send('Catalogue invalide (mauvais domaine)')
            }
            debug("Catalogue recu OK")

            // Copier le fichier vers le repertoire de backup
            await req.consignationManager.sauvegarderBackupTransactions(uuid_backup, domaine, pathReception)

        } catch(err) {
            console.error("recevoirFichier Erreur validation catalogue recu %s : %O", pathReception, err)
            return res.sendStatus(500)
        }
    
    } finally {
        fsPromises.unlink(pathReception)
            .catch(err=>{
                if(err.code === 'ENOENT') return  // Ok, fichier deja supprime ou deplace
                console.warn(new Date() + " recevoirFichier Erreur suppression fichier temp %s", err)
            })
    }

    return res.sendStatus(200)
}

async function downloaderFichier(req, res) {

    const params = req.params
    debug("recevoirFichier Backup %s", params)
    const { nomfichier, domaine, uuid_backup } = params
    if(!nomfichier || !domaine || !uuid_backup ) {
        console.warn("recevoirFichier Mauvais requete, params manquants (recu: %O)", params)
        return res.status(400).send('Parametres manquants')
    }


    return res.sendStatus(500)  // TODO
}

module.exports = init
