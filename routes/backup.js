const debug = require('debug')('routes:backup')
const path = require('path')
const express = require('express')
const fs = require('fs')
const fsPromises = require('fs/promises')
const zlib = require('zlib')

const forgecommon = require('@dugrema/millegrilles.utiljs/src/forgecommon')

const PATH_BACKUP = '/var/opt/millegrilles/consignation/backupConsignation'

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

  debug("Route /fichiers_transfert/backup initialisee")

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

    const pathBackups = path.join(PATH_BACKUP, uuidBackup, domaine)

    const reponse = {}
    for await (const nomFichier of body.fichiers) {
        const pathFichier = path.join(pathBackups, nomFichier)

        let present = false
        try {
            await fsPromises.stat(pathFichier)
            present = true
        } catch(err) {
            if(err.code === 'ENOENT') {
                // Ok, fichier / path absent
            } else {
                debug("Erreur verification fichier %s : %O", pathFichier, err)
            }
        }
        
        reponse[nomFichier] = present
    }

    return res.status(200).send(reponse)
}

async function recevoirFichier(req, res) {

    const params = req.params
    debug("recevoirFichier Backup %s", params)
    const { nomfichier, domaine, uuid_backup } = params
    if(!nomfichier || !domaine || !uuid_backup ) {
        console.warn("recevoirFichier Mauvais requete, params manquants (recu: %O)", params)
        return res.status(400).send('Parametres manquants')
    }

    // Verifier que le fichier n'existe pas deja
    const destinationFichierBackup = path.join(PATH_BACKUP, uuid_backup, domaine, nomfichier)
    try {
        await fsPromises.stat(destinationFichierBackup)
        console.warn("recevoirFichier Le fichier %s existe, upload refuse", destinationFichierBackup)
        return res.status(400).send('Fichier de backup existe deja')
    } catch(err) {
        if(err.code === 'ENOENT') {
            // Ok, fichier n'existe pas
        } else {
            throw err
        }
    }

    // Recevoir le fichier dans staging
    const pathReception = `/tmp/${params.nomfichier}`
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
            await deplacerFichierBackup(pathReception, destinationFichierBackup)

        } catch(err) {
            console.error("recevoirFichier Erreur validation catalogue recu %s : %O", pathReception, err)
            return res.sendStatus(500)
        }
    
    } finally {
        fsPromises.unlink(pathReception)
            .catch(err=>console.warn(new Date() + " recevoirFichier Erreur suppression fichier temp %s", err))
    }

    return res.sendStatus(200)
}

async function deplacerFichierBackup(pathSource, pathDestination) {
    const dirFichier = path.dirname(pathDestination)
    await fsPromises.mkdir(dirFichier, {recursive: true})

    try {
        // Methode simple, rename (move)
        await fsPromises.rename(pathSource, pathDestination)
    } catch(err) {
        if(err.code === 'ENOENT') {
            console.error(new Date() + " copierFichierBackup Fichier %s introuvable ", pathSource)
            throw err
        }
        debug("copierFichierBackup Erreur rename, tenter copy ", err)
        const reader = fs.createReadStream(pathSource)
        const writer = fs.createWriteStream(pathDestination)
        await new Promise((resolve, reject)=>{
            writer.on('close', resolve)
            writer.on('error', err => {
                fsPromises.unlink(pathDestination)
                    .catch(err=>console.warn("Erreur suppression fichier incomplet %s : %O", pathDestination, err))
                reject(err)
            })
            reader.pipe(writer)
        })
        fsPromises.rm(pathSource).catch(err=>console.error("Erreur suppression fichier : %O", err))
    }

}

module.exports = init
