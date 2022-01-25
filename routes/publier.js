const debug = require('debug')('millegrilles:fichiers:publier')
const express = require('express')
const bodyParser = require('body-parser')
const path = require('path')
// const fs = require('fs')
const fsPromises = require('fs/promises')
const multer = require('multer')
const {v4: uuidv4} = require('uuid')
// const readdirp = require('readdirp')
// const FormData = require('form-data')
// const { addRepertoire: ipfsPublish, getPins } = require('../util/ipfs')

// const { connecterSSH, preparerSftp, listerConsignation: _listerConsignationSftp } = require('../util/ssh')
// const { preparerConnexionS3, addRepertoire: awss3Publish, listerConsignation: _listerConsignationAwsS3 } = require('../util/awss3')

// const ipfsHost = process.env.IPFS_HOST || 'http://ipfs:5001'
// initIpfs(ipfsHost)

var _mq = null,
    _pathConsignation = null

function init(mq, pathConsignation) {
  _mq = mq
  _pathConsignation = pathConsignation

  const pathStaging = _pathConsignation.consignationPathUploadStaging
  const multerMiddleware = multer({dest: pathStaging, preservePath: true})

  const bodyParserJson = bodyParser.json()

  const route = express.Router()

  route.put('/publier/repertoire', multerMiddleware.array('files', 1000), publierRepertoire)
  // route.put('/publier/fichierIpns', multerMiddleware.array('files', 1), publierFichierIpns)
  route.post('/publier/listerConsignationSftp', bodyParserJson, listerConsignationSftp)
  // route.post('/publier/listerPinsIpfs', listerPinsIpfs)
  route.post('/publier/listerConsignationAwsS3', bodyParserJson, listerConsignationAwsS3)

  return route
}

async function publierRepertoire(req, res, next) {
  try {
    /* Recoit et publie un repertoire */
    debug("publier.publierRepertoire %O, files: %O, body: %O", req.headers, req.files, req.body)

    // Recreer la structure de fichiers dans un sous-repertoie de staging
    const uuidRepTemporaire = uuidv4()
    const repTemporaire = _pathConsignation.consignationPathUploadStaging + '/' + uuidRepTemporaire
    debug("Recreer structure de repertoire sous %s", repTemporaire)

    const pathMimetypes = {}
    for(let idx in req.files) {
      const file = req.files[idx]
      pathMimetypes[file.originalname] = file.mimetype
      const filePath = path.join(repTemporaire, file.originalname)
      const fileDir = path.dirname(filePath)

      debug("Creer sous-repertoire temporaire %s", fileDir)
      await fsPromises.mkdir(fileDir, {recursive: true})

      debug("Deplacer fichier %s", filePath)
      await fsPromises.rename(file.path, filePath)
    }
    debug("Structure de repertoire recree sous %s", repTemporaire)

    // Extraire information des methodes de publication
    const cdns = JSON.parse(req.body.cdns)
    const identificateur_document = JSON.parse(req.body.identificateur_document)
    const maxAge = req.body.max_age || 86400,
          contentEncoding = req.body.content_encoding,
          securite = req.body.securite,
          fichierUnique = req.body.fichier_unique

    // Demarrer publication selon methodes demandees
    for await (const cdn of cdns) {
      const commande = {
        ...cdn, contentEncoding, maxAge, pathMimetypes,
        identificateur_document, securite,
        repertoireStaging: repTemporaire,
      }
      if(cdn.type_cdn === 'sftp') {
        debug("Publier repertoire avec SFTP : %O", cdn)
        const domaineAction = 'commande.fichiers.publierRepertoireSftp'
        _mq.transmettreCommande(domaineAction, commande, {nowait: true})
        debug("Commande publier SFTP emise")
      }
      // if(cdn.type_cdn === 'ipfs') {
      //   if(fichierUnique) {
      //     const file = req.files[0]
      //     const filePath = path.join(repTemporaire, file.originalname)
      //     commande.fichierUnique = filePath
      //   }
      //   debug("Publier repertoire avec IPFS : %O", commande)
      //   const domaineAction = 'commande.fichiers.publierRepertoireIpfs'
      //   _mq.transmettreCommande(domaineAction, commande, {nowait: true})
      //   debug("Commande publier IPFS emise")
      // }
      if(cdn.type_cdn === 'awss3') {
        debug("Publier repertoire avec AWS S3 : %O", cdn)
        const domaineAction = 'commande.fichiers.publierRepertoireAwsS3'
        _mq.transmettreCommande(domaineAction, commande, {nowait: true})
        debug("Commande publier AWS S3 emise")
      }
    }

    res.sendStatus(200)
  } catch(err) {
    console.error("publier.publierRepertoire: Erreur %O", err)
    res.sendStatus(500)
  }
}

async function listerConsignationSftp(req, res, next) {
  debug("publier.listerConsignation : %O", req.body)

  const {host, port, username, repertoireRemote} = req.body

  const conn = await connecterSSH(host, port, username)
  const sftp = await preparerSftp(conn)
  debug("Connexion SSH et SFTP OK")

  // On va streamer la reponse
  try {
    res.status(200)
    await _listerConsignationSftp(sftp, repertoireRemote, {res})
  } catch(err) {
    console.error("ERROR publier.listerConsignation %O", err)
    res.send("!!!ERR!!!")
  } finally {
    res.end()
  }

  // res.send({ok: true})
}

// async function listerPinsIpfs(req, res, next) {
//   debug('publier.listerPinsIpfs')
//
//   try {
//     res.status(200)
//     getPins(res)
//   } catch(err) {
//     console.error("ERROR publier.listerPinsIpfs %O", err)
//     res.sendStatus(500)
//   }
// }

async function listerConsignationAwsS3(req, res, next) {
  debug("publier.listerConsignationAwsS3 %O", req.body)

  const {bucketRegion, credentialsAccessKeyId, secretAccessKey_chiffre, permission, bucketName, bucketDirfichier} = JSON.parse(req.body.data)
  const secretKeyInfo = {secretAccessKey: secretAccessKey_chiffre, permission}
  const s3 = await preparerConnexionS3(_mq, bucketRegion, credentialsAccessKeyId, secretKeyInfo)

  // On va streamer la reponse
  try {
    res.status(200)
    await _listerConsignationAwsS3(s3, bucketName, bucketDirfichier, {res})
  } catch(err) {
    console.error("ERROR publier.listerConsignationAwsS3 %O", err)
    res.send("!!!ERR!!!")
  } finally {
    res.end()
  }
}

// async function publierFichierIpns(req, res, next) {
//   debug("publier.publierFichierIpns\n%O\nfichiers: %O", req.body, req.files)
//
//   const fichier = req.files[0]
//   const commande = {
//     ...req.body, fichier,
//   }
//
//   debug("Publier fichier avec IPFS")
//   const domaineAction = 'commande.fichiers.publierFichierIpns'
//   _mq.transmettreCommande(domaineAction, commande, {nowait: true})
//   debug("Commande publier IPFS emise")
//
//   res.sendStatus(200)
// }

module.exports = {init}
