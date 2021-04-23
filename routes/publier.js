const debug = require('debug')('millegrilles:fichiers:publier')
const express = require('express')
const bodyParser = require('body-parser')
const path = require('path')
const fs = require('fs')
const fsPromises = require('fs/promises')
const multer = require('multer')
const {v4: uuidv4} = require('uuid')
const readdirp = require('readdirp')
const FormData = require('form-data')
const { addRepertoire: ipfsPublish, getPins } = require('../util/ipfs')
const { connecterSSH, preparerSftp, listerConsignation: _listerConsignation } = require('../util/ssh')
const { preparerConnexionS3, addRepertoire: awss3Publish } = require('../util/awss3')

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
  route.post('/publier/listerConsignationSftp', bodyParserJson, listerConsignationSftp)
  route.post('/publier/listerPinsIpfs', listerPinsIpfs)

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

    for(let idx in req.files) {
      const file = req.files[idx]
      const filePath = path.join(repTemporaire, file.originalname)
      const fileDir = path.dirname(filePath)

      debug("Creer sous-repertoire temporaire %s", fileDir)
      await fsPromises.mkdir(fileDir, {recursive: true})

      debug("Deplacer fichier %s", filePath)
      await fsPromises.rename(file.path, filePath)
    }
    debug("Structure de repertoire recree sous %s", repTemporaire)

    // Demarrer publication selon methodes demandees
    if(req.body.publierSsh) {
      debug("Publier repertoire avec SSH : %O", req.body.publierSsh)
      const paramsSsh = JSON.parse(req.body.publierSsh)
      const commande = {
        ...paramsSsh,
        repertoireStaging: repTemporaire,
      }
      const domaineAction = 'commande.fichiers.publierRepertoireSftp'
      _mq.transmettreCommande(domaineAction, commande, {nowait: true})
      debug("Commande publier SSH emise")
    }
    if(req.body.publierIpfs) {
      debug("Publier repertoire avec IPFS")
      const paramsIpfs = JSON.parse(req.body.publierIpfs)
      const commande = {
        ...paramsIpfs,
        repertoireStaging: repTemporaire,
      }
      const domaineAction = 'commande.fichiers.publierRepertoireIpfs'
      _mq.transmettreCommande(domaineAction, commande, {nowait: true})
      debug("Commande publier IPFS emise")
    }
    if(req.body.publierAwsS3) {
      debug("Publier repertoire avec AWS S3 : %O", req.body.publierAwsS3)
      // const {bucketRegion, credentialsAccessKeyId, secretAccessKey, bucketName, bucketDirfichier} = paramsAwsS3
      //
      // // Connecter AWS S3
      // const s3 = await preparerConnexionS3(bucketRegion, credentialsAccessKeyId, secretAccessKey)
      // const reponse = await awss3Publish(s3, repTemporaire, bucketName, {bucketDirfichier})
      // debug("Fin upload AWS S3 : %O", reponse)

      const paramsAwsS3 = JSON.parse(req.body.publierAwsS3)
      const commande = {
        ...paramsAwsS3,
        repertoireStaging: repTemporaire,
      }
      const domaineAction = 'commande.fichiers.publierRepertoireAwsS3'
      _mq.transmettreCommande(domaineAction, commande, {nowait: true})
      debug("Commande publier AWS S3 emise")
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
    await _listerConsignation(sftp, repertoireRemote, {res})
  } catch(err) {
    console.error("ERROR publier.listerConsignation %O", err)
    res.send("!!!ERR!!!")
  } finally {
    res.end()
  }

  // res.send({ok: true})
}

async function listerPinsIpfs(req, res, next) {
  debug('publier.listerPinsIpfs')

  try {
    res.status(200)
    getPins(res)
  } catch(err) {
    console.error("ERROR publier.listerPinsIpfs %O", err)
    res.sendStatus(500)
  }
}

module.exports = {init}
