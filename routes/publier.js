const debug = require('debug')('millegrilles:fichiers:publier')
const express = require('express')
const path = require('path')
const fsPromises = require('fs/promises')
const multer = require('multer')
const {v4: uuidv4} = require('uuid')
const readdirp = require('readdirp')
const FormData = require('form-data')
// const {} = require('../util/ipfs')

var _mq = null,
    _pathConsignation = null

function init(mq, pathConsignation) {
  _mq = mq
  _pathConsignation = pathConsignation

  const pathStaging = _pathConsignation.consignationPathUploadStaging
  const multerMiddleware = multer({dest: pathStaging, preservePath: true})

  const route = express.Router()

  route.put('/publier/repertoire', multerMiddleware.array('files', 1000), publierRepertoire)

  return route
}

async function publierRepertoire(req, res, next) {
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
    debug("Publier repertoire avec SSH")
  }
  if(req.body.publierIpfs) {
    debug("Publier repertoire avec IPFS")
    const formData = new FormData()
    const cb = entry => cbPreparerIpfs(entry, formData, repTemporaire)
    const info = await preparerPublicationRepertoire(repTemporaire, cb)
    debug("Info publication repertoire avec IPFS : %O, FormData: %O", info, formData)
  }
  if(req.body.publierAwsS3) {
    debug("Publier repertoire avec AWS S3")
  }

  res.sendStatus(200)
}

async function preparerPublicationRepertoire(pathRepertoire, cbCommandeHandler) {
  /*
    Prepare l'information et commandes pour publier un repertoire
    - pathRepertoire : repertoire a publier (base exclue de la publication)
    - cbCommandeHandler : fonction (stat)=>{...} invoquee sur chaque entree du repertoire
    Retourne : {bytesTotal}
  */
  var bytesTotal = 0
  const params = {
    alwaysStat: true,
    type: 'files_directories',
  }
  for await(const entry of readdirp(pathRepertoire, params)) {
    // const stat = await fsPromises.stat(entry.fullPath)
    debug("Entry readdirp : %O", entry)
    const stats = entry.stats
    await cbCommandeHandler(entry)
    if(stats.isFile()) {
      bytesTotal += stats.size
    }
  }
  return {bytesTotal}
}

function cbPreparerIpfs(entry, formData, pathStaging) {
  /* Sert a preparer l'upload d'un repertoire vers IPFS. Append a FormData. */
  const pathRelatif = encodeURIComponent(entry.fullPath.replace(pathStaging, ''))
  debug("Ajout path relatif : %s", pathRelatif)
  if(entry.stats.isFile()) {
    formData.append('file', entry.fullPath, pathRelatif)
  } else if(entry.stats.isDirectory()) {
    var dirOptions = {
      filename: pathRelatif,
      contentType: 'application/x-directory',
      knownLength: 0
    }
    formData.append('file', '', dirOptions)
  }
}

module.exports = {init}
