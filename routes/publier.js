const debug = require('debug')('millegrilles:fichiers:publier')
const express = require('express')
const path = require('path')
const fsPromises = require('fs/promises')
const multer = require('multer')
const {v4: uuidv4} = require('uuid')

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

  res.sendStatus(200)
}

module.exports = {init}
