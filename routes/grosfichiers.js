const debug = require('debug')('millegrilles:fichiers:grosfichiers')
const express = require('express');
const path = require('path');
const fs = require('fs');
const multer = require('multer')
const bodyParser = require('body-parser')

const {PathConsignation, TraitementFichier} = require('../util/traitementFichier')

// const throttle = require('@sitespeed.io/throttle');

function InitialiserGrosFichiers() {

  const router = express.Router();

  const bodyParserInstance = bodyParser.urlencoded({ extended: false })

  router.get('^/fichiers/:fuuid', downloadFichierLocal, pipeReponse)

  // router.post('*', bodyParserInstance, downloadFichierLocalChiffre)

  const multerProcessor = multer({dest: '/var/opt/millegrilles/consignation/multer'}).single('fichier')

  router.put('^/fichiers/*', multerProcessor, async (req, res, next) => {
    console.debug("nouveauFichier PUT %s,\nHeaders: %O\nFichiers: %O\nBody: %O", req.url, req.headers, req.file, req.body)

    const idmg = req.autorisationMillegrille.idmg
    const rabbitMQ = req.rabbitMQ  //fctRabbitMQParIdmg(idmg)
    const traitementFichier = new TraitementFichier(rabbitMQ)

    // Streamer fichier vers FS
    try {
      // Returns a promise
      const msg = await traitementFichier.traiterPut(req)

      response = {
        hachage: msg.hachage
      }

      res.end(JSON.stringify(response))

    } catch (err) {
      console.error(err);
      res.sendStatus(500);
    }

  })

  return router
}

function downloadFichierLocal(req, res, next) {
  debug("downloadFichierLocalChiffre methode:" + req.method + ": " + req.url);
  debug(req.headers);
  debug(req.autorisationMillegrille)

  const securite = req.headers.securite || '2.prive'
  var encrypted = false
  if(securite === '3.protege') encrypted = true

  const fuuid = req.params.fuuid

  console.debug("Fuuid : %s", fuuid)

  var contentType = req.headers.mimetype || 'application/octet-stream'
  const idmg = req.autorisationMillegrille.idmg;
  const pathConsignation = new PathConsignation({idmg})

  debug("Info idmg: %s, paths: %s", idmg, pathConsignation);

  res.setHeader('Cache-Control', 'private, max-age=604800, immutable')
  res.setHeader('Content-Type', contentType)
  res.setHeader('fuuid', fuuid)
  res.setHeader('securite', securite)

  // Note : ajouter extension fichier pour mode non-chiffre

  res.filePath = pathConsignation.trouverPathLocal(fuuid, encrypted);

  next()
}

function pipeReponse(req, res) {
  const filePath = res.filePath
  const header = res.responseHeader

  fs.stat(filePath, (err, stats)=>{
    if(err) {
      console.error(err);
      if(err.errno == -2) {
        res.sendStatus(404);
      } else {
        res.sendStatus(500);
      }
      return;
    }

    debug("Stats fichier : %O", stats)
    res.setHeader('Content-Length', stats.size)
    res.setHeader('Last-Modified', stats.mtime)

    res.writeHead(200)
    var readStream = fs.createReadStream(filePath);
    readStream.pipe(res);
  });
}

module.exports = {InitialiserGrosFichiers};
