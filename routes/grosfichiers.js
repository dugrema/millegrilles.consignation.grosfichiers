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

  router.get('*', downloadFichierLocal)

  router.post('*', bodyParserInstance, downloadFichierLocalChiffre)

  const multerProcessor = multer({dest: '/var/opt/millegrilles/consignation/multer'}).single('fichier')

  router.put('*', multerProcessor, async (req, res, next) => {
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

function downloadFichierLocal(header, req, res) {
  debug("ProcessFichiersLocaux methode:" + req.method + ": " + req.url);
  debug(req.headers);
  debug(req.autorisationMillegrille)

  res.sendStatus(503)
}

function downloadFichierLocalChiffre(req, res) {
  debug("ProcessFichiersLocaux methode:" + req.method + ": " + req.url);
  debug(req.headers);
  debug(req.autorisationMillegrille)

  let fuuid = req.body.fuuid

  var contentType = req.headers.mimetype || 'application/octet-stream'
  header = {
    fuuid: fuuid,
    'Content-Type': contentType,
    securite: '3.protege',
  }

  const idmg = req.autorisationMillegrille.idmg;
  const pathConsignation = new PathConsignation({idmg})
  console.info("Path consignation idmg:%s = %s", idmg, pathConsignation);

  // Le serveur supporte une requete GET ou POST pour aller chercher les fichiers
  // GET devrait surtout etre utilise pour le developpement
  let encrypted = true

  // if(!req.autorisationMillegrille.protege) {
  //   throw Exception("SSL Client Cert: Non autorise a acceder fichier protege/secure");
  // }

  var filePath = pathConsignation.trouverPathLocal(fuuid, encrypted);
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

    header['Content-Length'] = stats.size,
    // Forcer download plutot que open dans le browser

    res.writeHead(200, header);
    var readStream = fs.createReadStream(filePath);
    readStream.pipe(res);
  });
}

module.exports = {InitialiserGrosFichiers};
