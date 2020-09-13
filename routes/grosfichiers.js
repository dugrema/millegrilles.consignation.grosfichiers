const debug = require('debug')('millegrilles:fichiers:grosfichiers')
const express = require('express');
const path = require('path');
const fs = require('fs');
const multer = require('multer')

const {PathConsignation, TraitementFichier} = require('../util/traitementFichier')

// const throttle = require('@sitespeed.io/throttle');

function InitialiserGrosFichiers() {

  const router = express.Router();

  router.get('*', (req, res, next) => {
    // Tenter de charger les parametres via MQ
    let fuuide = req.url;
    let contentType = 'application/octet-stream';
    header = {
      fuuid: req.url,
      contentType: contentType,
    }
    processFichiersLocaux(header, req, res);
  })

  router.post('*', (req, res, next) => {
    let fuuid = req.headers.fuuid;
    // var contentType = req.headers.contenttype || 'application/octet-stream';
    var contentType = 'application/octet-stream';
    header = {
      fuuid: fuuid,
      'Content-Type': contentType,
    }
    processFichiersLocaux(header, req, res);
  })

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
        sha256Hash: msg.sha256Hash
      }

      res.end(JSON.stringify(response))

    } catch (err) {
      console.error(err);
      res.sendStatus(500);
    }

  })

  return router
}

function processFichiersLocaux(header, req, res) {
  // console.log("ProcessFichiersLocaux methode:" + req.method + ": " + req.url);
  // console.log(req.headers);
  // console.debug(req.autorisationMillegrille)
  const idmg = req.autorisationMillegrille.idmg;
  const pathConsignation = new PathConsignation({idmg})
  console.info("Path consignation idmg:%s = %s", idmg, pathConsignation);

  // Le serveur supporte une requete GET ou POST pour aller chercher les fichiers
  // GET devrait surtout etre utilise pour le developpement
  let fuuid = header.fuuid;
  let encrypted = (req.headers.securite === '3.protege' || req.headers.securite === '4.secure');

  if(encrypted && !req.autorisationMillegrille.protege) {
    throw Exception("SSL Client Cert: Non autorise a acceder fichier protege/secure");
  }

  let extension = req.headers.extension;

  var filePath = pathConsignation.trouverPathLocal(fuuid, encrypted, {extension});
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
    header['Content-Disposition'] = 'attachment;';
    // Pour mettre le nom complet: Content-Disposition: attachment; filename="titles.txt"

    res.writeHead(200, header);
    var readStream = fs.createReadStream(filePath);

    readStream.pipe(res);
  });
}

module.exports = {InitialiserGrosFichiers};
