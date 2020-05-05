const express = require('express');
const path = require('path');
const fs = require('fs');

const {PathConsignation, TraitementFichier} = require('../util/traitementFichier')

// const throttle = require('@sitespeed.io/throttle');

function InitialiserGrosFichiers(fctRabbitMQParIdmg) {

  const router = express.Router();

  // Router pour fichiers locaux (meme MilleGrille)
  const localRouter = express.Router();

  // router.use(clientCertificateAuth(checkAuth))
  router.use('/local', localRouter);

  localRouter.get('*', (req, res, next) => {
    // Tenter de charger les parametres via MQ
    let fuuide = req.url;
    let contentType = 'application/octet-stream';
    header = {
      fuuid: req.url,
      contentType: contentType,
    }
    processFichiersLocaux(header, req, res);
  });
  localRouter.post('*', (req, res, next) => {
    let fuuid = req.headers.fuuid;
    // var contentType = req.headers.contenttype || 'application/octet-stream';
    var contentType = 'application/octet-stream';
    header = {
      fuuid: fuuid,
      'Content-Type': contentType,
    }
    processFichiersLocaux(header, req, res);
  });

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
  };

  router.put('/local/nouveauFichier/*', function(req, res, next) {
    console.debug("nouveauFichier PUT " + req.url);
    // console.debug(req.headers);
    const idmg = req.autorisationMillegrille.idmg;
    const rabbitMQ = fctRabbitMQParIdmg(idmg);
    const traitementFichier = new TraitementFichier(rabbitMQ);

    // Streamer fichier vers FS
    try {
      // Returns a promise
      // throttle.start({up: 1000, down: 1000, rtt: 200})
      // .then(() => traitementFichier.traiterPut(req))
      traitementFichier.traiterPut(req)
      .then(msg=>{
          // console.log("Retour top, grosfichier traite");
          // res.sendStatus(200);
          response = {
            sha256Hash: msg.sha256Hash
          };
          res.end(JSON.stringify(response));

      })
      .catch(err=>{
        console.error("Erreur traitement fichier " + req.url);
        console.error(err);
        res.sendStatus(500);
      });
    } catch (err) {
      console.error(err);
      res.sendStatus(500);
    }

  });

  return router;
}

module.exports = {InitialiserGrosFichiers};
