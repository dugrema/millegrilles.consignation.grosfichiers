const express = require('express');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const bodyParser = require('body-parser');

const {traitementFichier, pathConsignation} = require('../util/traitementFichier');
// const throttle = require('@sitespeed.io/throttle');

const router = express.Router();
const jsonParser = bodyParser.json();

// Creer path stockage temporaire pour upload fichiers backup
const pathBackup = '/tmp/backup_uploads/';
fs.mkdirSync(pathBackup, {recursive: true, mode: 0o700});
const backupUpload = multer({ dest: pathBackup });

// Router pour fichiers locaux (meme MilleGrille)
const backupRouter = express.Router();
// router.use('/', backupRouter);

router.put('/domaine/*', backupUpload.array('fichiers_backup'), function(req, res, next) {
  // console.debug("fichier backup PUT " + req.url);
  // console.debug("Headers: ");
  // console.debug(req.headers);
  console.debug("Body: ");
  console.debug(req.body);
  // console.debug(req.files);

  // Streamer fichier vers FS
  traitementFichier.traiterPutBackup(req)
  .then(msg=>{
      console.debug("Retour top, grosfichier traite");
      console.debug(msg);
      response = {
       ...msg,
      };
      res.end(JSON.stringify(response));

      // res.sendStatus(200);

  })
  .catch(err=>{
    console.error("Erreur traitement fichier " + req.url);
    console.error(err);
    res.sendStatus(500);

    // Tenter de supprimer les fichiers
    req.files.forEach(file=>{
      console.debug("Supprimer fichier " + file.path);
      fs.unlink(file.path, err=>{
        if(err) {
          console.warn("Erreur suppression fichier backup " + file.path);
          console.warn(err);
        }
      });
    });

  })

});

module.exports = router;
