const express = require('express');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const bodyParser = require('body-parser');

const {traitementFichier, pathConsignation} = require('../util/traitementFichier');
// const throttle = require('@sitespeed.io/throttle');

const router = express.Router();
const jsonParser = bodyParser.json();
const backupUpload = multer({ dest: 'backup_uploads/' });

// Router pour fichiers locaux (meme MilleGrille)
const backupRouter = express.Router();
// router.use('/', backupRouter);

router.put('/domaine/*', backupUpload.array('fichiers_backup'), function(req, res, next) {
  console.debug("fichier backup PUT " + req.url);
  console.debug("Headers: ");
  console.debug(req.headers);
  console.debug("Body: ");
  console.debug(req.body);
  console.debug(req.files);

  // const listeFuuidGrosfichiers = req.body.fuuid_grosfichiers;
  // // console.debug("GrosFichiers : ");
  // console.debug(listeFuuidGrosfichiers);
  //
  // for(let fichierBackup in req.files) {
  //   console.debug(fichierBackup);
  // }

  // Streamer fichier vers FS
  try {
    // Returns a promise
    traitementFichier.traiterPutBackup(req)
    .then(msg=>{
        // console.log("Retour top, grosfichier traite");
        // response = {
        //   sha512Hash: msg.sha512Hash
        // };
        // res.send(JSON.stringify(response));

        res.sendStatus(200);

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

  // res.sendStatus(200);

});

module.exports = router;
