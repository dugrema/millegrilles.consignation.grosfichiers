const express = require('express');
const path = require('path');
const fs = require('fs');
const {traitementFichier, pathConsignation} = require('../util/traitementFichier');
// const throttle = require('@sitespeed.io/throttle');

const router = express.Router();

// Router pour fichiers locaux (meme MilleGrille)
const backupRouter = express.Router();
router.use('/', backupRouter);

router.put('/*', function(req, res, next) {
  console.debug("fichier backup PUT " + req.url);
  console.debug(req.headers);

  // Streamer fichier vers FS
  // try {
  //   // Returns a promise
  //   traitementFichier.traiterPutBackup(req)
  //   .then(msg=>{
  //       // console.log("Retour top, grosfichier traite");
  //       response = {
  //         sha512Hash: msg.sha512Hash
  //       };
  //       res.send(JSON.stringify(response));
  //   })
  //   .catch(err=>{
  //     console.error("Erreur traitement fichier " + req.url);
  //     console.error(err);
  //     res.sendStatus(500);
  //   });
  // } catch (err) {
  //   console.error(err);
  //   res.sendStatus(500);
  // }

  res.sendStatus(200);

});

module.exports = router;
