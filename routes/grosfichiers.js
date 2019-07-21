const express = require('express');
const path = require('path');
const fs = require('fs');
const {traitementFichier, pathConsignation} = require('../util/traitementFichier');

const router = express.Router();

// Router pour fichiers locaux (meme MilleGrille)
const localRouter = express.Router();
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
  console.log("ProcessFichiersLocaux methode:" + req.method + ": " + req.url);
  // console.log(req.headers);

  // Le serveur supporte une requete GET ou POST pour aller chercher les fichiers
  // GET devrait surtout etre utilise pour le developpement
  let fuuid = header.fuuid, encrypted = false;

  var filePath = pathConsignation.trouverPathLocal(fuuid, encrypted);  //path.join('/home/mathieu/work/downloadStaging/local', fuuid);
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

  // Streamer fichier vers FS
  try {
    traitementFichier.traiterPut(req)
      .then(msg=>{
        // console.log("Retour top, grosfichier traite");
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

});

router.put('/local/nouvelleVersion/*', function(req, res, next) {
  console.debug("nouvelleVersion PUT " + req.url);
  console.debug(req.headers);
  console.log(req.files);
  res.sendStatus(200);
});

// Router pour fichiers tiers (autres MilleGrilles)
// var tiersRouter = express.Router();
// router.use('/tiers', tiersRouter);

module.exports = router;
