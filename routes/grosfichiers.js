const express = require('express');
const path = require('path');
const fs = require('fs');
const {traitementFichier} = require('../util/traitementFichier');

const router = express.Router();

// Router pour fichiers locaux (meme MilleGrille)
const localRouter = express.Router();
router.use('/local', localRouter);

localRouter.get('*', (req, res, next) => {
  // Tenter de charger les parametres via MQ
  let fuuide = req.url;
  let contentType = 'application/octet-stream';
  header = {
    fuuide: req.url,
    contentType: contentType,
  }
  processFichiersLocaux(header, req, res);
});
localRouter.post('*', (req, res, next) => {
  let fuuide = req.headers.fuuide;
  var fileName = req.headers.nomfichier;
  var contentType = req.headers.contenttype || 'application/octet-stream';
  header = {
    fuuide: fuuide,
    'Content-Type': contentType,
    'Content-Disposition': 'filename="' + fileName + '"',
  }
  processFichiersLocaux(header, req, res);
});

function processFichiersLocaux(header, req, res) {
  console.log("ProcessFichiersLocaux methode:" + req.method + ": " + req.url);
  console.log(req.headers);

  // Le serveur supporte une requete GET ou POST pour aller chercher les fichiers
  // GET devrait surtout etre utilise pour le developpement
  let fuuide = header.fuuide;

  var filePath = path.join('/home/mathieu/work/downloadStaging/local', fuuide);
  var stat = fs.statSync(filePath);
  header['Content-Length'] = stat.size,

  res.writeHead(200, header);
  var readStream = fs.createReadStream(filePath);

  readStream.pipe(res);
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
