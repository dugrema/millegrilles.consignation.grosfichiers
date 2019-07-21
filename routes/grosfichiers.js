const express = require('express');
const path = require('path');
const fs = require('fs');
const traitementFichier = require('../util/traitementFichier');

const multer = require('multer');
const router = express.Router();

const stagingFolder = process.env.MG_STAGING_FOLDER || "/tmp/uploadStagingCentral";
const consignationFolder = process.env.MG_STAGING_FOLDER || "/tmp/consignation_local";
const multer_fn = multer({dest: stagingFolder}).array('grosfichier');

// Router pour fichiers locaux (meme MilleGrille)
const localRouter = express.Router();
router.use('/local', localRouter);
router.use(multer_fn);

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
  console.debug(req.headers);
  console.log(req.files);

  if(req.files.length === 1) {
    let fichier = req.files[0];
    traitementFichier.traiterPut(req.headers, fichier)
    .then(resultat=>{
      res.sendStatus(200);
    })
    .catch(err=>{
      console.error("Erreur traitement fichier " + req.headers.fuuide);
      console.error(err);
      res.sendStatus(500);
    });

  } else {
    console.error("0 ou plus d'un fichier recu dans le PUT");
    console.error(req.files);
    res.sendStatus(400);
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
