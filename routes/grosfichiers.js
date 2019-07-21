var express = require('express');
var path = require('path');
var fs = require('fs');
var router = express.Router();


// Router pour fichiers locaux (meme MilleGrille)
var localRouter = express.Router();
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

router.put('/nouveauFichier', function(req, res, next) {

});

router.put('/nouvelleVersion', function(req, res, next) {

});

// Router pour fichiers tiers (autres MilleGrilles)
// var tiersRouter = express.Router();
// router.use('/tiers', tiersRouter);

module.exports = router;
