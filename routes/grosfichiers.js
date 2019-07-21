var express = require('express');
var path = require('path');
var fs = require('fs');
var router = express.Router();


// Router pour fichiers locaux (meme MilleGrille)
var localRouter = express.Router();
router.use('/local', localRouter);
localRouter.get('*', processFichiersLocaux);
localRouter.post('*', processFichiersLocaux);

function processFichiersLocaux(req, res) {
  console.log("ProcessFichiersLocaux methode:" + req.method + ": " + req.url);
  console.log(req.headers);

  // Le serveur supporte une requete GET ou POST pour aller chercher les fichiers
  // GET devrait surtout etre utilise pour le developpement
  let fuuide = req.headers.fuuide || req.body.fuuide || req.url;
  var fileName = req.headers.nomfichier || req.body.nomfichier;
  var contentType = req.headers.contentype || req.body.contenttype || 'application/octet-stream';

  var filePath = path.join('/home/mathieu/work/downloadStaging/local', fuuide);
  var stat = fs.statSync(filePath);

  let header = {
    'Content-Type': contentType,
    'Content-Length': stat.size,
  }
  if(fileName) {
    header['Content-Disposition'] = 'filename="' + fileName + '"';
  }

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
