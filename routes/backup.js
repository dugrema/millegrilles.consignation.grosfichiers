const express = require('express');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const bodyParser = require('body-parser');

const {traitementFichier, pathConsignation} = require('../util/traitementFichier');

const router = express.Router();
const jsonParser = bodyParser.json();

// Creer path stockage temporaire pour upload fichiers backup
const pathBackup = '/tmp/backup_uploads/';
fs.mkdirSync(pathBackup, {recursive: true, mode: 0o700});
const backupUpload = multer({ dest: pathBackup });

// Router pour fichiers locaux (meme MilleGrille)
const backupRouter = express.Router();

const backupFileFields = [
  {name: 'transactions', maxcount: 4},
  {name: 'catalogue', maxcount: 1},
]

router.put('/domaine/*', backupUpload.fields(backupFileFields), traiterUploadHoraire);

// Path de download des fichiers de backup horaires
router.get('/liste/backups_horaire', getListeBackupHoraire);
router.get('/:aggregation(horaire)/:type(transactions)/:pathFichier(*)', getFichierBackup);
router.get('/:aggregation(horaire)/:type(catalogues)/:pathFichier(*)', getFichierBackup);

async function traiterUploadHoraire(req, res, next) {
  // console.debug("fichier backup PUT " + req.url);
  // console.debug("Headers: ");
  // console.debug(req.headers);
  // console.debug("Body: ");
  // console.debug(req.body);
  // console.debug(req.files);

  // Streamer fichier vers FS
  await traitementFichier.traiterPutBackup(req)
  .then(msg=>{
      // console.debug("Retour top, grosfichier traite");
      // console.debug(msg);
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
    try {
      req.files.forEach(file=>{
        // console.debug("Supprimer fichier " + file.path);
        fs.unlink(file.path, err=>{
          if(err) {
            console.warn("Erreur suppression fichier backup " + file.path);
            console.warn(err);
            return({err});
          }
        });
      });
    } catch(err) {
      console.warn("Unlink fichiers non complete pour requete " + req.url);
    }

  });


};

async function getListeBackupHoraire(req, res, next) {

  // console.debug("Retourner la liste des backups horaires");

  try {
    const listeBackupsHoraires = await traitementFichier.genererListeBackupsHoraire(req);
    res.end(JSON.stringify(listeBackupsHoraires));
  } catch(err) {
    console.error("Erreur preparation liste backups horaire");
    console.error(err);
    res.sendStatus(500);
  }

}

async function getFichierBackup(req, res, next) {
  try {
    // console.debug(req.params);
    const pathFichier = req.params.pathFichier;
    const typeFichier = req.params.type;
    const aggregation = req.params.aggregation;

    // Valider que c'est bien un fichier de backup de transactions
    if( pathFichier.split('/')[4] != typeFichier) {
      console.error("Le fichier n'est pas sous le repertoire de " + typeFichier + ": " + pathFichier);
      return res.sendStatus(403);
    }

    // console.debug("Path fichier transactions backup: " + pathFichier);

    // S'assurer que le fichier existe, recuperer le full path
    const statFichier = await traitementFichier.getStatFichierBackup(pathFichier, aggregation);

    let contentType = 'application/x-xz';
    if(statFichier.size) {
      const header = {
        'Content-Type': contentType,
        'Content-Length': statFichier.size,
        'Content-Disposition': 'attachment;',
      };

      res.writeHead(200, header);
      var readStream = fs.createReadStream(statFichier.fullPathFichier);
      readStream.pipe(res);

    } else {
      // Fichier non trouve
      res.sendStatus(404);
    }

  } catch(err) {
    console.error("Erreur download transactions");
    console.error(err);
    res.sendStatus(500);
  }

};

module.exports = router;
