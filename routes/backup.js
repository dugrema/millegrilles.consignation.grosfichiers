const debug = require('debug')('millegrilles:routes:backup')
const express = require('express')
const path = require('path')
const fs = require('fs')
const multer = require('multer')
const bodyParser = require('body-parser')

const {PathConsignation} = require('../util/traitementFichier')
const {TraitementFichierBackup} = require('../util/traitementBackup')
const {RestaurateurBackup} = require('../util/restaurationBackup')

function InitialiserBackup(fctRabbitMQParIdmg) {

  const router = express.Router()
  const jsonParser = bodyParser.json()

  // Creer path stockage temporaire pour upload fichiers backup
  const pathBackup = '/var/opt/millegrilles/consignation/backup_uploads/'
  fs.mkdirSync(pathBackup, {recursive: true, mode: 0o700})
  const backupUpload = multer({ dest: pathBackup })

  // Router pour fichiers locaux (meme MilleGrille)
  const backupRouter = express.Router()

  const backupFileFields = [
    {name: 'transactions', maxcount: 4},
    {name: 'catalogue', maxcount: 1},
  ]

  router.put('/fichiers/backup/domaine/*',
    // (req, res, next)=>{debug("Avant upload"); next()},
    backupUpload.fields(backupFileFields),
    // (req, res, next)=>{debug("Apres upload"); next()},
    traiterUploadHoraire
  )

  // Path de download des fichiers de backup horaires
  router.get('/fichiers/backup/restaurerDomaine/:domaine', restaurerDomaine)
  router.get('/fichiers/backup/:aggregation(horaire)/:type(transactions)/:pathFichier(*)', getFichierBackup)
  router.get('/fichiers/backup/:aggregation(horaire)/:type(catalogues)/:pathFichier(*)', getFichierBackup)

  router.get('/fichiers/backup/backup.tar', getFichierTar)

  return router
}

async function traiterUploadHoraire(req, res, next) {
  debug("Fichier backup PUT : %s", req.url);
  // debug("Headers:\n%O", req.headers);

  const idmg = req.autorisationMillegrille.idmg
  const rabbitMQ = req.rabbitMQ
  const traitementFichier = new TraitementFichierBackup(rabbitMQ);

  // Streamer fichier vers FS
  await traitementFichier.traiterPutBackup(req)
  .then(msg=>{
      response = {
       ...msg,
      };
      res.end(JSON.stringify(response));
  })
  .catch(err=>{
    console.error("Erreur traitement fichier " + req.url)
    console.error(err)
    res.sendStatus(500)

    // Tenter de supprimer les fichiers
    try {
      debug("Supprimer fichiers : %O", req.files)
      const fichiers = [...req.files.transactions, ...req.files.catalogue]
      fichiers.forEach(file=>{
        // console.debug("Supprimer fichier " + file.path);
        fs.unlink(file.path, err=>{
          if(err) {
            console.warn("Erreur suppression fichier backup " + file.path)
            return({err})
          }
        })
      })
    } catch(err) {
      console.warn("Unlink fichiers non complete pour requete %s :\n%O", req.url, err)
    }

  })

}

// Recupere tous les fichiers dans une archive tar
async function getFichierTar(req, res, next) {
  const idmg = req.autorisationMillegrille.idmg
  const rabbitMQ = req.rabbitMQ
  const traitementFichier = new TraitementFichierBackup(rabbitMQ)

  traitementFichier.getFichierTarBackupComplet(req, res)
}

async function restaurerDomaine(req, res, next) {

  // console.debug("Retourner la liste des backups horaires");
  try {
    const idmg = req.autorisationMillegrille.idmg
    const rabbitMQ = req.rabbitMQ
    const traitementFichier = new RestaurateurBackup(rabbitMQ)
    await traitementFichier.restaurerDomaine(req, res, next)
  } catch(err) {
    console.error(`Erreur restauration domaine ${req.query.nomDomaine}\n%O`, err)
    res.sendStatus(500)
  }

}

async function getFichierBackup(req, res, next) {
  try {
    // console.debug(req.params);
    const pathFichier = req.params.pathFichier
    const typeFichier = req.params.type
    const aggregation = req.params.aggregation

    // Valider que c'est bien un fichier de backup de transactions
    if( pathFichier.split('/')[4] != typeFichier) {
      console.error("Le fichier n'est pas sous le repertoire de " + typeFichier + ": " + pathFichier)
      return res.sendStatus(403)
    }

    // console.debug("Path fichier transactions backup: " + pathFichier);

    // S'assurer que le fichier existe, recuperer le full path
    const statFichier = await traitementFichier.getStatFichierBackup(pathFichier, aggregation)

    let contentType = 'application/x-xz'
    if(statFichier.size) {
      const header = {
        'Content-Type': contentType,
        'Content-Length': statFichier.size,
        'Content-Disposition': 'attachment;',
      }

      res.writeHead(200, header)
      var readStream = fs.createReadStream(statFichier.fullPathFichier)
      readStream.pipe(res)

    } else {
      // Fichier non trouve
      res.sendStatus(404)
    }

  } catch(err) {
    console.error("Erreur download transactions")
    console.error(err)
    res.sendStatus(500)
  }

}

module.exports = {InitialiserBackup}
