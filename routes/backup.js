const debug = require('debug')('millegrilles:routes:backup')
const express = require('express')
const path = require('path')
const fs = require('fs')
const multer = require('multer')
const bodyParser = require('body-parser')

const {PathConsignation, streamListeFichiers} = require('../util/traitementFichier')
const {
  TraitementFichierBackup, getListeDomaines, getCataloguesDomaine,
  getListeFichiers, getFichier
} = require('../util/traitementBackup')
const {RestaurateurBackup} = require('../util/restaurationBackup')

function backupMiddleware(req, res, next) {
  const rabbitMQ = req.rabbitMQ
  if(!rabbitMQ) {
    console.error('backupMiddleware: RabbitMQ non initialise')
    return res.sendStatus(500)
  }

  const traitementBackup = new TraitementFichierBackup(rabbitMQ)
  req.traitementBackup = traitementBackup

  const pathConsignation = new PathConsignation({idmg: rabbitMQ.pki.idmg})
  req.pathConsignation = pathConsignation

  next()
}

function InitialiserBackup(fctRabbitMQParIdmg) {

  const router = express.Router()
  const jsonParser = bodyParser.json()

  // Creer path stockage temporaire pour upload fichiers backup
  const pathBackup = '/var/opt/millegrilles/consignation/backup_uploads/'
  fs.mkdirSync(pathBackup, {recursive: true, mode: 0o700})
  const backupUpload = multer({ dest: pathBackup })

  // Router pour fichiers locaux (meme MilleGrille)
  const backupRouter = express.Router()

  router.use(backupMiddleware)

  // GET pour contenu de backup (domaines, fichiers, catalogues)
  router.get('/backup/listeDomaines', getListeDomaines)
  router.get('/backup/catalogues/:domaine', getCataloguesDomaine)
  router.get('/backup/listeFichiers/:domaine', getListeFichiers)
  router.get('/backup/fichier/:domaine/:nomFichier', getFichier)
  router.get('/backup/fichier/:domaine/:sousrep?/:nomFichier', getFichier)

  // Backup (upload)
  const backupFileFields = [
    {name: 'transactions', maxcount: 1},
    {name: 'catalogue', maxcount: 1},
    {name: 'cles', maxcount: 1},
  ]
  router.put('/backup/domaine/:nomCatalogue',
    backupUpload.fields(backupFileFields),
    traiterUploadHoraire
  )

  const applicationFileFields = [
    {name: 'application', maxcount: 1},
    {name: 'catalogue', maxcount: 1},
    {name: 'cles', maxcount: 1},
  ]
  router.put('/backup/application/:nomApplication',
    backupUpload.fields(applicationFileFields),
    traiterUploadApplication
  )

  // Path de download des fichiers de backup
  router.get('/backup/restaurerDomaine/:domaine', restaurerDomaine, streamListeFichiers)
  router.get('/backup/application/:nomApplication', restaurerApplication)

  // Ajouter une methode GET suivante :
  // - retourner la liste des applications presentes dans backup (fichiers sous app)

  return router
}

async function traiterUploadHoraire(req, res, next) {
  debug("Fichier backup PUT : %s", req.url);
  // debug("Headers:\n%O", req.headers);

  const idmg = req.autorisationMillegrille.idmg
  const rabbitMQ = req.rabbitMQ
  const traitementFichierBackup = new TraitementFichierBackup(rabbitMQ);

  // Streamer fichier vers FS
  await traitementFichierBackup.traiterPutBackup(req)
  .then(msg=>{
      response = {
       ...msg,
      }
      if(response.err_serveur) {
        console.error("traiterUploadHoraire ERROR SERVEUR %O", response)
        res.status(504)
      } else if(response.err) {
        console.error("traiterUploadHoraire ERROR GENERIQUE %O", response)
        res.status(400)
      } else {
        res.status(200)
      }
      res.send(response)
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

async function traiterUploadApplication(req, res, next) {
  debug("Fichier application PUT : %s", req.url)

  const idmg = req.autorisationMillegrille.idmg
  const rabbitMQ = req.rabbitMQ
  const traitementFichier = new TraitementFichierBackup(rabbitMQ)

  try {
    await traitementFichier.traiterPutApplication(req)
    res.status(200).end()
  } catch(err) {
    console.error("Erreur traitement fichier %s : %O", req.url, err)
    res.sendStatus(500)
  } finally {
    // Nettoyage des fichiers temporaires sous multer
    try {
      const fichiers = [...req.files.application, ...req.files.cles, ...req.files.catalogue]
      fichiers.forEach(file=>{
        fs.unlink(file.path, err=>{
          if(err) {console.warn("Erreur suppression fichier backup " + file.path)}
        })
      })
    } catch(err) {
      //console.warn("Unlink fichiers non complete pour requete %s :\n%O", req.url, err)
    }
  }

}

async function restaurerDomaine(req, res, next) {

  // console.debug("Retourner la liste des backups horaires");
  try {
    const rabbitMQ = req.rabbitMQ
    const traitementFichier = new RestaurateurBackup(rabbitMQ)
    await traitementFichier.restaurerDomaine(req, res, next)
  } catch(err) {
    console.error(`Erreur restauration domaine ${req.params.domaine}\n%O`, err)
    res.sendStatus(500)
  }

}

async function restaurerApplication(req, res, next) {
  const nomApplication = req.params.nomApplication
  debug("restaurerApplication : %s", nomApplication)

  try {
    const rabbitMQ = req.rabbitMQ
    const traitementFichier = new RestaurateurBackup(rabbitMQ)
    await traitementFichier.restaurerApplication(req, res, next)
  } catch(err) {
    console.error(`Erreur restauration application ${req.params.nomApplication}\n%O`, err)
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
