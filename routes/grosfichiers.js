const debug = require('debug')('fichiers:routeGrosfichiers')
const express = require('express');
const fs = require('fs');
const path = require('path')
const readdirp = require('readdirp')

const uploadFichier = require('./uploadFichier')
// const { stagingFichier: stagingPublic, creerStreamDechiffrage } = require('../util/publicStaging')

// const throttle = require('@sitespeed.io/throttle');

const STAGING_FILE_TIMEOUT_MSEC = 300000,
      L2PRIVE = '2.prive'

let _storeConsignation = null

function InitialiserGrosFichiers(mq, storeConsignation, opts) {
  opts = opts || {}
  _storeConsignation = storeConsignation

  const router = express.Router();
  const routerFichiersTransfert = express.Router()
  router.use('/fichiers_transfert', routerFichiersTransfert)

  // backup : /fichiers_transfert/backup
  const routerBackup = express.Router()
  routerFichiersTransfert.use('/backup', routerBackup)
  routerBackup.get('/liste', getListeBackup)
  routerBackup.get('/transactions/:domaine/:fichier', getFichierTransaction)

  //router.get('/fichiers/:fuuid', downloadFichierLocal, pipeReponse)
  //router.head('/fichiers/:fuuid', downloadFichierLocal)

  // Path fichiers_transfert. Comportement identique a /fichiers, utilise
  // pour faire une authentification systeme avec cert SSL (en amont,
  // deja valide rendu ici)
  //router.get('/fichiers_transfert/backup/liste', getListeFichiers)
  staticRouteData(router, storeConsignation)
  routerFichiersTransfert.get('/:fuuid', headersFichier, pipeReponse)
  routerFichiersTransfert.head('/:fuuid', headersFichier, returnOk)

  routerFichiersTransfert.use(uploadFichier.init(mq, storeConsignation, opts))

  return router
}

function returnOk(req, res) {
  res.sendStatus(200)
}

function staticRouteData(route, storeConsignation) {
  // Route utilisee pour transmettre fichiers react de la messagerie en production
  const folderStatic = storeConsignation.getPathDataFolder()
  const router2 = new express.Router()
  route.use('/fichiers_transfert/data', router2)
  router2.use((req, res, next)=>{
    debug("staticRouteData ", req.url)
    next()
  })
  // router2.use(cacheRes, express.static(folderStatic))
  router2.use(express.static(folderStatic))
  debug("staticRouteData Route %s pour data fichiers initialisee", folderStatic)
}

function cacheRes(req, res, next) {
  res.append('Cache-Control', 'max-age=300')
  res.append('Cache-Control', 'public')
  next()
}

async function getListeBackup(req, res) {
  debug("getListeBackup start")
  const traiterFichier = fichier => {
    if(!fichier) return  // Derniere entree

    const pathFichierSplit = fichier.directory.split('/')
    const pathBase = pathFichierSplit.slice(pathFichierSplit.length-2).join('/')

    // Conserver uniquement le contenu de transaction/ (transaction_archive/ n'est pas copie)
    //if(pathBase.startsWith('transaction/')) {
      const fichierPath = path.join(pathBase, fichier.filename)
      debug("getListeBackup fichier %s", fichierPath)
      res.write(fichierPath + '\n')
    //}
  }

  res.status(200)
  // res.set('Content-Type', 'text/plain')
  await _storeConsignation.parcourirBackup(traiterFichier)
  res.end()
  console.debug("getListeBackup done")
}

async function getFichierTransaction(req, res) {
  debug("getFichierTransaction url %s params %O", req.url, req.params)
  try {
    const { domaine, fichier } = req.params
    const pathFichier = path.join(domaine, fichier)
    const stream = await _storeConsignation.getBackupTransactionStream(pathFichier)
    // debug("Stream fichier transactions ", stream)
    res.status(200)
    // res.set('Content-Length', ...)
    stream.pipe(res)
  } catch(err) {
    console.error(new Date() + " getFichierTransaction Erreur ", err)
    res.sendStatus(500)
  }
}

// async function getListeFichiers(req, res, next) {

//   res.status(200)
//   res.setHeader('Cache-Control', 'no-cache')

//   let skipCount = 0, count = 0
//   for await (const entry of readdirp('/var/opt/millegrilles/consignation/local', {type: 'files', alwaysStat: true})) {
//     const { basename } = entry
//     if(basename.split('.').length > 1) {
//       skipCount++
//       continue
//     }
//     count++
//     const fichierParse = path.parse(basename)
//     const hachage_bytes = fichierParse.name
//     res.write(hachage_bytes)
//     res.write('\n')
//   }

//   debug("getListeFichiers Fichiers count %d, skip %d", count, skipCount)

//   res.end()
// }

async function headersFichier(req, res, next) {
  const fuuid = req.params.fuuid
  res.fuuid = fuuid
  debug("HEAD fichier %s", fuuid)

  try {
    const infoFichier = await _storeConsignation.getInfoFichier(fuuid, {recover: true})
    debug("Info fichier %s: %O", fuuid, infoFichier)
    if(!infoFichier) {

      if(_storeConsignation.estPrimaire() !== true) {
        debug("Tenter transferer fichier %s a partir primaire", fuuid)
        _storeConsignation.ajouterDownloadPrimaire(fuuid)
      }

      return res.sendStatus(404)
    } else if(infoFichier.fileRedirect) {
      res.setHeader('Cache-Control', 'public, max-age=300, immutable')
      return res.redirect(307, infoFichier.fileRedirect)
    }
    res.stat = infoFichier.stat
    res.filePath = infoFichier.filePath
  } catch(err) {
    console.error("ERROR %O Erreur head fichier %s : %O", new Date(), fuuid, err)
    return res.sendStatus(500)
  }
  
  // Transfert du fichier chiffre directement, on met les stats du filesystem
  var contentType = req.headers.mimetype || 'application/octet-stream'
  res.setHeader('Content-Length', res.stat.size)
  res.setHeader('Content-Type', contentType)

  // Cache control public, permet de faire un cache via proxy (nginx)
  res.setHeader('Cache-Control', 'public, max-age=604800, immutable')
  res.setHeader('fuuid', fuuid)
  // res.setHeader('securite', niveauAcces)
  res.setHeader('Last-Modified', res.stat.mtime)

  next()
}

// async function downloadFichierLocal(req, res, next) {
//   debug("downloadFichierLocal methode:" + req.method + ": " + req.url);
//   debug("headers : %O", req.headers);

//   const encrypted = true
//   const fuuid = req.params.fuuid
//   res.fuuid = fuuid
//   debug("Fuuid : %s", fuuid)

//   // Verifier si le fichier existe
//   const idmg = req.autorisationMillegrille.idmg;
//   const pathConsignation = new PathConsignation({idmg})
//   res.filePath = pathConsignation.trouverPathLocal(res.fuuid, encrypted);

//   try {
//     const stat = await new Promise((resolve, reject)=>{
//       fs.stat(res.filePath, (err, stat)=>{
//         if(err) {
//           if(err.errno == -2) return reject(404)
//           console.error(err);
//           return reject(500)
//         }
//         resolve(stat)
//       })
//     })
//     res.stat = stat
//   } catch(statusCode) {
//     // console.error("Erreur acces fichier %s", statusCode)
//     return res.sendStatus(statusCode)
//   }

//   // Verifier si l'acces est en mode chiffre (protege) ou dechiffre (public, prive)
//   const niveauAcces = req.autorisationMillegrille.securite

//   // Transfert du fichier chiffre directement, on met les stats du filesystem
//   var contentType = req.headers.mimetype || 'application/octet-stream'
//   res.setHeader('Content-Length', res.stat.size)
//   res.setHeader('Content-Type', contentType)

//   debug("Info idmg: %s, paths: %s", idmg, pathConsignation);

//   // Cache control public, permet de faire un cache via proxy (nginx)
//   res.setHeader('Cache-Control', 'public, max-age=604800, immutable')
//   res.setHeader('fuuid', res.fuuid)
//   res.setHeader('securite', niveauAcces)
//   res.setHeader('Last-Modified', res.stat.mtime)

//   if(req.method === "GET") {
//     next()
//   } else if(req.method === "HEAD") {
//     return res.sendStatus(200)
//   }
  
// }

// async function downloadFichierPublic(req, res, next) {
//   debug("downloadFichierLocalChiffre methode:" + req.method + ": " + req.url);
//   debug("Headers : %O\nAutorisation: %o", req.headers, req.autorisationMillegrille);

//   const securite = req.headers.securite || L2PRIVE
//   var encrypted = true

//   var utiliserPreview = req.query.preview?true:false
//   var videoResolution = req.query.resolution
//   var nofile = req.query.nofile?true:false

//   const fuuid = req.params.fuuid
//   res.fuuid = fuuid

//   console.debug("Fuuid : %s", fuuid)

//   // Verifier si le fichier existe
//   const idmg = req.autorisationMillegrille.idmg;
//   const pathConsignation = new PathConsignation({idmg})
//   res.filePath = pathConsignation.trouverPathLocal(res.fuuid, encrypted);

//   try {
//     const stat = await new Promise((resolve, reject)=>{
//       fs.stat(res.filePath, (err, stat)=>{
//         if(err) {
//           if(err.errno == -2) return reject(404)
//           console.error(err);
//           return reject(500)
//         }
//         resolve(stat)
//       })
//     })
//     res.stat = stat
//   } catch(statusCode) {
//     // console.error("Erreur acces fichier %s", statusCode)
//     return res.sendStatus(statusCode)
//   }

//   // Verifier si l'acces est en mode chiffre (protege) ou dechiffre (public, prive)
//   const niveauAcces = '1.public'  // req.autorisationMillegrille.securite

//   // if(encrypted && ['1.public', '2.prive'].includes(niveauAcces)) {

//   // Le fichier est chiffre mais le niveau d'acces de l'usager ne supporte
//   debug("Verifier si permission d'acces en mode %s pour %s", niveauAcces, req.url)
//   var amqpdao = req.amqpdao

//   // pas le mode chiffre. Demander une permission de dechiffrage au domaine
//   // et dechiffrer le fichier au vol si permis.
//   try {
//     const infoStream = await creerStreamDechiffrage(amqpdao, req.params.fuuid)
//     if(infoStream.acces === '0.refuse') {
//       debug("Permission d'acces refuse en mode %s pour %s", niveauAcces, req.url)
//       return res.sendStatus(403)  // Acces refuse
//     }

//     // Ajouter information de dechiffrage pour la reponse
//     res.decipherStream = infoStream.decipherStream
//     res.permission = infoStream.permission
//     res.fuuid = infoStream.fuuidEffectif

//     const fuuidEffectif = infoStream.fuuidEffectif

//     // Preparer le fichier dechiffre dans repertoire de staging
//     const infoFichierEffectif = await stagingPublic(pathConsignation, fuuidEffectif, infoStream)
//     res.stat = infoFichierEffectif.stat
//     res.filePath = infoFichierEffectif.filePath

//     // Ajouter information de header pour slicing (HTTP 206)
//     res.setHeader('Content-Length', res.stat.size)
//     res.setHeader('Accept-Ranges', 'bytes')

//     // if(fuuidEffectif !== fuuid) {
//     //   // Preview
//     //   if(infoStream.infoVideo) {
//     //     res.setHeader('Content-Type', infoStream.infoVideo.mimetype)
//     //     // res.setHeader('Content-Length', infoStream.infoVideo.taille)
//     //   } else {
//     //     res.setHeader('Content-Type', res.permission['mimetype_preview'])
//     //   }
//     //
//     //   // S'assurer que le fichier de preview existe avant de changer le filePath
//     //   var previewPath = pathConsignation.trouverPathLocal(fuuidEffectif, encrypted)
//     //   try {
//     //     // Changer information de fichier - on transmet preview
//     //     res.fuuid = fuuidEffectif
//     //   } catch(err) {
//     //     console.error("Preview non disponible : %O", err)
//     //   }
//     //
//     //   // Override du filepath par celui du preview
//     // } else {
//       // Ajouter nom fichier
//       const nomFichier = res.permission['nom_fichier']
//       if(!nofile) {
//         res.setHeader('Content-Disposition', 'attachment; filename="' + nomFichier +'"')
//       }
//       // res.setHeader('Content-Length', res.tailleFichier)
//       var mimetype = res.permission['mimetype'] || 'application/stream'
//       res.setHeader('Content-Type', mimetype)
//     // }

//   } catch(err) {
//     console.error("Erreur traitement dechiffrage stream pour %s:\n%O", req.url, err)
//     debug("Permission d'acces refuse en mode %s pour %s", niveauAcces, req.url)
//     return res.sendStatus(403)  // Acces refuse
//   }

//   debug("Info idmg: %s, stat fichier: %s", idmg, pathConsignation);

//   // Cache control public, permet de faire un cache via proxy (nginx)
//   res.setHeader('Cache-Control', 'public, max-age=604800, immutable')
//   res.setHeader('fuuid', res.fuuid)
//   res.setHeader('securite', '1.public')
//   res.setHeader('Last-Modified', res.stat.mtime)

//   const range = req.headers.range
//   if(range) {
//     console.debug("Range request : %s, taille fichier %s", range, res.stat.size)
//     const infoRange = readRangeHeader(range, res.stat.size)
//     res.range = infoRange
//   }

//   next()
// }

// async function downloadVideoPrive(req, res, next) {
//   debug("downloadVideoPrive methode:" + req.method + ": " + req.url);
//   debug("Headers : %O\nAutorisation: %o", req.headers, req.autorisationMillegrille);

//   const fuuid = req.params.fuuid
//   res.fuuid = fuuid

//   debug("downloadVideoPrive Fuuid : %s", fuuid)

//   // Verifier si le fichier existe
//   const idmg = req.autorisationMillegrille.idmg;
//   const pathConsignation = new PathConsignation({idmg})
//   res.filePath = pathConsignation.trouverPathLocal(res.fuuid, true);

//   try {
//     const stat = await new Promise((resolve, reject)=>{
//       fs.stat(res.filePath, (err, stat)=>{
//         if(err) {
//           if(err.errno == -2) return reject(404)
//           console.error(err);
//           return reject(500)
//         }
//         resolve(stat)
//       })
//     })
//     res.stat = stat
//   } catch(statusCode) {
//     // console.error("Erreur acces fichier %s", statusCode)
//     return res.sendStatus(statusCode)
//   }

//   // Verifier si l'acces est en mode chiffre (protege) ou dechiffre (public, prive)

//   // if(encrypted && ['1.public', '2.prive'].includes(niveauAcces)) {

//   // Le fichier est chiffre mais le niveau d'acces de l'usager ne supporte
//   debug("Verifier si permission d'acces en mode prive pour video %s", req.url)
//   var amqpdao = req.amqpdao

//   // pas le mode chiffre. Demander une permission de dechiffrage au domaine
//   // et dechiffrer le fichier au vol si permis.
//   try {
//     const infoStream = await creerStreamDechiffrage(amqpdao, req.params.fuuid, {prive: true})

//     debug("Information stream : %O", infoStream)

//     const permission = infoStream.permission || {}
//     if(infoStream.acces === '0.refuse' || !permission.mimetype.startsWith('video/')) {
//       debug("Permission d'acces refuse pour video en mode prive pour %s", req.url)
//       return res.sendStatus(403)  // Acces refuse
//     }

//     // Ajouter information de dechiffrage pour la reponse
//     res.decipherStream = infoStream.decipherStream
//     res.permission = infoStream.permission
//     res.fuuid = infoStream.fuuidEffectif

//     const fuuidEffectif = infoStream.fuuidEffectif

//     // Preparer le fichier dechiffre dans repertoire de staging
//     const infoFichierEffectif = await stagingPublic(pathConsignation, fuuidEffectif, infoStream)
//     res.stat = infoFichierEffectif.stat
//     res.filePath = infoFichierEffectif.filePath

//     // Ajouter information de header pour slicing (HTTP 206)
//     res.setHeader('Content-Length', res.stat.size)
//     res.setHeader('Accept-Ranges', 'bytes')

//     // res.setHeader('Content-Length', res.tailleFichier)
//     var mimetype = res.permission.mimetype
//     res.setHeader('Content-Type', mimetype)

//   } catch(err) {
//     console.error("Erreur traitement dechiffrage stream pour %s:\n%O", req.url, err)
//     debug("Permission d'acces refuse en mode %s pour %s", niveauAcces, req.url)
//     return res.sendStatus(403)  // Acces refuse
//   }

//   debug("Info idmg: %s, stat fichier: %s", idmg, pathConsignation);

//   // Cache control public, permet de faire un cache via proxy (nginx)
//   res.setHeader('Cache-Control', 'public, max-age=604800, immutable')
//   res.setHeader('fuuid', res.fuuid)
//   res.setHeader('securite', '2.prive')
//   res.setHeader('Last-Modified', res.stat.mtime)

//   const range = req.headers.range
//   if(range) {
//     console.debug("Range request : %s, taille fichier %s", range, res.stat.size)
//     const infoRange = readRangeHeader(range, res.stat.size)
//     res.range = infoRange
//   }

//   next()
// }

// Sert a preparer un fichier temporaire local pour determiner la taille, supporter slicing
function pipeReponse(req, res) {
  // const header = res.responseHeader
  const { range, filePath, fileRedirect, stat } = res

  if(range) {
    // Implicitement un fichier 1.public, staging local
    var start = range.Start,
        end = range.End

    // If the range can't be fulfilled.
    if (start >= stat.size) { // || end >= stat.size) {
      // Indicate the acceptable range.
      res.setHeader('Content-Range', 'bytes */' + stat.size)  // File size.

      // Return the 416 'Requested Range Not Satisfiable'.
      res.writeHead(416)
      return res.end()
    }

    res.setHeader('Content-Range', 'bytes ' + start + '-' + end + '/' + stat.size)

    debug("Transmission range fichier %d a %d bytes (taille :%d) : %s", start, end, stat.size, filePath)
    const readStream = fs.createReadStream(filePath, { start: start, end: end })
    res.status(206)
    readStream.pipe(res)
  } else if(fileRedirect) {
    // Redirection
    res.status(307).send(fileRedirect)
  } else if(filePath) {
    // Transmission directe du fichier
    const readStream = fs.createReadStream(filePath)
    res.writeHead(200)
    readStream.pipe(res)
  }

}

// function readRangeHeader(range, totalLength) {
//     /* src : https://www.codeproject.com/articles/813480/http-partial-content-in-node-js
//      * Example of the method 'split' with regular expression.
//      *
//      * Input: bytes=100-200
//      * Output: [null, 100, 200, null]
//      *
//      * Input: bytes=-200
//      * Output: [null, null, 200, null]
//      */

//     if (range == null || range.length == 0)
//         return null;

//     var array = range.split(/bytes=([0-9]*)-([0-9]*)/);
//     var start = parseInt(array[1]);
//     var end = parseInt(array[2]);
//     var result = {
//         Start: isNaN(start) ? 0 : start,
//         End: isNaN(end) ? (totalLength - 1) : end
//     }
// }

module.exports = {InitialiserGrosFichiers};
