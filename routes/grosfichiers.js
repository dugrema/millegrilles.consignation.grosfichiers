const debug = require('debug')('millegrilles:fichiers:routeGrosfichiers')
const express = require('express');
const fs = require('fs');
const bodyParser = require('body-parser')

const {PathConsignation} = require('../util/traitementFichier')
const uploadFichier = require('./uploadFichier')
// const { stagingFichier: stagingPublic, creerStreamDechiffrage } = require('../util/publicStaging')

// const throttle = require('@sitespeed.io/throttle');

const STAGING_FILE_TIMEOUT_MSEC = 300000,
      L2PRIVE = '2.prive'

function InitialiserGrosFichiers() {

  const router = express.Router();
  //router.get('/fichiers/:fuuid', downloadFichierLocal, pipeReponse)
  //router.head('/fichiers/:fuuid', downloadFichierLocal)

  // Path fichiers_transfert. Comportement identique a /fichiers, utilise
  // pour faire une authentification systeme avec cert SSL (en amont,
  // deja valide rendu ici)
  router.get('/fichiers_transfert/:fuuid', downloadFichierLocal, pipeReponse)
  router.head('/fichiers_transfert/:fuuid', downloadFichierLocal)

  router.use(uploadFichier.init())

  return router
}

async function downloadFichierLocal(req, res, next) {
  debug("downloadFichierLocal methode:" + req.method + ": " + req.url);
  debug(req.headers);
  debug(req.autorisationMillegrille)

  const encrypted = true
  const fuuid = req.params.fuuid
  res.fuuid = fuuid
  debug("Fuuid : %s", fuuid)

  // Verifier si le fichier existe
  const idmg = req.autorisationMillegrille.idmg;
  const pathConsignation = new PathConsignation({idmg})
  res.filePath = pathConsignation.trouverPathLocal(res.fuuid, encrypted);

  try {
    const stat = await new Promise((resolve, reject)=>{
      fs.stat(res.filePath, (err, stat)=>{
        if(err) {
          if(err.errno == -2) return reject(404)
          console.error(err);
          return reject(500)
        }
        resolve(stat)
      })
    })
    res.stat = stat
  } catch(statusCode) {
    // console.error("Erreur acces fichier %s", statusCode)
    return res.sendStatus(statusCode)
  }

  // Verifier si l'acces est en mode chiffre (protege) ou dechiffre (public, prive)
  const niveauAcces = req.autorisationMillegrille.securite

  // Transfert du fichier chiffre directement, on met les stats du filesystem
  var contentType = req.headers.mimetype || 'application/octet-stream'
  res.setHeader('Content-Length', res.stat.size)
  res.setHeader('Content-Type', contentType)

  debug("Info idmg: %s, paths: %s", idmg, pathConsignation);

  // Cache control public, permet de faire un cache via proxy (nginx)
  res.setHeader('Cache-Control', 'public, max-age=604800, immutable')
  res.setHeader('fuuid', res.fuuid)
  res.setHeader('securite', niveauAcces)
  res.setHeader('Last-Modified', res.stat.mtime)

  if(req.method === "GET") {
    next()
  } else if(req.method === "HEAD") {
    return res.sendStatus(200)
  }
  
}

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
  const header = res.responseHeader
  const filePath = res.filePath

  if(res.range) {
    // Implicitement un fichier 1.public, staging local
    var start = res.range.Start,
        end = res.range.End,
        stat = res.stat

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
  } else {
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
