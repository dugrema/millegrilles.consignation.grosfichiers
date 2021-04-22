const debug = require('debug')('millegrilles:fichiers:grosfichiers')
const express = require('express');
const path = require('path');
const fs = require('fs');
const multer = require('multer')
const bodyParser = require('body-parser')

const {PathConsignation, TraitementFichier} = require('../util/traitementFichier')
const {getDecipherPipe4fuuid} = require('../util/cryptoUtils')
const uploadFichier = require('./uploadFichier')
const { stagingFichier: stagingPublic} = require('../util/publicStaging')

// const throttle = require('@sitespeed.io/throttle');

const STAGING_FILE_TIMEOUT_MSEC = 300000

function InitialiserGrosFichiers() {

  const router = express.Router();

  const bodyParserInstance = bodyParser.urlencoded({ extended: false })

  router.get('/fichiers/public/:fuuid', downloadFichierPublic, pipeReponse)
  router.get('/fichiers/:fuuid', downloadFichierLocal, pipeReponse)
  router.use(uploadFichier.init())

  // router.post('*', bodyParserInstance, downloadFichierLocalChiffre)

  // const multerProcessor = multer({dest: '/var/opt/millegrilles/consignation/multer'}).single('fichier')

  // router.put('^/fichiers/*', multerProcessor, async (req, res, next) => {
  //   console.debug("nouveauFichier PUT %s,\nHeaders: %O\nFichiers: %O\nBody: %O", req.url, req.headers, req.file, req.body)
  //
  //   const idmg = req.autorisationMillegrille.idmg
  //   const rabbitMQ = req.rabbitMQ  //fctRabbitMQParIdmg(idmg)
  //   const traitementFichier = new TraitementFichier(rabbitMQ)
  //
  //   // Streamer fichier vers FS
  //   try {
  //     // Returns a promise
  //     const msg = await traitementFichier.traiterPut(req)
  //
  //     response = {
  //       hachage: msg.hachage
  //     }
  //
  //     res.end(JSON.stringify(response))
  //
  //   } catch (err) {
  //     console.error(err);
  //     res.sendStatus(500);
  //   }
  //
  // })

  // Activer nettoyage sur cedule des repertoires de staging
  // setInterval(cleanupStaging, 120000)

  return router
}

async function downloadFichierLocal(req, res, next) {
  debug("downloadFichierLocalChiffre methode:" + req.method + ": " + req.url);
  debug(req.headers);
  debug(req.autorisationMillegrille)
  // debug("PARAMS\n%O", req.params)
  // debug("QUERY\n%O", req.query)

  const securite = req.headers.securite || '3.protege'
  var encrypted = false
  if(securite === '3.protege') encrypted = true
  var utiliserPreview = req.query.preview?true:false
  var nofile = req.query.nofile?true:false

  const fuuid = req.params.fuuid
  res.fuuid = fuuid

  console.debug("Fuuid : %s", fuuid)

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

  next()
}

async function downloadFichierPublic(req, res, next) {
  debug("downloadFichierLocalChiffre methode:" + req.method + ": " + req.url);
  debug("Headers : %O\nAutorisation: %o", req.headers, req.autorisationMillegrille);

  const securite = req.headers.securite || '3.protege'
  var encrypted = true

  var utiliserPreview = req.query.preview?true:false
  var videoResolution = req.query.resolution
  var nofile = req.query.nofile?true:false

  const fuuid = req.params.fuuid
  res.fuuid = fuuid

  console.debug("Fuuid : %s", fuuid)

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
  const niveauAcces = '1.public'  // req.autorisationMillegrille.securite

  // if(encrypted && ['1.public', '2.prive'].includes(niveauAcces)) {

  // Le fichier est chiffre mais le niveau d'acces de l'usager ne supporte
  debug("Verifier si permission d'acces en mode %s pour %s", niveauAcces, req.url)
  var amqpdao = req.amqpdao

  // pas le mode chiffre. Demander une permission de dechiffrage au domaine
  // et dechiffrer le fichier au vol si permis.
  try {
    const infoStream = await creerStreamDechiffrage(amqpdao, req)
    if(infoStream.acces === '0.refuse') {
      debug("Permission d'acces refuse en mode %s pour %s", niveauAcces, req.url)
      return res.sendStatus(403)  // Acces refuse
    }

    // Ajouter information de dechiffrage pour la reponse
    res.decipherStream = infoStream.decipherStream
    res.permission = infoStream.permission
    res.fuuid = infoStream.fuuidEffectif

    const fuuidEffectif = infoStream.fuuidEffectif

    // Preparer le fichier dechiffre dans repertoire de staging
    const infoFichierEffectif = await stagingPublic(pathConsignation, fuuidEffectif, infoStream)
    res.stat = infoFichierEffectif.stat
    res.filePath = infoFichierEffectif.filePath

    // Ajouter information de header pour slicing (HTTP 206)
    res.setHeader('Content-Length', res.stat.size)
    res.setHeader('Accept-Ranges', 'bytes')

    if(fuuidEffectif !== fuuid) {
      // Preview
      if(infoStream.infoVideo) {
        res.setHeader('Content-Type', infoStream.infoVideo.mimetype)
        // res.setHeader('Content-Length', infoStream.infoVideo.taille)
      } else {
        res.setHeader('Content-Type', res.permission['mimetype_preview'])
      }

      // S'assurer que le fichier de preview existe avant de changer le filePath
      var previewPath = pathConsignation.trouverPathLocal(fuuidEffectif, encrypted)
      try {
        // Changer information de fichier - on transmet preview
        res.fuuid = fuuidEffectif
      } catch(err) {
        console.error("Preview non disponible : %O", err)
      }

      // Override du filepath par celui du preview
    } else {
      // Ajouter nom fichier
      const nomFichier = res.permission['nom_fichier']
      if(!nofile) {
        res.setHeader('Content-Disposition', 'attachment; filename="' + nomFichier +'"')
      }
      // res.setHeader('Content-Length', res.tailleFichier)
      var mimetype = res.permission['mimetype'] || 'application/stream'
      res.setHeader('Content-Type', mimetype)
    }

  } catch(err) {
    console.error("Erreur traitement dechiffrage stream pour %s:\n%O", req.url, err)
    debug("Permission d'acces refuse en mode %s pour %s", niveauAcces, req.url)
    return res.sendStatus(403)  // Acces refuse
  }

  debug("Info idmg: %s, stat fichier: %s", idmg, pathConsignation);

  // Cache control public, permet de faire un cache via proxy (nginx)
  res.setHeader('Cache-Control', 'public, max-age=604800, immutable')
  res.setHeader('fuuid', res.fuuid)
  res.setHeader('securite', '1.public')
  res.setHeader('Last-Modified', res.stat.mtime)

  const range = req.headers.range
  if(range) {
    console.debug("Range request : %s, taille fichier %s", range, res.stat.size)
    const infoRange = readRangeHeader(range, res.stat.size)
    res.range = infoRange
  }

  next()
}

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

async function creerStreamDechiffrage(mq, req) {
  const fuuidFichier = req.params.fuuid
  debug("Creer stream dechiffrage, query : %O", req.query)

  // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  const chainePem = mq.pki.chainePEM
  const domaineActionDemandePermission = 'GrosFichiers.demandePermissionDechiffragePublic',
        requetePermission = {fuuid: fuuidFichier}
  const reponsePermission = await mq.transmettreRequete(domaineActionDemandePermission, requetePermission)

  debug("Reponse permission access a %s:\n%O", fuuidFichier, reponsePermission)

  if( ! reponsePermission.roles_permis ) {
    debug("Permission refuse sur %s, le fichier n'est pas public", fuuidFichier)
    return {acces: '0.refuse'}
  }

  // permission['_certificat_tiers'] = chainePem
  const domaineActionDemandeCle = 'MaitreDesCles.dechiffrage'
  const reponseCle = await mq.transmettreRequete(domaineActionDemandeCle, {
    liste_hachage_bytes: reponsePermission.liste_hachage_bytes,
  })
  debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)
  if(reponseCle.acces === '0.refuse') {
    return {acces: responseCle.acces, 'err': 'Acces refuse'}
  }

  var cleChiffree, iv, fuuidEffectif = fuuidFichier, infoVideo = ''

  if(req.query.preview) {
    debug("Utiliser le preview pour extraction")
    fuuidEffectif = reponsePermission['fuuid_preview']
  } else if(req.query.video) {
    const resolution = req.query.video
    debug("Utiliser le video resolution %s pour extraction", resolution)
    // Faire une requete pour trouver le video associe a la resolution
    const domaineRequeteFichier = 'GrosFichiers.documentsParFuuid'
    const infoFichier = await mq.transmettreRequete(domaineRequeteFichier, {fuuid: fuuidFichier})
    debug("Information fichier video : %O", infoFichier)
    infoVideo = infoFichier.versions[fuuidFichier].video[resolution]
    fuuidEffectif = infoVideo.fuuid
    debug("Fuuid effectif pour video %s : %s", resolution, fuuidEffectif)
  }

  var infoClePreview = reponseCle.cles[fuuidEffectif]
  cleChiffree = infoClePreview.cle
  iv = infoClePreview.iv
  tag = infoClePreview.tag

  // Dechiffrer cle recue
  const cleDechiffree = await mq.pki.decrypterAsymetrique(cleChiffree)

  const decipherStream = getDecipherPipe4fuuid(cleDechiffree, iv, {tag})

  return {acces: reponseCle.acces, permission: reponsePermission, fuuidEffectif, decipherStream, infoVideo}
}

function readRangeHeader(range, totalLength) {
    /* src : https://www.codeproject.com/articles/813480/http-partial-content-in-node-js
     * Example of the method 'split' with regular expression.
     *
     * Input: bytes=100-200
     * Output: [null, 100, 200, null]
     *
     * Input: bytes=-200
     * Output: [null, null, 200, null]
     */

    if (range == null || range.length == 0)
        return null;

    var array = range.split(/bytes=([0-9]*)-([0-9]*)/);
    var start = parseInt(array[1]);
    var end = parseInt(array[2]);
    var result = {
        Start: isNaN(start) ? 0 : start,
        End: isNaN(end) ? (totalLength - 1) : end
    }
}

// async function stagingFichier(pathConsignation, fuuidEffectif, infoStream) {
//   // Staging de fichier public
//
//   // Verifier si le fichier existe deja
//   const pathFuuidLocal = pathConsignation.trouverPathLocal(fuuidEffectif, true)
//   const pathFuuidEffectif = path.join(pathConsignation.consignationPathDownloadStaging, fuuidEffectif)
//   var statFichier = await new Promise((resolve, reject) => {
//     // S'assurer que le path de staging existe
//     fs.mkdir(pathConsignation.consignationPathDownloadStaging, {recursive: true}, err=>{
//       if(err) return reject(err)
//       // Verifier si le fichier existe
//       fs.stat(pathFuuidEffectif, (err, stat)=>{
//         if(err) {
//           if(err.errno == -2) {
//             resolve(null)  // Le fichier n'existe pas, on va le creer
//           } else {
//             reject(err)
//           }
//         } else {
//           // Touch et retourner stat
//           const time = new Date()
//           fs.utimes(pathFuuidEffectif, time, time, err=>{
//             if(err) {
//               debug("Erreur touch %s : %o", pathFuuidEffectif, err)
//               return
//             }
//             resolve({pathFuuidLocal, filePath: pathFuuidEffectif, stat})
//           })
//         }
//       })
//     })
//   })
//
//   // Verifier si on a toute l'information
//   if(statFichier) return statFichier
//
//   // Le fichier n'existe pas, on le dechiffre dans staging
//   const outStream = fs.createWriteStream(pathFuuidEffectif, {flags: 'wx'})
//   return new Promise((resolve, reject)=>{
//     outStream.on('close', _=>{
//       fs.stat(pathFuuidEffectif, (err, stat)=>{
//         if(err) {
//           reject(err)
//         } else {
//           debug("Fin staging fichier %O", stat)
//           resolve({pathFuuidLocal, filePath: pathFuuidEffectif, stat})
//         }
//       })
//
//     })
//     outStream.on('error', err=>{
//       debug("Erreur staging fichier %s : %O", pathFuuidEffectif, err)
//       reject(err)
//     })
//
//     debug("Staging fichier %s", pathFuuidEffectif)
//     infoStream.decipherStream.writer.pipe(outStream)
//     var readStream = fs.createReadStream(pathFuuidLocal);
//     readStream.pipe(infoStream.decipherStream.reader)
//   })
//
// }
//
// function cleanupStaging() {
//   // Supprime les fichiers de staging en fonction de la derniere modification (touch)
//   const pathConsignation = new PathConsignation()
//   const pathDownloadStaging = pathConsignation.consignationPathDownloadStaging
//
//   // debug("Appel cleanupStagingDownload " + pathDownloadStaging)
//
//   fs.readdir(pathDownloadStaging, (err, files)=>{
//     if(err) {
//       if(err.code === 'ENOENT') return  // Repertoire n'existe pas
//       return console.error("cleanupStagingDownload ERROR: %O", err)
//     }
//
//     const expirationMs = new Date().getTime() - STAGING_FILE_TIMEOUT_MSEC
//
//     files.forEach(file=>{
//       const filePath = path.join(pathDownloadStaging, file)
//       fs.stat(filePath, (err, stat)=>{
//         if(err) {
//           if(err.code === 'ENOENT') return  // Repertoire n'existe pas
//           return console.error("cleanupStagingDownload ERROR: %O", err)
//         }
//
//         // debug("Info fichier %s: %O", filePath, stat)
//         if(stat.mtimeMs < expirationMs) {
//           debug("Cleanup fichier download staging %s", filePath)
//           fs.unlink(filePath, err=>{
//             if(err) debug("Erreur unlink fichier %O", err)
//           })
//         }
//       })
//     })
//
//   })
// }

module.exports = {InitialiserGrosFichiers};
