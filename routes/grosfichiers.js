const debug = require('debug')('millegrilles:fichiers:grosfichiers')
const express = require('express');
const path = require('path');
const fs = require('fs');
const multer = require('multer')
const bodyParser = require('body-parser')

const {PathConsignation, TraitementFichier} = require('../util/traitementFichier')
const {getDecipherPipe4fuuid} = require('../util/cryptoUtils')

// const throttle = require('@sitespeed.io/throttle');

function InitialiserGrosFichiers() {

  const router = express.Router();

  const bodyParserInstance = bodyParser.urlencoded({ extended: false })

  router.get('^/fichiers/public/:fuuid', downloadFichierPublic, pipeReponse)
  router.get('^/fichiers/:fuuid', downloadFichierLocal, pipeReponse)

  // router.post('*', bodyParserInstance, downloadFichierLocalChiffre)

  const multerProcessor = multer({dest: '/var/opt/millegrilles/consignation/multer'}).single('fichier')

  router.put('^/fichiers/*', multerProcessor, async (req, res, next) => {
    console.debug("nouveauFichier PUT %s,\nHeaders: %O\nFichiers: %O\nBody: %O", req.url, req.headers, req.file, req.body)

    const idmg = req.autorisationMillegrille.idmg
    const rabbitMQ = req.rabbitMQ  //fctRabbitMQParIdmg(idmg)
    const traitementFichier = new TraitementFichier(rabbitMQ)

    // Streamer fichier vers FS
    try {
      // Returns a promise
      const msg = await traitementFichier.traiterPut(req)

      response = {
        hachage: msg.hachage
      }

      res.end(JSON.stringify(response))

    } catch (err) {
      console.error(err);
      res.sendStatus(500);
    }

  })

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

  // if(encrypted && ['1.public', '2.prive'].includes(niveauAcces)) {
  //   // Le fichier est chiffre mais le niveau d'acces de l'usager ne supporte
  //   debug("Verifier si permission d'acces en mode %s pour %s", niveauAcces, req.url)
  //   var amqpdao = req.amqpdao
  //
  //   // pas le mode chiffre. Demander une permission de dechiffrage au domaine
  //   // et dechiffrer le fichier au vol si permis.
  //   try {
  //     const {permission, decipherStream, fuuidEffectif} = await creerStreamDechiffrage(amqpdao, fuuid, utiliserPreview)
  //
  //     // Ajouter information de dechiffrage pour la reponse
  //     res.decipherStream = decipherStream
  //     res.permission = permission
  //     res.fuuid = fuuidEffectif
  //
  //     if(fuuidEffectif !== fuuid) {
  //       // Preview
  //       res.setHeader('Content-Type', res.permission['mimetype_preview'])
  //
  //       // S'assurer que le fichier de preview existe avant de changer le filePath
  //       var previewPath = pathConsignation.trouverPathLocal(fuuidEffectif, encrypted)
  //       try {
  //         const statPreview = await new Promise((resolve, reject)=>{
  //           fs.stat(previewPath, (err, stat)=>{
  //             if(err) return reject(err)
  //             return resolve(stat)
  //           })
  //         })
  //
  //         // Changer information de fichier - on transmet preview
  //         res.stat = statPreview
  //         res.fuuid = fuuidEffectif
  //         res.filePath = previewPath
  //
  //       } catch(err) {
  //         console.error("Preview non disponible : %O", err)
  //       }
  //
  //       // Override du filepath par celui du preview
  //     } else {
  //       // Ajouter nom fichier
  //       const nomFichier = res.permission['nom_fichier']
  //       if(!nofile) {
  //         res.setHeader('Content-Disposition', 'attachment; filename="' + nomFichier +'"')
  //       }
  //       res.setHeader('Content-Length', res.permission['taille'])
  //       res.setHeader('Content-Type', res.permission['mimetype'])
  //     }
  //
  //   } catch(err) {
  //     console.error("Erreur traitement dechiffrage stream pour %s:\n%O", req.url, err)
  //     debug("Permission d'acces refuse en mode %s pour %s", niveauAcces, req.url)
  //     return res.sendStatus(403)  // Acces refuse
  //   }
  //
  // } else {
    // Transfert du fichier chiffre directement, on met les stats du filesystem
    var contentType = req.headers.mimetype || 'application/octet-stream'
    res.setHeader('Content-Length', res.stat.size)
    res.setHeader('Content-Type', contentType)
  // }

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
  debug(req.headers);
  debug(req.autorisationMillegrille)
  // debug("PARAMS\n%O", req.params)
  // debug("QUERY\n%O", req.query)

  const securite = req.headers.securite || '3.protege'
  var encrypted = true
  // if(securite === '3.protege') encrypted = true
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
  const niveauAcces = '1.public'  // req.autorisationMillegrille.securite

  // if(encrypted && ['1.public', '2.prive'].includes(niveauAcces)) {

  // Le fichier est chiffre mais le niveau d'acces de l'usager ne supporte
  debug("Verifier si permission d'acces en mode %s pour %s", niveauAcces, req.url)
  var amqpdao = req.amqpdao

  // pas le mode chiffre. Demander une permission de dechiffrage au domaine
  // et dechiffrer le fichier au vol si permis.
  try {
    const {permission, decipherStream, fuuidEffectif} = await creerStreamDechiffrage(amqpdao, fuuid, utiliserPreview)

    // Ajouter information de dechiffrage pour la reponse
    res.decipherStream = decipherStream
    res.permission = permission
    res.fuuid = fuuidEffectif

    if(fuuidEffectif !== fuuid) {
      // Preview
      res.setHeader('Content-Type', res.permission['mimetype_preview'])

      // S'assurer que le fichier de preview existe avant de changer le filePath
      var previewPath = pathConsignation.trouverPathLocal(fuuidEffectif, encrypted)
      try {
        const statPreview = await new Promise((resolve, reject)=>{
          fs.stat(previewPath, (err, stat)=>{
            if(err) return reject(err)
            return resolve(stat)
          })
        })

        // Changer information de fichier - on transmet preview
        res.stat = statPreview
        res.fuuid = fuuidEffectif
        res.filePath = previewPath

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
      res.setHeader('Content-Length', res.permission['taille'])
      res.setHeader('Content-Type', res.permission['mimetype'])
    }

  } catch(err) {
    console.error("Erreur traitement dechiffrage stream pour %s:\n%O", req.url, err)
    debug("Permission d'acces refuse en mode %s pour %s", niveauAcces, req.url)
    return res.sendStatus(403)  // Acces refuse
  }

  // } else {
  //   // Transfert du fichier chiffre directement, on met les stats du filesystem
  //   // var contentType = req.headers.mimetype || 'application/octet-stream'
  //   // res.setHeader('Content-Length', res.stat.size)
  //   // res.setHeader('Content-Type', contentType)
  //
  //   // Le fichier n'est pas public
  //   return res.sendStatus(403)  // Acces refuse
  // }

  debug("Info idmg: %s, paths: %s", idmg, pathConsignation);

  // Cache control public, permet de faire un cache via proxy (nginx)
  res.setHeader('Cache-Control', 'public, max-age=604800, immutable')
  res.setHeader('fuuid', res.fuuid)
  res.setHeader('securite', '1.public')
  res.setHeader('Last-Modified', res.stat.mtime)

  next()
}

function pipeReponse(req, res) {
  const filePath = res.filePath
  const header = res.responseHeader

  res.writeHead(200)

  // Ouvrir un stream de lecture sur le fichier chiffre
  var readStream = fs.createReadStream(filePath);

  // Transmettre les bytes a res
  if(res.decipherStream) {
    debug("Dechiffrer le fichier %s au vol", req.url)
    res.decipherStream.pipe(res)
    readStream.pipe(res.decipherStream)
  } else {
    readStream.pipe(res)
  }

}

async function creerStreamDechiffrage(mq, fuuid, utiliserPreview) {
  // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  const chainePem = mq.pki.getChainePems()
  const domaineActionDemandePermission = 'GrosFichiers.demandePermissionDechiffragePublic',
        requetePermission = {fuuid}
  const reponsePermission = await mq.transmettreRequete(domaineActionDemandePermission, requetePermission)

  debug("Reponse permission access a %s:\n%O", fuuid, reponsePermission)

  // permission['_certificat_tiers'] = chainePem
  const domaineActionDemandeCle = 'MaitreDesCles.decryptageGrosFichier'
  const reponseCle = await mq.transmettreRequete(
    domaineActionDemandeCle, reponsePermission, {noformat: true, attacherCertificat: true})
  debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)

  var cleChiffree, iv, fuuidEffectif
  if(utiliserPreview && reponsePermission['fuuid_preview']) {
    debug("Utiliser le preview pour extraction")
    fuuidEffectif = reponsePermission['fuuid_preview']
    var infoClePreview = reponseCle.cles_par_fuuid[fuuidEffectif]
    cleChiffree = infoClePreview.cle
    iv = infoClePreview.iv
  } else {
    cleChiffree = reponseCle.cle
    iv = reponseCle.iv
    fuuidEffectif = fuuid
  }

  // Dechiffrer cle recue
  const cleDechiffree = await mq.pki.decrypterAsymetrique(cleChiffree)

  // debug("Cle dechiffree prete : %O", cleDechiffree)

  const decipherStream = getDecipherPipe4fuuid(cleDechiffree, iv, {cleFormat: 'hex'})

  return {permission: reponsePermission, fuuidEffectif, decipherStream}
}

module.exports = {InitialiserGrosFichiers};
