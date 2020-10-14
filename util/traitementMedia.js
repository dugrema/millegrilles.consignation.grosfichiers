const debug = require('debug')('millegrilles:fichiers:traitementMedia')
const tmp = require('tmp-promise')

const { Decrypteur } = require('../util/cryptoUtils.js')
const transformationImages = require('../util/transformationImages')
const uuidv1 = require('uuid/v1')
const path = require('path')
const fs = require('fs')

const {calculerHachageFichier} = require('./utilitairesHachage')

const decrypteur = new Decrypteur()

// const crypto = require('crypto');
// const im = require('imagemagick');
// const { DecrypterFichier, decrypterCleSecrete, getDecipherPipe4fuuid } = require('./crypto.js')
// const { Decrypteur } = require('../util/cryptoUtils.js');
// const {PathConsignation} = require('../util/traitementFichier');
// const transformationImages = require('../util/transformationImages');

function genererPreviewImage(mq, pathConsignation, message, opts) {
  if(!opts) opts = {}
  const fctConversion = traiterImage
  return _genererPreview(mq, pathConsignation, message, opts, fctConversion)
}

function genererPreviewVideo(mq, pathConsignation, message, opts) {
  if(!opts) opts = {}
  const fctConversion = traiterVideo
  return _genererPreview(mq, pathConsignation, message, opts, fctConversion)
}

async function _genererPreview(mq, pathConsignation, message, opts, fctConversion) {

  const uuidDocument = message.uuid,
        fuuid = message.fuuid

  debug("Message genererPreviewImage uuid:%s/fuuid:%s, chiffre:%s", uuidDocument, fuuid, opts.iv?true:false);
  // debug("Parametres dechiffrage : %O", opts)

  var fichierSource = null, fichierDestination = null,
      fichierSrcTmp = null, fichierDstTmp = null,
      extension = null
  try {

    const fuuidPreviewImage = uuidv1()
    var pathPreviewImage = pathConsignation.trouverPathLocal(fuuidPreviewImage, false, {})

    // Makedir consignation
    const pathRepertoire = path.dirname(pathPreviewImage)
    await new Promise((resolve, reject) => {
      fs.mkdir(pathRepertoire, { recursive: true }, async (err)=>{
        if(err) return reject(err)
        resolve()
      })
    })

    if(opts.iv) {
      // Trouver fichier original crypte    const pathFichierChiffre = this.pathConsignation.trouverPathLocal(fuuid, true);
      fichierSrcTmp = await dechiffrerTemporaire(pathConsignation, fuuid, message.extension, opts.cleSymmetrique, opts.iv)
      fichierSource = fichierSrcTmp.path

      fichierDstTmp = await tmp.file({ mode: 0o600, postfix: '.' + message.extension })
      fichierDestination = fichierDstTmp.path

      extension = 'mgs1'

    } else {
      fichierSource = pathConsignation.trouverPathLocal(fuuid, false, {extension: message.extension})
      fichierDestination = pathPreviewImage
    }

    debug("Fichier (dechiffre) %s pour generer preview image", fichierSource)
    var resultatConversion = await fctConversion(fichierSource, fichierDestination)
    debug("Resultat conversion : %O", resultatConversion)
    // const mimetypePreviewImage = resultatConversion.mimetype

    var resultatChiffrage = {}
    if(fichierDstTmp) {
      // Chiffrer preview
      debug("Chiffrer preview de %s vers %s", fichierDstTmp, pathPreviewImage)
      resultatChiffrage = await chiffrerTemporaire(mq, fichierDstTmp.path, pathPreviewImage, opts.clesPubliques)
      debug("Resultat chiffrage preview image : %O", resultatChiffrage)
    } else {
      extension = resultatConversion.extension
    }

    // Calculer hachage fichier
    const hachage = await calculerHachageFichier(pathPreviewImage)
    debug("Hachage nouveau preview/thumbnail : %s", hachage)

    // Changer extension fichier destination
    const fichierDestAvecExtension = pathPreviewImage + '.' + extension
    await new Promise((resolve, reject)=>{
      debug("Renommer fichier dest pour ajouter extension : %s", fichierDestAvecExtension)
      fs.rename(pathPreviewImage, fichierDestAvecExtension, err=>{
        if(err) return reject(err)
        resolve()
      })
    })

    return {
      fuuid: fuuidPreviewImage,
      extension,
      hachage,
      ...resultatConversion,
      ...resultatChiffrage
    }

  } finally {
    // Effacer le fichier temporaire
    const fichiersTmp = [fichierSrcTmp, fichierDstTmp]
    fichiersTmp.forEach(item=>{
      try {
        if(item) {
          debug("Nettoyage fichier tmp %s", item.path)
          item.cleanup()
        }
      } catch(err) {
        console.error("Erreur suppression fichier temp %s: %O", item.path, err)
      }
    })
  }

}

async function dechiffrerTemporaire(pathConsignation, fuuid, extension, cleSymmetrique, iv) {
  const pathFichierChiffre = pathConsignation.trouverPathLocal(fuuid, true);

  const tmpDecrypted = await tmp.file({ mode: 0o600, postfix: '.' + extension })
  const decryptedPath = tmpDecrypted.path

  // Decrypter
  var resultatsDecryptage = await decrypteur.decrypter(
    pathFichierChiffre, decryptedPath, cleSymmetrique, iv, {cleFormat: 'hex'})

  return tmpDecrypted
}

async function chiffrerTemporaire(mq, fichierSrc, fichierDst, clesPubliques) {

  const writeStream = fs.createWriteStream(fichierDst);

  // Creer cipher
  const infoChiffrage = await mq.pki.creerCipherChiffrageAsymmetrique(clesPubliques)
  const cipher = infoChiffrage.cipher,
        iv = infoChiffrage.iv,
        clesChiffrees = infoChiffrage.certClesChiffrees

  debug("Info chipher : iv:%s, cles:\n%O", iv, clesChiffrees)

  return await new Promise((resolve, reject)=>{
    const s = fs.ReadStream(fichierSrc)
    var tailleFichier = 0
    s.on('data', data => {
      const contenuCrypte = cipher.update(data);
      tailleFichier += contenuCrypte.length
      writeStream.write(contenuCrypte)

      // writeStream.write(data)
    })
    s.on('end', _ => {
      const contenuCrypte = cipher.final()
      tailleFichier += contenuCrypte.length
      writeStream.write(contenuCrypte)
      writeStream.close()
      return resolve({tailleFichier, iv, clesChiffrees})
    })
  })

}

// async function genererThumbnail(routingKey, message) {
//
//   const fuuid = message.fuuid;
//   const cleSecreteChiffree = message.cleSecreteChiffree;
//   const iv = message.iv;
//
//   // console.debug("Message pour generer thumbnail protege " + message.fuuid);
//
//   // Decrypter la cle secrete
//   const cleSecreteDechiffree = decrypterCleSecrete(cleSecreteChiffree);
//
//   // Trouver fichier original crypte
//   const pathFichierCrypte = this.pathConsignation.trouverPathLocal(fuuid, true);
//
//   // Preparer fichier destination decrypte
//   // Aussi preparer un fichier tmp pour le thumbnail
//   var thumbnailBase64Content, metadata;
//   return await tmp.file({ mode: 0o600, postfix: '.'+message.extension }).then(async tmpDecrypted => {
//     const decryptedPath = tmpDecrypted.path;
//     // Decrypter
//     try {
//       var resultatsDecryptage = await decrypteur.decrypter(
//         pathFichierCrypte, decryptedPath, cleSecreteDechiffree, iv);
//       // console.debug("Fichier decrypte pour thumbnail sous " + pathTemp +
//       //               ", taille " + resultatsDecryptage.tailleFichier +
//       //               ", sha256 " + resultatsDecryptage.sha256Hash);
//
//       let mimetype = message.mimetype.split('/')[0];
//       if(mimetype === 'image') {
//         thumbnailBase64Content = await transformationImages.genererThumbnail(decryptedPath);
//       } else if(mimetype === 'video') {
//         var dataVideo = await transformationImages.genererThumbnailVideo(decryptedPath);
//         thumbnailBase64Content = dataVideo.base64Content;
//         metadata = dataVideo.metadata;
//       }
//
//       // _imConvertPromise([decryptedPath, '-resize', '128', '-format', 'jpg', thumbnailPath]);
//       this._transmettreTransactionThumbnailProtege(fuuid, thumbnailBase64Content, metadata)
//     } finally {
//       // Effacer le fichier temporaire
//       tmpDecrypted.cleanup();
//     }
//   })
//   .catch(err=>{
//     console.error("Erreur dechiffrage fichier multimedia pour thumbnail");
//     console.error(err);
//   })
//
// }

// async function transcoderVideoDecrypte(routingKey, message) {
//
//   const fuuid = message.fuuid;
//   const extension = message.extension;
//   const securite = message.securite;
//   // console.debug("Message transcoder video")
//   // console.debug(message);
//
//   // console.debug("Message pour generer thumbnail protege " + message.fuuid);
//
//   const pathFichier = this.pathConsignation.trouverPathLocal(fuuid, false, {extension: extension});
//
//   var thumbnailBase64Content, metadata;
//
//   let mimetype = message.mimetype.split('/')[0];
//   if(mimetype !== 'video') {
//     throw new Error("Erreur, type n'est pas video: " + mimetype)
//   }
//
//   const fuuidVideo480p = uuidv1(), fuuidPreviewImage = uuidv1();
//   const pathVideo480p = this.pathConsignation.trouverPathLocal(fuuidVideo480p, false, {extension: 'mp4'});
//   const pathPreviewImage = this.pathConsignation.trouverPathLocal(fuuidPreviewImage, false, {extension: 'jpg'});
//   return await new Promise((resolve, reject)=>{
//     fs.mkdir(path.dirname(pathVideo480p), {recursive: true}, e=>{
//       if(e) reject(e);
//       fs.mkdir(path.dirname(pathPreviewImage), {recursive: true}, e=>{
//         if(e) reject(e);
//         resolve();
//       })
//     })
//   }).then( async () => {
//     // console.debug("Decryptage video, generer un preview pour " + fuuid + " sous " + fuuidPreviewImage);
//     var resultatPreview = await transformationImages.genererPreviewVideoPromise(pathFichier, pathPreviewImage);
//
//     // console.debug("Decryptage video, re-encoder en MP4, source " + fuuid + " sous " + fuuidVideo480p);
//     var resultatMp4 = await transformationImages.genererVideoMp4_480p(pathFichier, pathVideo480p);
//
//     var base64Thumbnail = await transformationImages.genererThumbnail(pathPreviewImage);
//
//     var resultat = {};
//     resultat.fuuidPreview = fuuidPreviewImage;
//     resultat.thumbnail = base64Thumbnail;
//     resultat.fuuidVideo480p = fuuidVideo480p;
//     resultat.mimetypeVideo480p = 'video/mp4';
//     resultat.tailleVideo480p = resultatMp4.tailleFichier;
//     resultat.sha256Video480p = resultatMp4.sha256;
//     resultat.data_video = resultatPreview.data_video;
//     resultat.securite = securite;
//
//     // console.debug("Fichier converti");
//     // console.debug(convertedFile);
//     this._transmettreTransactionVideoTranscode(fuuid, resultat)
//   })
//   .catch(err=>{
//     console.error("Erreur conversion video");
//     console.error(err);
//   })
//
// }

function _transmettreTransactionPreview(mq, transaction) {
  const domaineTransaction = 'GrosFichiers.associerThumbnail';
  mq.transmettreTransactionFormattee(transaction, domaineTransaction);
}

function _transmettreTransactionVideoTranscode(fuuid, resultat) {
  const domaineTransaction = 'millegrilles.domaines.GrosFichiers.associerVideo';

  const transaction = {fuuid, ...resultat}

  this.mq.transmettreTransactionFormattee(transaction, domaineTransaction);
}

// Extraction de thumbnail et preview pour images
//   - pathImageSrc : path de l'image source dechiffree
async function traiterImage(pathImageSrc, pathImageDst) {
  await transformationImages.genererPreviewImage(pathImageSrc, pathImageDst)
  return {
    mimetype: 'image/jpeg',
    extension: 'jpg',
  }
}

// Extraction de thumbnail, preview et recodage des videos pour le web
async function traiterVideo(pathImageSrc, pathImageDst) {
  const dataVideo = await transformationImages.genererPreviewVideo(pathImageSrc, pathImageDst)
  //var thumbnail = await transformationImages.genererThumbnail(pathPreviewImage);

  // return new Promise((resolve, reject)=>{
  //   debug("Copie de %s", pathImageDst)
  //     fs.copyFile(pathImageDst, '/tmp/preview.jpg', err=>{
  //       debug("Copy file result : %O", err)
  //       resolve({
  //         mimetype: 'image/jpeg',
  //         extension: 'jpg',
  //         dataVideo,
  //       })
  //     })
  // })

  return {
    mimetype: 'image/jpeg',
    extension: 'jpg',
    dataVideo,
  }

}

module.exports = {
  genererPreviewImage, genererPreviewVideo,
}
