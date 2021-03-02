const debug = require('debug')('millegrilles:fichiers:traitementMedia')
const tmp = require('tmp-promise')
const uuidv1 = require('uuid/v1')
const path = require('path')
const fs = require('fs')

// const { Decrypteur } = require('./cryptoUtils.js')
const { creerCipher, creerDecipher } = require('@dugrema/millegrilles.common/lib/chiffrage')
const { hacher } = require('@dugrema/millegrilles.common/lib/hachage')
const transformationImages = require('./transformationImages')

const { calculerHachageFichier } = require('./utilitairesHachage')

// const decrypteur = new Decrypteur()

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
    var pathPreviewImage = pathConsignation.trouverPathLocal(fuuidPreviewImage, true, {})

    // Makedir consignation
    const pathRepertoire = path.dirname(pathPreviewImage)
    await new Promise((resolve, reject) => {
      fs.mkdir(pathRepertoire, { recursive: true }, async (err)=>{
        if(err) return reject(err)
        resolve()
      })
    })

    // Trouver fichier original crypte    const pathFichierChiffre = this.pathConsignation.trouverPathLocal(fuuid, true);
    fichierSrcTmp = await dechiffrerTemporaire(pathConsignation, fuuid, message.extension, opts.cleSymmetrique, opts.iv)
    fichierSource = fichierSrcTmp.path

    fichierDstTmp = await tmp.file({ mode: 0o600, postfix: '.' + message.extension })
    fichierDestination = fichierDstTmp.path

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
    await new Promise((resolve, reject)=>{
      debug("Renommer fichier dest pour ajouter extension : %s", pathPreviewImage)
      fs.rename(pathPreviewImage, pathPreviewImage, err=>{
        if(err) return reject(err)
        resolve()
      })
    })

    return {
      fuuid: fuuidPreviewImage,
      extension,
      hachage_preview: hachage,
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
    var ivInsere = false
    s.on('data', data => {
      if(!ivInsere) {
        // Inserer le IV au debut du fichier
        ivInsere = true
        debug("Inserer IV : %O", iv)
        const ivBuffer = new Buffer(iv, 'base64')
        const contenuCrypte = cipher.update(ivBuffer);
        tailleFichier += contenuCrypte.length
        writeStream.write(contenuCrypte)
      }
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

async function transcoderVideo(mq, pathConsignation, clesPubliques, cleSymmetrique, iv, message) {

  const fuuid = message.fuuid;
  const extension = message.extension;
  // const securite = message.securite;
  console.debug("Message transcoder video")
  console.debug(message)

  // Requete info fichier
  const infoFichier = await mq.transmettreRequete('GrosFichiers.documentsParFuuid', {fuuid})
  debug("Recue detail fichiers pour transcodage : %O", infoFichier)
  const infoVersion = infoFichier.versions[fuuid]

  let mimetype = infoVersion.mimetype.split('/')[0];
  if(mimetype !== 'video') {
    throw new Error("Erreur, type n'est pas video: " + mimetype)
  }

  mq.emettreEvenement({fuuid, progres: 1}, 'evenement.fichiers.transcodageEnCours')
  var fichierSrcTmp = '', fichierDstTmp = ''
  try {
    fichierSrcTmp = await dechiffrerTemporaire(pathConsignation, fuuid, 'vid', cleSymmetrique, iv)
    fichierDstTmp = await tmp.file({ mode: 0o600, postfix: '.mp4'})
    var fichierSource = fichierSrcTmp.path
    debug("Fichier transcodage video, source dechiffree : %s", fichierSource)

    debug("Decryptage video, re-encoder en MP4, source %s sous %s", fuuid, fichierDstTmp.path);
    mq.emettreEvenement({fuuid, progres: 5}, 'evenement.fichiers.transcodageEnCours')
    var resultatMp4 = await transformationImages.genererVideoMp4_480p(fichierSource, fichierDstTmp.path);
    mq.emettreEvenement({fuuid, progres: 95}, 'evenement.fichiers.transcodageEnCours')

    // Chiffrer le video transcode
    const fuuidVideo480p = uuidv1()
    const pathVideo480p = pathConsignation.trouverPathLocal(fuuidVideo480p, true)

    await new Promise((resolve, reject)=>{
      fs.mkdir(path.dirname(pathVideo480p), {recursive: true}, e => {
        if(e) return reject(e)
        resolve()
      })
    })

    debug("Chiffrer le fichier transcode (%s) vers %s", fichierDstTmp.path, pathVideo480p)
    const resultatChiffrage = await chiffrerTemporaire(mq, fichierDstTmp.path, pathVideo480p, clesPubliques)

    // Calculer hachage
    const hachage = await calculerHachageFichier(pathVideo480p)
    debug("Hachage nouveau fichier transcode : %s", hachage)

    const resultat = {
      uuid: infoFichier.uuid,
      height: resultatMp4.height,
      bitrate: resultatMp4.bitrate,
      fuuidVideo: fuuidVideo480p,
      mimetypeVideo: 'video/mp4',
      hachage,
      ...resultatChiffrage,
    }
    // resultat.fuuidVideo480p = fuuidVideo480p
    // resultat.mimetypeVideo480p = 'video/mp4'
    // resultat.tailleVideo480p = resultatMp4.tailleFichier
    // resultat.sha256Video480p = resultatMp4.sha256
    // resultat.hachage = hachage

    console.debug("Fichier converti : %O", resultat);
    // console.debug(convertedFile);
    // this._transmettreTransactionVideoTranscode(fuuid, resultat)
    return resultat
  } catch(err) {
    console.error("Erreur conversion video");
    console.error(err);
  } finally {
    // S'assurer que les fichiers temporaires sont supprimes
    if(fichierSrcTmp) {
      fichierSrcTmp.cleanup()
      // fs.unlink(fichierSrcTmp.path, err=>{
      //   if(err) console.error("Erreur suppression fichier tmp dechiffre : %O", err)
      // })
    }
    if(fichierDstTmp) {
      fichierDstTmp.cleanup()
      // fs.unlink(fichierDstTmp.path, err=>{
      //   if(err) console.error("Erreur suppression fichier tmp transcode (dechiffre) : %O", err)
      // })
    }
  }

}

function _transmettreTransactionPreview(mq, transaction) {
  const domaineTransaction = 'GrosFichiers.associerThumbnail';
  mq.transmettreTransactionFormattee(transaction, domaineTransaction);
}

function _transmettreTransactionVideoTranscode(mq, transaction) {
  //function _transmettreTransactionVideoTranscode(fuuid, resultat) {
  const domaineTransaction = 'GrosFichiers.associerVideo';
  mq.transmettreTransactionFormattee(transaction, domaineTransaction);
  // const transaction = {fuuid, ...resultat}
  // this.mq.transmettreTransactionFormattee(transaction, domaineTransaction);
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
  return {
    mimetype: 'image/jpeg',
    extension: 'jpg',
    dataVideo,
  }
}

module.exports = {
  genererPreviewImage, genererPreviewVideo, transcoderVideo, dechiffrerTemporaire
}
