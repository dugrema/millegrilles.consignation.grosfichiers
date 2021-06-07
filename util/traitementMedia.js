const debug = require('debug')('millegrilles:fichiers:traitementMedia')
const tmp = require('tmp-promise')
const {v1: uuidv1} = require('uuid')
const path = require('path')
const fs = require('fs')
const fsPromises = require('fs/promises')

const { decrypterGCM } = require('./cryptoUtils.js')
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
      pathPreviewImageTmp = null, extension = null
  try {

    const fuuidPreviewImage = uuidv1()
    pathPreviewImageTmp = await tmp.file({
      mode: 0o600,
      postfix: '.' + message.extension,
      // dir: pathConsignation.consignationPathUploadStaging, // Meme drive que storage pour rename
    })

    // Trouver fichier original crypte    const pathFichierChiffre = this.pathConsignation.trouverPathLocal(fuuid, true);
    fichierSrcTmp = await dechiffrerTemporaire(pathConsignation, fuuid, message.extension, opts.cleSymmetrique, opts.metaCle)
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
      debug("Chiffrer preview de %s vers %s", fichierDstTmp, pathPreviewImageTmp.path)
      resultatChiffrage = await chiffrerTemporaire(mq, fichierDstTmp.path, pathPreviewImageTmp.path, opts.clesPubliques)
      debug("Resultat chiffrage preview image : %O", resultatChiffrage)
    } else {
      extension = resultatConversion.extension
    }

    // Calculer hachage fichier
    // const hachage = await calculerHachageFichier(pathPreviewImage)
    // debug("Hachage nouveau preview/thumbnail : %s", hachage)
    await _deplacerVersStorage(pathConsignation, resultatChiffrage, pathPreviewImageTmp)

    return {
      extension,
      hachage_preview: resultatChiffrage.meta.hachage_bytes,
      ...resultatConversion,
      ...resultatChiffrage
    }

  } finally {
    // Effacer le fichier temporaire
    const fichiersTmp = [fichierSrcTmp, fichierDstTmp, pathPreviewImageTmp]
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

async function dechiffrerTemporaire(pathConsignation, fuuid, extension, cleSymmetrique, metaCle, opts) {
  opts = opts || {}

  const pathFichierChiffre = pathConsignation.trouverPathLocal(fuuid, true);

  const tmpDecrypted = await tmp.file({ mode: 0o600, postfix: '.' + extension })
  const decryptedPath = tmpDecrypted.path

  // Decrypter
  // var resultatsDecryptage = await decrypteur.decrypter(
  //   pathFichierChiffre, decryptedPath, cleSymmetrique, iv, {cleFormat: 'hex'})

  await decrypterGCM(pathFichierChiffre, decryptedPath, cleSymmetrique, metaCle.iv, metaCle.tag, opts)

  return tmpDecrypted
}

async function chiffrerTemporaire(mq, fichierSrc, fichierDst, clesPubliques) {

  const writeStream = fs.createWriteStream(fichierDst);

  // Creer cipher
  const cipher = await mq.pki.creerCipherChiffrageAsymmetrique(
    clesPubliques, 'GrosFichiers', {}
  )
  // const cipher = infoChiffrage.cipher,
  //       iv = infoChiffrage.iv,
  //       clesChiffrees = infoChiffrage.certClesChiffrees

  // debug("Info chipher : iv:%s, cles:\n%O", iv, clesChiffrees)

  return new Promise((resolve, reject)=>{
    const s = fs.ReadStream(fichierSrc)
    var tailleFichier = 0
    // var ivInsere = false
    s.on('data', data => {
      // if(!ivInsere) {
      //   // Inserer le IV au debut du fichier
      //   ivInsere = true
      //   debug("Inserer IV : %O", iv)
      //   const ivBuffer = new Buffer(iv, 'base64')
      //   const contenuCrypte = cipher.update(ivBuffer);
      //   tailleFichier += contenuCrypte.length
      //   writeStream.write(contenuCrypte)
      // }
      const contenuCrypte = cipher.update(data);
      tailleFichier += contenuCrypte.length
      writeStream.write(contenuCrypte)

      // writeStream.write(data)
    })
    s.on('end', async _ => {
      const informationChiffrage = await cipher.finish()
      console.debug("Information chiffrage fichier : %O", informationChiffrage)
      // tailleFichier += contenuCrypte.length
      // writeStream.write(contenuCrypte)
      writeStream.close()
      return resolve({
        tailleFichier,
        meta: informationChiffrage.meta,
        commandeMaitreCles: informationChiffrage.commandeMaitreCles
      })
    })
  })

}

async function transcoderVideo(mq, pathConsignation, message, opts) {
  opts = opts || {}

  const {cleSymmetrique, metaCle, clesPubliques} = opts

  const fuuid = message.fuuid;
  const extension = message.extension;
  // const securite = message.securite;

  // Requete info fichier
  const infoFichier = await mq.transmettreRequete('GrosFichiers.documentsParFuuid', {fuuid})
  debug("Recue detail fichiers pour transcodage : %O", infoFichier)
  const infoVersion = infoFichier.versions[fuuid]

  let mimetype = infoVersion.mimetype.split('/')[0];
  if(mimetype !== 'video') {
    throw new Error("Erreur, type n'est pas video: " + mimetype)
  }

  mq.emettreEvenement({fuuid, progres: 1}, 'evenement.fichiers.transcodageEnCours')
  var fichierSrcTmp = '', fichierDstTmp = '', pathVideoDestTmp = ''
  try {
    fichierSrcTmp = await dechiffrerTemporaire(pathConsignation, fuuid, 'vid', cleSymmetrique, metaCle)
    fichierDstTmp = await tmp.file({ mode: 0o600, postfix: '.mp4'})
    var fichierSource = fichierSrcTmp.path
    debug("Fichier transcodage video, source dechiffree : %s", fichierSource)

    debug("Decryptage video, re-encoder en MP4, source %s sous %s", fuuid, fichierDstTmp.path);
    mq.emettreEvenement({fuuid, progres: 5}, 'evenement.fichiers.transcodageEnCours')
    var resultatMp4 = await transformationImages.genererVideoMp4_480p(fichierSource, fichierDstTmp.path);
    mq.emettreEvenement({fuuid, progres: 95}, 'evenement.fichiers.transcodageEnCours')

    // Chiffrer le video transcode
    // const fuuidVideo480p = uuidv1()
    // const pathVideo480p = pathConsignation.trouverPathLocal(fuuidVideo480p, true)

    pathVideoDestTmp = await tmp.file({
      mode: 0o600,
      postfix: '.mp4',
      // dir: pathConsignation.consignationPathUploadStaging, // Meme drive que storage pour rename
    })

    await new Promise((resolve, reject)=>{
      fs.mkdir(path.dirname(pathVideoDestTmp.path), {recursive: true}, e => {
        if(e) return reject(e)
        resolve()
      })
    })

    debug("Chiffrer le fichier transcode (%s) vers %s", fichierDstTmp.path, pathVideoDestTmp.path)
    const resultatChiffrage = await chiffrerTemporaire(mq, fichierDstTmp.path, pathVideoDestTmp.path, clesPubliques)
    debug("Resultat chiffrage fichier transcode : %O", resultatChiffrage)

    // Calculer hachage
    // const hachage = await calculerHachageFichier(pathVideo480p)
    // debug("Hachage nouveau fichier transcode : %s", hachage)

    // Deplacer le fichier vers le repertoire de storage
    await _deplacerVersStorage(pathConsignation, resultatChiffrage, pathVideoDestTmp)
    const hachage = resultatChiffrage.meta.hachage_bytes

    const resultat = {
      uuid: infoFichier.uuid,
      height: resultatMp4.height,
      bitrate: resultatMp4.bitrate,
      fuuidVideo: hachage,
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
    [fichierSrcTmp, fichierDstTmp, pathVideoDestTmp].forEach(fichier=>{
      console.debug("Cleanup fichier tmp : %O", fichier)
      fichier.cleanup()
    })
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

async function _deplacerVersStorage(pathConsignation, resultatChiffrage, pathPreviewImageTmp) {
  const hachage = resultatChiffrage.meta.hachage_bytes
  const pathPreviewImage = pathConsignation.trouverPathLocal(hachage)

  // Makedir consignation
  const pathRepertoire = path.dirname(pathPreviewImage)
  await new Promise((resolve, reject) => {
    fs.mkdir(pathRepertoire, { recursive: true }, async (err)=>{
      if(err) return reject(err)
      resolve()
    })
  })

  // Changer extension fichier destination
  debug("Renommer fichier dest pour ajouter extension : %s", pathPreviewImage)
  try {
    await fsPromises.rename(pathPreviewImageTmp.path, pathPreviewImage)
  } catch(err) {
    console.warn("WARN traitementMedia._deplacerVersStorage: move (rename) echec, on fait copy")
    await fsPromises.copyFile(pathPreviewImageTmp.path, pathPreviewImage)
    await fsPromises.unlink(pathPreviewImageTmp.path)
  }
}

module.exports = {
  genererPreviewImage, genererPreviewVideo, transcoderVideo, dechiffrerTemporaire
}
