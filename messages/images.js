const path = require('path');
const fs = require('fs');
const tmp = require('tmp-promise');
const crypto = require('crypto');
const im = require('imagemagick');
const uuidv1 = require('uuid/v1');
const { DecrypterFichier, decrypterCleSecrete, getDecipherPipe4fuuid } = require('./crypto.js')
const { Decrypteur } = require('../util/cryptoUtils.js');
const {pathConsignation} = require('../util/traitementFichier');
const transformationImages = require('../util/transformationImages');

const decrypteur = new Decrypteur();

// Traitement d'images pour creer des thumbnails et preview
class GenerateurImages {

  constructor(mq) {
    this.mq = mq;

    this.genererThumbnail.bind(this);
    this.transcoderVideoDecrypte.bind(this);
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      this.genererThumbnail(routingKey, message)}, ['commande.grosfichiers.genererThumbnailProtege']);
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      // Retourner la promise pour rendre cette operation bloquante (longue duree)
      return this.transcoderVideoDecrypte(routingKey, message)}, ['commande.grosfichiers.transcoderVideo']);
  }

  async genererThumbnail(routingKey, message) {

    const fuuid = message.fuuid;
    const cleSecreteChiffree = message.cleSecreteChiffree;
    const iv = message.iv;

    // console.debug("Message pour generer thumbnail protege " + message.fuuid);

    // Decrypter la cle secrete
    const cleSecreteDechiffree = decrypterCleSecrete(cleSecreteChiffree);

    // Trouver fichier original crypte
    const pathFichierCrypte = pathConsignation.trouverPathLocal(fuuid, true);

    // Preparer fichier destination decrypte
    // Aussi preparer un fichier tmp pour le thumbnail
    var thumbnailBase64Content, metadata;
    return await tmp.file({ mode: 0o600, postfix: '.'+message.extension }).then(async tmpDecrypted => {
      const decryptedPath = tmpDecrypted.path;
      // Decrypter
      try {
        var resultatsDecryptage = await decrypteur.decrypter(
          pathFichierCrypte, decryptedPath, cleSecreteDechiffree, iv);
        // console.debug("Fichier decrypte pour thumbnail sous " + pathTemp +
        //               ", taille " + resultatsDecryptage.tailleFichier +
        //               ", sha256 " + resultatsDecryptage.sha256Hash);

        let mimetype = message.mimetype.split('/')[0];
        if(mimetype === 'image') {
          thumbnailBase64Content = await transformationImages.genererThumbnail(decryptedPath);
        } else if(mimetype === 'video') {
          var dataVideo = await transformationImages.genererThumbnailVideo(decryptedPath);
          thumbnailBase64Content = dataVideo.base64Content;
          metadata = dataVideo.metadata;
        }

        // _imConvertPromise([decryptedPath, '-resize', '128', '-format', 'jpg', thumbnailPath]);
      } finally {
        // Effacer le fichier temporaire
        tmpDecrypted.cleanup();
      }
    })

    // Lire le fichier converti en memoire pour transformer en base64
    // convertedFile = new Buffer.from(await fs.promises.readFile(thumbnailPath)).toString("base64");

    // console.debug("Fichier converti");
    // console.debug(convertedFile);
    this._transmettreTransactionThumbnailProtege(fuuid, thumbnailBase64Content, metadata)

  }

  async transcoderVideoDecrypte(routingKey, message) {

    const fuuid = message.fuuid;
    const extension = message.extension;
    const securite = message.securite;
    // console.debug("Message transcoder video")
    // console.debug(message);

    // console.debug("Message pour generer thumbnail protege " + message.fuuid);

    const pathFichier = pathConsignation.trouverPathLocal(fuuid, false, {extension: extension});

    var thumbnailBase64Content, metadata;

    let mimetype = message.mimetype.split('/')[0];
    if(mimetype !== 'video') {
      throw new Error("Erreur, type n'est pas video: " + mimetype)
    }

    const fuuidVideo480p = uuidv1(), fuuidPreviewImage = uuidv1();
    const pathVideo480p = pathConsignation.trouverPathLocal(fuuidVideo480p, false, {extension: 'mp4'});
    const pathPreviewImage = pathConsignation.trouverPathLocal(fuuidPreviewImage, false, {extension: 'jpg'});
    return await new Promise((resolve, reject)=>{
      fs.mkdir(path.dirname(pathVideo480p), {recursive: true}, e=>{
        if(e) reject(e);
        fs.mkdir(path.dirname(pathPreviewImage), {recursive: true}, e=>{
          if(e) reject(e);
          resolve();
        })
      })
    }).then( async () => {
      // console.debug("Decryptage video, generer un preview pour " + fuuid + " sous " + fuuidPreviewImage);
      var resultatPreview = await transformationImages.genererPreviewVideoPromise(pathFichier, pathPreviewImage);

      // console.debug("Decryptage video, re-encoder en MP4, source " + fuuid + " sous " + fuuidVideo480p);
      var resultatMp4 = await transformationImages.genererVideoMp4_480p(pathFichier, pathVideo480p);

      var base64Thumbnail = await transformationImages.genererThumbnail(pathPreviewImage);

      var resultat = {};
      resultat.fuuidPreview = fuuidPreviewImage;
      resultat.thumbnail = base64Thumbnail;
      resultat.fuuidVideo480p = fuuidVideo480p;
      resultat.mimetypeVideo480p = 'video/mp4';
      resultat.tailleVideo480p = resultatMp4.tailleFichier;
      resultat.sha256Video480p = resultatMp4.sha256;
      resultat.data_video = resultatPreview.data_video;
      resultat.securite = securite;

      // console.debug("Fichier converti");
      // console.debug(convertedFile);
      this._transmettreTransactionVideoTranscode(fuuid, resultat)
    })

  }

  _transmettreTransactionThumbnailProtege(fuuid, thumbnail, metadata) {
    const domaineTransaction = 'millegrilles.domaines.GrosFichiers.associerThumbnail';

    const transaction = {fuuid, thumbnail, metadata}

    // console.debug("Transaction thumbnail protege");
    // console.debug(transaction);

    this.mq.transmettreTransactionFormattee(transaction, domaineTransaction);
  }

  _transmettreTransactionVideoTranscode(fuuid, resultat) {
    const domaineTransaction = 'millegrilles.domaines.GrosFichiers.associerVideo';

    const transaction = {fuuid, ...resultat}

    this.mq.transmettreTransactionFormattee(transaction, domaineTransaction);
  }

}

module.exports = {GenerateurImages}
