const path = require('path');
const fs = require('fs');
const tmp = require('tmp-promise');
const crypto = require('crypto');
const im = require('imagemagick');
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
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      this.genererThumbnail(routingKey, message)}, ['commande.grosfichiers.genererThumbnailProtege']);
  }

  async genererThumbnail(routingKey, message) {

    console.log("Message pour generer thumbnail protege");
    console.log(message);

    const fuuid = message.fuuid;
    const cleSecreteChiffree = message.cleSecreteChiffree;
    const iv = message.iv;

    // Decrypter la cle secrete
    const cleSecreteDechiffree = decrypterCleSecrete(cleSecreteChiffree);

    // Trouver fichier original crypte
    const pathFichierCrypte = pathConsignation.trouverPathLocal(fuuid, true);

    // Preparer fichier destination decrypte
    // Aussi preparer un fichier tmp pour le thumbnail
    var thumbnailBase64Content, metadata;
    await tmp.file({ mode: 0o600, postfix: '.'+message.extension }).then(async tmpDecrypted => {
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

  _transmettreTransactionThumbnailProtege(fuuid, thumbnail, metadata) {
    const domaineTransaction = 'millegrilles.domaines.GrosFichiers.associerThumbnail';

    const transaction = {fuuid, thumbnail, metadata}

    // console.debug("Transaction thumbnail protege");
    // console.debug(transaction);

    this.mq.transmettreTransactionFormattee(transaction, domaineTransaction);
  }

}

module.exports = {GenerateurImages}
