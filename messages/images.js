const path = require('path');
const fs = require('fs');
const tmp = require('tmp-promise');
const crypto = require('crypto');
const im = require('imagemagick');
const { DecrypterFichier } = require('./crypto.js')
const { Decrypteur } = require('../util/cryptoUtils.js');
const {pathConsignation} = require('../util/traitementFichier');

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

    // console.log("Message de declassement de grosfichiers");
    // console.log(message);

    const fuuid = message.fuuid;
    const cleSecreteDecryptee = message.cleSecreteDecryptee;
    const iv = message.iv;

    // Trouver fichier original crypte
    const pathFichierCrypte = pathConsignation.trouverPathLocal(fuuid, true);

    // Preparer fichier destination decrypte
    // Aussi preparer un fichier tmp pour le thumbnail
    var thumbnailBase64Content;
    await tmp.file({ mode: 0o600 }).then(async tmpDecrypted => {
      const decryptedPath = tmpDecrypted.path;
      // Decrypter
      try {
        var resultatsDecryptage = await decrypteur.decrypter(
          pathFichierCrypte, decryptedPath, cleSecreteDecryptee, iv);
        // console.debug("Fichier decrypte pour thumbnail sous " + pathTemp +
        //               ", taille " + resultatsDecryptage.tailleFichier +
        //               ", sha256 " + resultatsDecryptage.sha256Hash);

        thumbnailBase64Content = await this._genererThumbnail(decryptedPath);
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
    this._transmettreTransactionThumbnailProtege(fuuid, thumbnailBase64Content)

  }

  async _genererThumbnail(sourcePath, opts) {
    // Preparer fichier destination decrypte
    // Aussi preparer un fichier tmp pour le thumbnail
    var base64Content;
    await tmp.file({ mode: 0o600, postfix: '.jpg' }).then(async o => {

      try {
        const thumbnailPath = o.path;
        // Convertir l'image - si c'est un gif anime, le fait de mettre [0]
        // prend la premiere image de l'animation pour faire le thumbnail.
        await this._imConvertPromise([sourcePath+'[0]', '-thumbnail', '200x150>', '-quality', '50', thumbnailPath]);

        // Lire le fichier converti en memoire pour transformer en base64
        base64Content = new Buffer.from(await fs.promises.readFile(thumbnailPath)).toString("base64");
      } finally {
        // Effacer le fichier temporaire
        o.cleanup();
      }
    })

    return base64Content;
  }

  async _genererPreview(sourcePath, destinationPath, opts) {
    await this._imConvertPromise([sourcePath+'[0]', '-resize', '720x540>', destinationPath]);
  }

  _transmettreTransactionThumbnailProtege(fuuid, thumbnail) {
    const domaineTransaction = 'millegrilles.domaines.GrosFichiers.associerThumbnail';

    const transaction = {fuuid, thumbnail}

    // console.debug("Transaction thumbnail protege");
    // console.debug(transaction);

    this.mq.transmettreTransactionFormattee(transaction, domaineTransaction);
  }

  _imConvertPromise(params) {
    return new Promise((resolve, reject) => {
      im.convert(params,
        function(err, stdout){
          if (err) reject(err);
          console.log('stdout:', stdout);
          resolve();
        });
    });
  }

  getDecipherPipe4fuuid(cleSecrete, iv) {
    // On prepare un decipher pipe pour decrypter le contenu.

    let ivBuffer = Buffer.from(iv, 'base64');
    // console.debug("IV (" + ivBuffer.length + "): ");
    // console.debug(iv);

    // decryptedSecretKey = Buffer.from(forge.util.binary.hex.decode(decryptedSecretKey));
    let decryptedSecretKey = Buffer.from(cleSecrete, 'base64');
    decryptedSecretKey = decryptedSecretKey.toString('utf8');

    var typedArray = new Uint8Array(decryptedSecretKey.match(/[\da-f]{2}/gi).map(function (h) {
      return parseInt(h, 16)
    }));
    // console.debug("Cle secrete decryptee (" + typedArray.length + ") bytes");

    // Creer un decipher stream
    var decipher = crypto.createDecipheriv('aes256', typedArray, ivBuffer);

    return decipher;

  }

}

module.exports = {GenerateurImages}
