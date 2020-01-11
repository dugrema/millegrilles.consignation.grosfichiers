const path = require('path');
const fs = require('fs');
const { withFile } = require('tmp-promise');
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

    console.log("Message de declassement de grosfichiers");
    console.log(message);

    const fuuid = message.fuuid;
    const cleSecreteDecryptee = message.cleSecreteDecryptee;
    const iv = message.iv;

    // Trouver fichier original crypte
    const pathFichierCrypte = pathConsignation.trouverPathLocal(fuuid, true);

    // Preparer fichier destination decrypte
    // const paramsType = {mimetype: message.mimetype};
    var convertedFile;
    withFile(async (tmp2) => {
      const thumbnailPath = tmp2.path;
      await withFile(async (tmp1) => {
        const pathTemp = tmp1.path;
        // Decrypter
        var resultatsDecryptage = await decrypteur.decrypter(
          pathFichierCrypte, pathTemp, cleSecreteDecryptee, iv);
        console.debug("Fichier decrypte pour thumbnail sous " + pathTemp +
                      ", taille " + resultatsDecryptage.tailleFichier +
                      ", sha256 " + resultatsDecryptage.sha256Hash);

        await this._imConvertPromise([pathTemp, '-resize', '120', thumbnailPath]);
      })

      // Lire le fichier converti en memoire pour transformer en base64
      convertedFile = new Buffer.from(await fs.promises.readFile(thumbnailPath)).toString("base64");

      console.debug("Fichier converti");
      console.debug(convertedFile);
    })

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

  _transmettreTransactionFichierDecrypte(fuuidCrypte, fuuidDecrypte, tailleFichier, sha256Hash) {
    const domaineTransaction = 'millegrilles.domaines.GrosFichiers.nouveauFichierDecrypte';

    const transaction = {
      'fuuid_crypte': fuuidCrypte,
      'fuuid_decrypte': fuuidDecrypte,
      'taille': tailleFichier,
      'sha256Hash': sha256Hash,
    }

    console.debug("Transaction nouveauFichierDecrypte");
    console.debug(transaction);

    this.mq.transmettreTransactionFormattee(transaction, domaineTransaction);
  }

  getDecipherPipe4fuuid(cleSecrete, iv) {
    // On prepare un decipher pipe pour decrypter le contenu.

    let ivBuffer = Buffer.from(iv, 'base64');
    console.debug("IV (" + ivBuffer.length + "): ");
    console.debug(iv);

    // decryptedSecretKey = Buffer.from(forge.util.binary.hex.decode(decryptedSecretKey));
    let decryptedSecretKey = Buffer.from(cleSecrete, 'base64');
    decryptedSecretKey = decryptedSecretKey.toString('utf8');

    var typedArray = new Uint8Array(decryptedSecretKey.match(/[\da-f]{2}/gi).map(function (h) {
      return parseInt(h, 16)
    }));
    console.debug("Cle secrete decryptee (" + typedArray.length + ") bytes");

    // Creer un decipher stream
    var decipher = crypto.createDecipheriv('aes256', typedArray, ivBuffer);

    return decipher;

  }

}

module.exports = {GenerateurImages}
