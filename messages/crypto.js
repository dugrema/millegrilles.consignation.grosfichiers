const path = require('path');
const fs = require('fs');
const uuidv1 = require('uuid/v1');
const crypto = require('crypto');
const {pathConsignation} = require('../util/traitementFichier');

class DecrypterFichier {

  constructor(mq) {
    this.mq = mq;

    this.decrypterFichier.bind(this);
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      this.decrypterFichier(routingKey, message)}, ['commande.grosfichiers.decrypterFichier']);

  }

  decrypterFichier(routingKey, message) {
    console.log("Message de declassement de grosfichiers");
    console.log(message);

    const fuuid = message.fuuid;
    const cleSecreteDecryptee = message.cleSecreteDecryptee;
    const iv = message.iv;

    // Trouver fichier original crypte
    const pathFichierCrypte = pathConsignation.trouverPathLocal(fuuid, true);

    // Preparer fichier destination decrypte
    const fuuidFichierDecrypte = uuidv1();
    let extension = message.extension || path.parse(message.nomfichier).ext.toLowerCase().substr(1);
    const paramsType = {extension, mimetype: message.mimetype};
    const pathFichierDecrypte = pathConsignation.trouverPathLocal(fuuidFichierDecrypte, false, paramsType);
    const repFichierDecrypte = path.dirname(pathFichierDecrypte);

    fs.mkdir(repFichierDecrypte, {recursive: true}, e=>{
      if(e) {
        console.error("Erreur creation repertoire pour decrypter fichier : " + repFichierDecrypte);
        return;
      }

      let cryptoStream = this.getDecipherPipe4fuuid(cleSecreteDecryptee, iv);

      // Calculer taille et sha256 du fichier decrypte. Necessaire pour transaction.
      const sha256 = crypto.createHash('sha256');
      var sha256Hash = null;
      var tailleFichier = 0;
      cryptoStream.on('data', chunk=>{
        sha256.update(chunk);
        tailleFichier = tailleFichier + chunk.length;
      });
      cryptoStream.on('end', ()=>{
        // Comparer hash a celui du header
        sha256Hash = sha256.digest('hex');
        console.debug("Hash fichier " + sha256Hash);
      });

      console.log("Decryptage fichier " + fuuid + " vers " + pathFichierDecrypte);
      let writeStream = fs.createWriteStream(pathFichierDecrypte);

      writeStream.on('close', ()=>{
        console.debug("Fermeture fichier decrypte");
        this._transmettreTransactionFichierDecrypte(fuuid, fuuidFichierDecrypte, tailleFichier, sha256Hash);
      });
      writeStream.on('error', ()=>{
        console.error("Erreur decryptage fichier");
      });

      cryptoStream.pipe(writeStream);

      // Ouvrir et traiter fichier
      let readStream = fs.createReadStream(pathFichierCrypte);
      readStream.pipe(cryptoStream);

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

module.exports = {DecrypterFichier}
