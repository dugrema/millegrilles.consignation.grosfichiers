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
    const uuidFichierDecrypte = uuidv1();
    const pathFichierDecrypte = pathConsignation.trouverPathLocal(uuidFichierDecrypte, false);
    const repFichierDecrypte = path.dirname(pathFichierDecrypte);

    return new Promise((resolve, reject)=>{

      fs.mkdir(repFichierDecrypte, {recursive: true}, e=>{
        if(e) {
          reject(e);
          return;
        }

        let cryptoStream = this.getDecipherPipe4fuuid(cleSecreteDecryptee, iv);

        console.log("Decryptage fichier " + fuuid + " vers " + pathFichierDecrypte);
        let writeStream = fs.createWriteStream(pathFichierDecrypte);
        cryptoStream.pipe(writeStream);

        // Ouvrir et traiter fichier
        let readStream = fs.createReadStream(pathFichierCrypte);
        readStream.pipe(writeStream);

        resolve();
      });

    });

  }

  getDecipherPipe4fuuid(cleSecrete, iv) {
    // On prepare un decipher pipe pour decrypter le contenu.

    let ivBuffer = Buffer.from(iv, 'base64');
    console.debug("IV (" + ivBuffer.length + "): ");
    console.debug(iv);

    // decryptedSecretKey = Buffer.from(forge.util.binary.hex.decode(decryptedSecretKey));
    let decryptedSecretKey = Buffer.from(cleSecrete, 'base64');
    console.debug("Cle secrete decryptee (" + decryptedSecretKey.length + ") bytes");
    console.debug(decryptedSecretKey);

    // Creer un decipher stream
    var decipher = crypto.createDecipheriv('aes256', decryptedSecretKey, ivBuffer);

    return decipher;

  }

}

module.exports = {DecrypterFichier}
