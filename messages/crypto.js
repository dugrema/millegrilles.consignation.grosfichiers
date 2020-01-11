const path = require('path');
const fs = require('fs');
const uuidv1 = require('uuid/v1');
const crypto = require('crypto');
const { Decrypteur } = require('../util/cryptoUtils.js');
const {pathConsignation} = require('../util/traitementFichier');

const decrypteur = new Decrypteur();

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

      decrypteur.decrypter(pathFichierCrypte, pathFichierDecrypte, cleSecreteDecryptee, iv).
      then(({tailleFichier, sha256Hash})=>{
        this._transmettreTransactionFichierDecrypte(fuuid, fuuidFichierDecrypte, tailleFichier, sha256Hash);
      })
      .catch(err=>{
        console.error("Erreur decryptage fichier " + pathFichierCrypte);
        console.error(err);
      })

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

}

module.exports = {DecrypterFichier}
