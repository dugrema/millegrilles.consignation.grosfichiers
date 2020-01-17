const path = require('path');
const fs = require('fs');
const uuidv1 = require('uuid/v1');
const crypto = require('crypto');
const forge = require('node-forge');
const { Decrypteur } = require('../util/cryptoUtils.js');
const {pathConsignation} = require('../util/traitementFichier');
const transformationImages = require('../util/transformationImages');

const decrypteur = new Decrypteur();

var PRIVATE_KEY_FORGE;

function chargerCleForge() {
  fs.readFile(process.env.MG_MQ_KEYFILE, (err, data)=>{
    PRIVATE_KEY_FORGE = forge.pki.privateKeyFromPem(data);
    console.debug("Cle privee chargee")
  });
}
chargerCleForge();

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

  async decrypterFichier(routingKey, message) {
    console.log("Message de declassement de grosfichiers");
    console.log(message);

    const fuuid = message.fuuid;
    const cleSecreteDecryptee = message.cleSecreteDecryptee;
    const iv = message.iv;
    const securite = message.securite;

    // Trouver fichier original crypte
    const pathFichierCrypte = pathConsignation.trouverPathLocal(fuuid, true);

    // Preparer fichier destination decrypte
    const fuuidFichierDecrypte = uuidv1(), fuuidPreviewImage = uuidv1();
    let extension = message.extension || path.parse(message.nomfichier).ext.toLowerCase().substr(1);
    const paramsType = {extension, mimetype: message.mimetype};
    const pathFichierDecrypte = pathConsignation.trouverPathLocal(fuuidFichierDecrypte, false, paramsType);
    const repFichierDecrypte = path.dirname(pathFichierDecrypte);

    await fs.mkdir(repFichierDecrypte, {recursive: true}, e=>{
      if(e) {
        console.error("Erreur creation repertoire pour decrypter fichier : " + repFichierDecrypte);
        return;
      }

      decrypteur.decrypter(pathFichierCrypte, pathFichierDecrypte, cleSecreteDecryptee, iv).
      then(async resultat =>{

        if ( message.mimetype && message.mimetype.split('/')[0] === 'image' ) {
          const pathPreviewImage = pathConsignation.trouverPathLocal(fuuidPreviewImage, false, {extension: 'jpg'});
          const repFichierDecrypte = path.dirname(pathFichierDecrypte);
          console.debug("Decryptage image, generer un preview pour " + fuuid + " sous " + fuuidPreviewImage);

          await transformationImages.genererPreview(pathFichierDecrypte, pathPreviewImage);
          var base64Thumbnail = await transformationImages.genererThumbnail(pathFichierDecrypte);

          resultat.fuuidPreview = fuuidPreviewImage;
          resultat.thumbnail = base64Thumbnail;
          resultat.securite = securite;
        }

        return resultat;
      })
      .then(resultat => {
        var tailleFichier = resultat.tailleFichier;
        var sha256Hash = resultat.sha256Hash;

        this._transmettreTransactionFichierDecrypte(fuuid, fuuidFichierDecrypte, resultat);
      })
      .catch(err=>{
        console.error("Erreur decryptage fichier " + pathFichierCrypte);
        console.error(err);
      })

    });

  }

  _transmettreTransactionFichierDecrypte(fuuidCrypte, fuuidDecrypte, valeurs) {
    const domaineTransaction = 'millegrilles.domaines.GrosFichiers.nouveauFichierDecrypte';

    const transaction = {
      'fuuid_crypte': fuuidCrypte,
      'fuuid_decrypte': fuuidDecrypte,
      'taille': valeurs.tailleFichier,
      'sha256Hash': valeurs.sha256Hash,
      'securite': valeurs.securite,
    }

    if( valeurs.fuuidPreview ) {
      transaction['fuuid_preview'] = valeurs.fuuidPreview;
      transaction['mimetype_preview'] = 'image/jpeg';
    }
    if( valeurs.thumbnail ) {
      transaction['thumbnail'] = valeurs.thumbnail;
    }

    // console.debug("Transaction nouveauFichierDecrypte");
    // console.debug(transaction);

    this.mq.transmettreTransactionFormattee(transaction, domaineTransaction);
  }

}

function decrypterCleSecrete(cleChiffree) {
  console.debug("Cle chiffree: " + cleChiffree);
  let cleSecrete = forge.util.decode64(cleChiffree);

  // Decrypter la cle secrete avec notre cle privee
  var decryptedSecretKey = PRIVATE_KEY_FORGE.decrypt(cleSecrete, 'RSA-OAEP', {
    md: forge.md.sha256.create(),
    mgf1: {
      md: forge.md.sha256.create()
    }
  });

  decryptedSecretKey = Buffer.from(forge.util.binary.hex.decode(decryptedSecretKey));
  console.debug("Cle secrete decryptee (" + decryptedSecretKey.length + ") bytes");

  return decryptedSecretKey;
}

module.exports = {DecrypterFichier, decrypterCleSecrete}
