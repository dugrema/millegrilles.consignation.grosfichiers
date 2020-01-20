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
      return this.decrypterFichier(routingKey, message)}, ['commande.grosfichiers.decrypterFichier']);
  }

  async decrypterFichier(routingKey, message) {
    console.log("Message de declassement de grosfichiers, debut ");

    const fuuid = message.fuuid;
    const cleSecreteDecryptee = message.cleSecreteDecryptee;
    const iv = message.iv;
    const securite = message.securite;

    console.log("Message de declassement de grosfichiers " + fuuid);

    // Trouver fichier original crypte
    const pathFichierCrypte = pathConsignation.trouverPathLocal(fuuid, true);

    // Preparer fichier destination decrypte
    const fuuidFichierDecrypte = uuidv1(), fuuidPreviewImage = uuidv1();
    let extension = message.extension || path.parse(message.nomfichier).ext.toLowerCase().substr(1);
    const paramsType = {extension, mimetype: message.mimetype};
    const pathFichierDecrypte = pathConsignation.trouverPathLocal(fuuidFichierDecrypte, false, paramsType);
    const repFichierDecrypte = path.dirname(pathFichierDecrypte);

    console.debug("Creation repertoire");
    return new Promise((resolve, reject) => {
      fs.mkdir(repFichierDecrypte, {recursive: true}, async e=>{
        if(e) {
          console.error("Erreur creation repertoire pour decrypter fichier : " + repFichierDecrypte);
          reject(e);
          // return;
        } else {
          resolve();
        }
      });
    })
    .then(()=>{
      console.debug("Decrypter");
      return decrypteur.decrypter(pathFichierCrypte, pathFichierDecrypte, cleSecreteDecryptee, iv);
    })
    .then(async resultat =>{

      if ( message.mimetype && message.mimetype.split('/')[0] === 'image' ) {
        const pathPreviewImage = pathConsignation.trouverPathLocal(fuuidPreviewImage, false, {extension: 'jpg'});
        const repFichierDecrypte = path.dirname(pathFichierDecrypte);
        console.debug("Decryptage image, generer un preview pour " + fuuid + " sous " + fuuidPreviewImage);

        await transformationImages.genererPreview(pathFichierDecrypte, pathPreviewImage);
        var base64Thumbnail = await transformationImages.genererThumbnail(pathFichierDecrypte);

        resultat.fuuidPreview = fuuidPreviewImage;
        resultat.thumbnail = base64Thumbnail;
        resultat.securite = securite;
      } else if ( message.mimetype && message.mimetype.split('/')[0] === 'video' ) {
        const fuuidVideo480p = uuidv1();
        const pathVideo480p = pathConsignation.trouverPathLocal(fuuidVideo480p, false, {extension: 'mp4'});

        const pathPreviewImage = pathConsignation.trouverPathLocal(fuuidPreviewImage, false, {extension: 'jpg'});
        const repFichierDecrypte = path.dirname(pathFichierDecrypte);

        console.debug("Decryptage video, generer un preview pour " + fuuid + " sous " + fuuidPreviewImage);
        await transformationImages.genererPreviewVideoPromise(pathFichierDecrypte, pathPreviewImage);

        console.debug("Decryptage video, re-encoder en MP4, source " + fuuid + " sous " + fuuidVideo480p);
        var resultatMp4 = await transformationImages.genererVideoMp4_480p(pathFichierDecrypte, pathVideo480p);

        var base64Thumbnail = await transformationImages.genererThumbnail(pathPreviewImage);

        resultat.fuuidPreview = fuuidPreviewImage;
        resultat.thumbnail = base64Thumbnail;
        resultat.fuuidVideo480p = fuuidVideo480p;
        resultat.mimetypeVideo480p = 'video/mp4';
        resultat.tailleVideo480p = resultatMp4.tailleFichier;
        resultat.sha256Video480p = resultatMp4.sha256;
        resultat.securite = securite;
      }

      return resultat;
    })
    .then(async resultat => {
      console.debug("Resultat image/video " + fuuid);
      // console.debug(resultat);
      this._transmettreTransactionFichierDecrypte(fuuid, fuuidFichierDecrypte, resultat);
    })
    .catch(err=>{
      console.error("Erreur decryptage fichier " + pathFichierCrypte);
      console.error(err);
    })

    console.log("Fin message de declassement de grosfichiers " + fuuid);

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
    if(valeurs.fuuidVideo480p) {
      transaction.fuuidVideo480p = valeurs.fuuidVideo480p;
      transaction.mimetypeVideo480p = valeurs.mimetypeVideo480p;
      transaction.tailleVideo480p = valeurs.tailleVideo480p;
      transaction.sha256Video480p = valeurs.sha256Video480p;
    }

    // console.debug("Transaction nouveauFichierDecrypte");
    // console.debug(transaction);

    return this.mq.transmettreTransactionFormattee(transaction, domaineTransaction);
  }

}

function decrypterCleSecrete(cleChiffree) {
  // console.debug("Cle chiffree: " + cleChiffree);
  let cleSecrete = forge.util.decode64(cleChiffree);

  // Decrypter la cle secrete avec notre cle privee
  var decryptedSecretKey = PRIVATE_KEY_FORGE.decrypt(cleSecrete, 'RSA-OAEP', {
    md: forge.md.sha256.create(),
    mgf1: {
      md: forge.md.sha256.create()
    }
  });

  decryptedSecretKey = Buffer.from(forge.util.binary.hex.decode(decryptedSecretKey));
  // console.debug("Cle secrete decryptee (" + decryptedSecretKey.length + ") bytes");

  return decryptedSecretKey;
}

module.exports = {DecrypterFichier, decrypterCleSecrete}
