const path = require('path');
const fs = require('fs');
const {pathConsignation} = require('../util/traitementFichier');

class TorrentMessages {

  constructor(mq) {
    this.mq = mq;

    this.creerNouveauTorrent.bind(this);
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{this.creerNouveauTorrent(routingKey, message);}, ['commande.torrent.creerNouveau']);
    this.mq.routingKeyManager.addRoutingKeyCallback(this.requeteTorrent, ['requete.torrent.#']);
  }

  // transmettreCertificat() {
  //   let messageCertificat = pki.preparerMessageCertificat();
  //   let fingerprint = messageCertificat.fingerprint;
  //   let messageJSONStr = JSON.stringify(messageCertificat);
  //   this.mq._publish(
  //     'pki.certificat.' + fingerprint, messageJSONStr
  //   );
  //
  //   return fingerprint;
  // }

  creerNouveauTorrent(routingKey, message) {
    console.debug("Creer nouveau torrent");
    console.debug(message);

    this._creerRepertoireHardlinks(message)
    .then(result=>{
      console.debug("Hard links crees");

      // this._creerFichierTorrent(message);
      // this._transmettreTransactionTorrent(message);
      // this._seederTorrent(message);

    })
    .catch(err=>{
      console.error("Erreur creation torrent");
      console.error(err);
    });

  }

  requeteTorrent(routingKey, message) {
    console.debug("Requete torrent " + routingKey);
    console.debug(message);

  }

  // Fabriquer un repertoire pour la collection figee. Creer hard links pour
  // tous les fichiers de la collection.
  _creerRepertoireHardlinks(message) {
    console.debug("Creer repertoire hard links");

    // Creer repertoire pour collection figee
    const nomCollection = message.nom;
    const pathCollection = pathConsignation.formatPathTorrentCollection(nomCollection);

    return new Promise((resolve, reject) => {
      fs.mkdir(pathCollection, e=>{
        if(e) {
          reject(e);
          return;
        }

        const documents = message.documents;
        for(let idx in documents) {
          let fileDoc = documents[idx];

          let fuuid = fileDoc.fuuid;
          let securite = fileDoc.securite;
          let encrypte = securite == '3.protege' || securite == '4.protege';
          const pathFichier = pathConsignation.trouverPathLocal(fuuid, encrypte);
          console.debug("Creer hard link pour " + pathFichier);

        }

        resolve();
      })
    });

  }

  _creerFichierTorrent(message) {

  }

  _transmettreTransactionTorrent(message) {

  }

  _seederTorrent(message) {

  }

}

module.exports = {TorrentMessages};
