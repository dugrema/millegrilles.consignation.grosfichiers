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

    return new Promise((resolve, reject) => {

      // Creer repertoire pour collection figee
      const nomCollection = message.nom + '.' + message.uuid;
      const pathCollection = pathConsignation.formatPathTorrentCollection(nomCollection);

      fs.mkdir(pathCollection, e=>{
        if(e) {
          reject(e);
          return;
        }

        // Boucler sur les documents
        var idx = 0;
        const linkDocsLoop = function(documents) {
          let fileDoc = documents[idx++];

          let fuuid = fileDoc.fuuid;
          let securite = fileDoc.securite;
          let encrypte = securite == '3.protege' || securite == '4.secure';
          const pathFichier = pathConsignation.trouverPathLocal(fuuid, encrypte);
          console.debug("Creer hard link pour " + pathFichier);

          // Nom du fichier:
          // Si 1.public ou 2.prive: nomFichier (extension deja inclue)
          // Si 3.protege: nomFichier.mgs1 (2e extension)
          // Si 4.secure: fuuid.mgs1
          const nomFichier = ((securite!='4.secure')?fileDoc.nom+'.':'') + fileDoc.fuuid + (encrypte?'.mgs1':'');
          const newPathFichier = path.join(pathCollection, nomFichier);
          fs.link(pathFichier, newPathFichier, e=>{
            if(e) {
              reject(e);
              return;
            }

            if(documents.length < idx) {
              // Continuer boucle
              linkDocsLoop();
            } else{
              resolve();
            }
          })
        };
        linkDocsLoop(message.documents); // Demarrer boucle

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
