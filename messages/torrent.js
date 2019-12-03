const path = require('path');
const fs = require('fs');
const createTorrent = require('create-torrent')
const uuidv1 = require('uuid/v4');
const {pathConsignation} = require('../util/traitementFichier');

const domaineNouveauTorrent = 'millegrilles.domaines.GrosFichiers.nouveauTorrent';
const trackers = [
  ['https://mg-dev3.maple.maceroc.com:3004/announce/'],
  ['https://mg-dev3.local:3004/announce/'],
  ['udp://mg-dev3.maple.maceroc.com:6969'],
  ['http://mg-dev3.local:6969/announce'],
  ['http://mg-dev3.maple.maceroc.com:6969/announce'],
]

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
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      this.creerNouveauTorrent(routingKey, message)}, ['commande.torrent.creerNouveau']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      this.commencerSeeding(routingKey, message)}, ['commande.torrent.commencerSeeding']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      this.arreterSeeding(routingKey, message)}, ['commande.torrent.arreterSeeding']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      this.etatSeeding(routingKey, message)}, ['requete.torrent.etatSeeding']);
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

    // Creer repertoire pour collection figee
    const nomCollection = message.nom;
    const pathCollection = pathConsignation.formatPathTorrentStagingCollection(nomCollection);
    var pathFichierTorrent = null;

    this._creerRepertoireHardlinks(message, nomCollection, pathCollection)
    .then(result=>{
      console.debug("Hard links crees");
      return this._creerFichierTorrent(message, nomCollection, pathCollection);
    })
    .then(({fichierTorrent, transaction})=>{

      console.debug("Fichier torrent cree: " + fichierTorrent);
      return this._transmettreTransactionTorrent(transaction);
    })
    .then(result=>{
      console.debug("Transaction du nouveau torrent soumise");
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
  _creerRepertoireHardlinks(message, nomCollection, pathCollection) {
    console.debug("Creer repertoire hard links");

    return new Promise((resolve, reject) => {

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

  // Genere le fichier torrent avec le contenu de la transaction
  _creerFichierTorrent(message, nomCollection, pathCollection) {
    const securite = message.securite;
    const privateTorrent = false; // securite!='1.public';

    const uuidTorrent = uuidv1();
    const fichierTorrent = pathConsignation.formatPathFichierTorrent(uuidTorrent);

    const transaction = {
      'catalogue': message.documents,
      'securite': securite,
      'uuid-collection': message.uuid,
      'uuid-torrent': uuidTorrent,
      'etiquettes': message.etiquettes,
    }

    const transactionFormattee = this.mq.formatterTransaction(domaineNouveauTorrent, transaction);

    const info = {
      'millegrilles': transactionFormattee,
    }


    const opts = {
      name: message.nom,
      comment: message.commentaires,
      createdBy: 'create-torrent/millegrilles 1.16',
      private: privateTorrent,
      announceList: trackers,
      // urlList: [String],        // web seed urls (see [bep19](http://www.bittorrent.org/beps/bep_0019.html))
      info: info
    }

    return new Promise((resolve, reject)=>{

      createTorrent(pathCollection, opts, (err, torrent) => {
        if (err) {
          reject(err);
          return;
        }

        // `torrent` is a Buffer with the contents of the new .torrent file
        fs.writeFile(fichierTorrent, torrent, err=>{
          if (err) {
            reject(err);
            return;
          }

          console.debug("Fichier torrent cree");
          resolve({fichierTorrent, transaction});
        });
      });
    });
  }

  _transmettreTransactionTorrent(transaction) {
    return this.mq.transmettreEnveloppeTransaction(transaction, domaineNouveauTorrent);
  }

  _seederTorrent(message) {

  }

}

module.exports = {TorrentMessages};
