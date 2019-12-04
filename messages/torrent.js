const path = require('path');
const fs = require('fs');
const createTorrent = require('create-torrent')
const parseTorrent = require('parse-torrent')
const uuidv1 = require('uuid/v1');
const uuidv4 = require('uuid/v4');
const TransmissionRPC = require('transmission');
const {pathConsignation} = require('../util/traitementFichier');
const uuid = require('uuid');

const domaineNouveauTorrent = 'millegrilles.domaines.GrosFichiers.nouveauTorrent';
const domaineSeedingTorrent = 'millegrilles.domaines.GrosFichiers.seedingTorrent';
const evenementTorrent = 'noeuds.source.millegrilles_domaines_GrosFichiers.torrents.evenement';

// Creer instance de transmission RPC (torrents)
const transmission = new TransmissionRPC({
  host: process.env.TRANSMISSION_HOST || 'localhost',
  port: process.env.TRANSMISSION_PORT || 9091,
  username: process.env.TRANSMISSION_USERNAME || 'millegrilles',
  password: process.env.TRANSMISSION_PASSWORD,
  ssl: false,
});

TransmissionRPC.prototype.seeds = function (callback) {

    var filtrerSeeds = function(err, result) {
      if(err) {
        // Erreur, on passe tout de suite au callback
        callback(err, null);
        return;
      }

      let seeds = [];
      for(let idx in result.torrents) {
        let torrent = result.torrents[idx];
        if(torrent.status == 5 || torrent.status == 6) {
          seeds.push(torrent);
        }
      }

      callback(null, {torrents: seeds});
    };

    var options = {
        arguments: {
            fields: [
              'activityDate', 'addedDate', 'creator', 'dateCreated',
              'error', 'errorString', 'hashString', 'id', 'isPrivate',
              'peersConnected', 'rateUpload', 'seedIdleLimit', 'seedIdleMode',
              'seedRatioLimit', 'seedRatioMode', 'status', 'totalSize'],
        },
        method: transmission.methods.torrents.get,
        tag: uuidv4(),
    };
    transmission.callServer(options, filtrerSeeds);
    return transmission;
};

console.log(transmission);


class TorrentMessages {

  constructor(mq) {
    this.mq = mq;

    this.creerNouveauTorrent.bind(this);
    this.etatTransmission.bind(this);
    this.seederTorrent.bind(this);
    this.supprimerTorrent.bind(this);
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      this.creerNouveauTorrent(routingKey, message)}, ['commande.torrent.creerNouveau']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      this.seederTorrent(routingKey, message)}, ['commande.torrent.seederTorrent']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      this.supprimerTorrent(routingKey, message)}, ['commande.torrent.supprimer']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
      this.etatTransmission(routingKey, message, opts)}, ['requete.torrent.etat']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
      this.sommaireTransmission(routingKey, message, opts)}, ['requete.torrent.sommaire']);

  }

  creerNouveauTorrent(routingKey, message) {
    console.debug("Creer nouveau torrent");
    console.debug(message);

    const nomCollection = message.nom;
    const uuidCollection = message['uuid'];
    const pathCollection = path.join(pathConsignation.consignationPathSeeding, uuidCollection, nomCollection);
    var pathFichierTorrent = null;

    this._creerRepertoireHardlinks(message, nomCollection, pathCollection)
    .then(result=>{
      console.debug("Hard links crees");
      return this._creerFichierTorrent(message, nomCollection, pathCollection);
    })
    .then(({fichierTorrent, transactionTorrent})=>{

      console.debug("Fichier torrent cree: " + fichierTorrent);
      pathFichierTorrent = fichierTorrent;
      this._seederTorrent(pathFichierTorrent, uuidCollection);
      return this._transmettreTransactionTorrent(transactionTorrent);
    })
    .then(result=>{
      console.debug("Transaction du nouveau torrent soumise");
    })
    .catch(err=>{
      console.error("Erreur creation torrent");
      console.error(err);
    });

  }

  seederTorrent(routingKey, message) {
    console.debug("Seeder torrent");
    console.debug(message);

    const crypte = false;   // Cryptage torrent pas encore supporte
    const uuidCollection = message['uuid'];
    const fichierTorrent = pathConsignation.formatPathFichierTorrent(uuidCollection);
    const pathTorrentConsigne = pathConsignation.trouverPathLocal(uuidCollection, crypte);

    fs.readFile(pathTorrentConsigne, (e, bufferTorrent)=>{
      if(e) {
        console.error("Erreur leture fichier torrent " + bufferTorrent);
        console.error(e);
        return;
      }

      const contenuTorrent = parseTorrent(bufferTorrent);
      // console.log(contenuTorrent);

      const hashstring = contenuTorrent.infoHash;
      const trackers = contenuTorrent.announce;
      const infoCollection = contenuTorrent.info.millegrilles;
      const nomCollection = contenuTorrent.name;
      console.debug("Information collection extraite du torrent");
      console.debug("Nom collection : " + nomCollection);
      console.debug("HashString : " + hashstring);
      console.debug("Trackers : " + trackers);
      // console.log(infoCollection);

      let catalogue = infoCollection.catalogue;
      let fichiers = [];
      for(let idx in catalogue) {
        let fichierCatalogue = catalogue[idx];
        let fichier = {
          nom: fichierCatalogue.nom.toString(),
          securite: fichierCatalogue.securite.toString(),
          fuuid: fichierCatalogue.fuuid.toString(),
        };
        fichiers.push(fichier);
      }

      console.debug("Fichiers dans torrent");
      console.debug(fichiers);

      const pathCollection = path.join(pathConsignation.consignationPathSeeding, uuidCollection, nomCollection);
      this._creerRepertoireHardlinks({documents: fichiers}, nomCollection, pathCollection)
      .catch(err=>{
        console.error("Erreur creation hard links pour seeder");
        console.error(err);
      })
      .then(()=>{
        console.log("Hard links collection crees");

        // Creer hard link pour fichier torrent
        const fichierTorrent = pathConsignation.formatPathFichierTorrent(uuidCollection);
        fs.link(pathTorrentConsigne, fichierTorrent, e=>{
          if(e) {
            if(e.code == 'EEXIST') {
              console.debug("Hard link torrent existe deja");
            } else {
              console.error("Erreur creation hard link torrent");
              console.error(e);
              return;
            }
          }

          this._seederTorrent(fichierTorrent, uuidCollection);
        })
      })

    });

    // const nomCollection = message.nom;
    // const uuidCollection = message['uuid'];
    // const pathCollection = path.join(pathConsignation.consignationPathSeeding, uuidCollection, nomCollection);
    // var pathFichierTorrent = null;
    //
    // this._creerRepertoireHardlinks(message, nomCollection, pathCollection)
    // .catch(err=>{
    //   console.error("Erreur creation hard links pour seeder");
    //   console.error(err);
    // })
    // .then(()=>{
    //   const uuidTorrent = message['uuid'];
    //   const fichierTorrent = pathConsignation.formatPathFichierTorrent(uuidTorrent);
    //   const pathTorrentConsigne = pathConsignation.trouverPathLocal(uuidTorrent, crypte);
    //
    //   console.debug("Fichier torrent cree: " + fichierTorrent);
    //   pathFichierTorrent = fichierTorrent;
    //   this._seederTorrent(pathFichierTorrent, uuidCollection);
    // })
    // .catch(err=>{
    //   console.error("Erreur creation torrent");
    //   console.error(err);
    // });

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

      fs.mkdir(pathCollection, {recursive: true}, e=>{
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
    const privateTorrent = securite!='1.public';
    const crypte = securite=='3.protege' || securite=='4.secure';

    const uuidTorrent = message['uuid'];
    const fichierTorrent = pathConsignation.formatPathFichierTorrent(uuidTorrent);
    const pathTorrentConsigne = pathConsignation.trouverPathLocal(uuidTorrent, crypte);

    const transactionTorrent = {
      'catalogue': message.documents,
      'securite': securite,
      'uuid-collection': message['uuid_source_figee'],
      'uuid': uuidTorrent,
      'uuid-torrent': uuidTorrent,
      'etiquettes': message.etiquettes,
      'nom': message.nom,
    }

    const transactionFormattee = this.mq.formatterTransaction(domaineNouveauTorrent, transactionTorrent);

    const info = {
      'millegrilles': transactionFormattee,
    }

    // Creer repertoire pour collection figee
    const trackers = []
    for(let idx in message.trackers) {
      let tracker = message.trackers[idx];
      trackers.push([tracker]);  // Mettre dans un array, c'est le format pour transmission rpc.
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

          // Faire un hard link dans la base de consignation
          const torrentDirName = path.dirname(pathTorrentConsigne);
          fs.mkdir(torrentDirName, {recursive: true}, e=>{
            if(e) {
              console.error("Erreur creation repertoires consignation pour fichier torrent: " + pathTorrentConsigne);
              console.error(e)
              return;
            }

            fs.link(fichierTorrent, pathTorrentConsigne, e=>{
              if(e) {
                console.error("Erreur creation hard link consignation pour fichier torrent: " + pathTorrentConsigne);
                console.error(e)
                return;
              }

              console.debug("Creation hard link torrent: " + pathTorrentConsigne);
            });

          })

          resolve({fichierTorrent, transactionTorrent});
        });
      });
    });
  }

  _transmettreTransactionTorrent(transaction) {
    return this.mq.transmettreEnveloppeTransaction(transaction, domaineNouveauTorrent);
  }

  _transmettreEvenementTorrent(hashString, evenement, opts) {
    const transactionSeeding = {
      'hashstring-torrent': hashString,
      evenement,
    };
    if(opts) {
      transactionSeeding.opts = opts;
    }

    this.mq.emettreEvenement(transactionSeeding, evenementTorrent)
    .catch(err=>{
      console.error("Erreur transmission evenement");
      console.error(err);
    });
  }

  _seederTorrent(pathFichierTorrent, uuidCollection) {
    const pathCollection = path.join('/torrents/seeding', uuidCollection);

    const opts = {
      'download-dir': pathCollection,
    }

    transmission.addFile(pathFichierTorrent, opts, (err, arg)=>{
      if(err) {
        console.error("Erreur seeding torrent " + pathFichierTorrent);
        console.error(err);
        return;
      }
      console.log("Seeding torrent : " + pathFichierTorrent);
      console.log(arg);

      // Transmettre transaction pour confirmer ajout du torrent.
      // Inclue aussi la hashString
      const transactionSeeding = {
        'uuid-collection': uuidCollection,
        'hashstring-torrent': arg.hashString,
        'operation-torrent': 'seeding',
      };

      this.mq.transmettreTransactionFormattee(transactionSeeding, domaineSeedingTorrent)
      .catch(err=>{
        console.error("Erreur transaction seeding torrent");
        console.error(err);
      });

      this._transmettreEvenementTorrent(arg.hashString, 'seeding', {'uuid-collection': uuidCollection});

    })
  }

  supprimerTorrent(routingKey, message, opts) {
    const torrentHashList = message.hashlist;
    console.debug("Supprimer torrent " + torrentHashList);

    const deleteFolder = true;

    transmission.remove(torrentHashList, deleteFolder, (err, arg)=>{
      if(err) {
        console.error("Erreur suppression torrents");
        console.error(err);
        return;
      }

      console.debug("Torrents supprimes");
      console.debug(torrentHashList);
      console.debug(arg);

      this._transmettreEvenementTorrent(torrentHashList, 'Supprimer');
    });
  }

  sommaireTransmission(routingKey, message, opts) {
    const correlationId = opts.properties.correlationId;
    const replyTo = opts.properties.replyTo;
    console.debug("Sommaire transmission, repondre a Q " + replyTo + ", correlationId " + correlationId);

    var reponseCumulee = {};
    let transmettreReponse = (reponse) => {
      // Transmettre reponse
      this.mq.transmettreReponse(reponse, replyTo, correlationId)
      .catch(err=>{
        console.error("Erreur transmission reponse etat torrent");
        console.error(err);
      })
    }

    // Interroger Transmission
    transmission.sessionStats((err, reponse)=>{
      if(err) {
        console.error(err);
        transmettreReponse({erreur: "Erreur d'access a transmission"});
        return;
      }

      console.log("Reponse transmission.sessionStats");
      console.log(reponse);
      reponseCumulee['sessionStats'] = reponse;
      transmettreReponse(reponseCumulee);
    });

  }

  etatTransmission(routingKey, message, opts) {
    const correlationId = opts.properties.correlationId;
    const replyTo = opts.properties.replyTo;
    console.debug("Etat transmission, repondre a Q " + replyTo + ", correlationId " + correlationId);

    var reponseCumulee = {};

    let transmettreReponse = (reponse) => {
      // Transmettre reponse
      this.mq.transmettreReponse(reponse, replyTo, correlationId)
      .catch(err=>{
        console.error("Erreur transmission reponse etat torrent");
        console.error(err);
      })
    }

    // Interroger Transmission
    transmission.sessionStats((err, reponse)=>{
      if(err) {
        console.error(err);
        transmettreReponse({erreur: "Erreur d'access a transmission"});
        return;
      }

      console.log("Reponse transmission.sessionStats");
      console.log(reponse);
      reponseCumulee['sessionStats'] = reponse;
      // transmettreReponse(reponseCumulee);

      transmission.seeds((err, reponse)=>{
        if(err) {
          console.error(err);
          transmettreReponse({erreur: "Erreur d'access a transmission"});
          return;
        }

        console.log("Reponse transmission.all");
        console.log(reponse);
        reponseCumulee['seeds'] = reponse.torrents;

        transmission.session((err, reponse)=>{
          if(err) {
            console.error(err);
            transmettreReponse({erreur: "Erreur d'access a transmission"});
            return;
          }

          console.log("Reponse transmission.seeding");
          console.log(reponse);
          reponseCumulee['session'] = reponse;

          transmettreReponse(reponseCumulee);
        });

      });

    })

  }

}

module.exports = {TorrentMessages};
