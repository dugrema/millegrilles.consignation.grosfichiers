const path = require('path');
const fs = require('fs');
const createTorrent = require('create-torrent')
const parseTorrent = require('parse-torrent')
const uuidv1 = require('uuid/v1');
const uuidv4 = require('uuid/v4');
const TransmissionRPC = require('transmission');
const crypto = require('crypto');
const uuid = require('uuid');
const {pathConsignation} = require('../util/traitementFichier');

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

// console.log(transmission);


class TorrentMessages {

  constructor(mq) {
    this.mq = mq;

    this.creerNouveauTorrent.bind(this);
    this.etatTorrent.bind(this);
    this.seederTorrent.bind(this);
    this.supprimerTorrent.bind(this);
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
      this.creerNouveauTorrent(routingKey, message, opts)}, ['commande.torrent.creerNouveau']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
      this.seederTorrent(routingKey, message, opts)}, ['commande.torrent.seederTorrent']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
      this.supprimerTorrent(routingKey, message, opts)}, ['commande.torrent.supprimer']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
      this.etatTorrent(routingKey, message, opts)}, ['requete.torrent.etat']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
      this.sommaireTransmission(routingKey, message, opts)}, ['requete.torrent.sommaire']);

    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
      this.supprimerTousTorrents(routingKey, message, opts)}, ['commande.torrent.supprimerTout']);

  }

  creerNouveauTorrent(routingKey, message, opts) {
    // console.debug("Creer nouveau torrent");
    // console.debug(message);
    // console.debug(opts);

    const {replyTo, correlationId} = opts.properties;

    const nomCollection = message.nom;
    const uuidCollection = message['uuid'];
    const pathCollection = path.join(pathConsignation.consignationPathSeeding, uuidCollection, nomCollection);
    var pathFichierTorrent = null;
    var transactionTorrent_local = null;

    this._creerRepertoireHardlinks(message, nomCollection, pathCollection)
    .then(result=>{
      // console.debug("Hard links crees");
      return this._creerFichierTorrent(message, nomCollection, pathCollection);
    })
    .then(({fichierTorrent, transactionTorrent})=>{
      transactionTorrent_local = transactionTorrent;
      // console.debug("Fichier torrent cree: " + fichierTorrent);
      // console.debug(transactionTorrent);
      pathFichierTorrent = fichierTorrent;
      return this._seederTorrent(pathFichierTorrent, uuidCollection, opts);
    })
    .then(arg=>{
      console.debug("Seeding demarre, args:");
      console.debug(arg);

      this._transmettreTransactionTorrent(transactionTorrent_local);

      // Transmettre reponse a la demande de seeding
      // transmission.get([arg.hashString], (err, reponseTorrentsActifs)=>{
      //   if(err) {
      //     throw err;
      //   }
      //
      //   // console.log("Reponse transmission.all");
      //   // console.log(reponseTorrentsActifs);
      //
      //   const reponse = {
      //     hashstring: arg.hashString,
      //     uuidCollection,
      //     seeding: true,
      //     torrents: reponseTorrentsActifs.torrents,
      //   }
      //   this.mq.transmettreReponse(reponse, replyTo, correlationId)
      //   .catch(err=>{
      //     console.error("Erreur transmission reponse etat torrent");
      //     console.error(err);
      //   })
      // });

    })
    .catch(err=>{
      console.error("Erreur creation torrent");
      console.error(err);
    });

  }

  seederTorrent(routingKey, message, opts) {
    // console.debug("Seeder torrent");
    // console.debug(message);
    // console.debug(opts);

    const {replyTo, correlationId} = opts.properties;
    // console.debug("Reply to: " + replyTo + ", correlation id " + correlationId);

    const crypte = false;   // Cryptage torrent pas encore supporte
    const uuidCollection = message['uuid'];
    const fichierTorrent = pathConsignation.formatPathFichierTorrent(uuidCollection);
    const pathTorrentConsigne = pathConsignation.trouverPathLocal(uuidCollection, crypte, {extension: 'torrent'});

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
      // console.debug("Information collection extraite du torrent");
      // console.debug("Nom collection : " + nomCollection);
      // console.debug("HashString : " + hashstring);
      // console.debug("Trackers : " + trackers);
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

      // Extraire l'extension
      for(let idx in fichiers) {
        let fichier = fichiers[idx];
        fichier.extension = path.extname(fichier.nom).toLowerCase().replace('.', '');
      }

      // console.debug("Fichiers dans torrent");
      // console.debug(fichiers);

      const pathCollection = path.join(pathConsignation.consignationPathSeeding, uuidCollection, nomCollection);
      this._creerRepertoireHardlinks({documents: fichiers}, nomCollection, pathCollection)
      .catch(err=>{
        console.error("Erreur creation hard links pour seeder");
        console.error(err);
      })
      .then(()=>{
        // console.log("Hard links collection crees");

        // Creer hard link pour fichier torrent
        const fichierTorrent = pathConsignation.formatPathFichierTorrent(uuidCollection);
        fs.link(pathTorrentConsigne, fichierTorrent, e=>{
          if(e) {
            if(e.code == 'EEXIST') {
              // console.debug("Hard link torrent existe deja");
            } else {
              console.error("Erreur creation hard link torrent");
              console.error(e);
              return;
            }
          }

          this._seederTorrent(fichierTorrent, uuidCollection)
          .then(()=>{

            // Transmettre reponse a la demande de seeding
            transmission.get([hashstring], (err, reponseTorrentsActifs)=>{
              if(err) {
                throw err;
              }

              // console.log("Reponse transmission.all");
              // console.log(reponseTorrentsActifs);

              const reponse = {
                hashstring, uuidCollection, 'seeding': true, torrents: reponseTorrentsActifs.torrents
              }
              this.mq.transmettreReponse(reponse, replyTo, correlationId)
              .catch(err=>{
                console.error("Erreur transmission reponse etat torrent");
                console.error(err);
              })

            });

          })
          .catch(err=>{
            console.error("Erreur seed torrent");
            console.error(err);
            this.mq.transmettreReponse({erreur: "Erreur d'access a transmission"}, replyTo, correlationId);
          });

        })
      })

    });

  }

  requeteTorrent(routingKey, message) {
    // console.debug("Requete torrent " + routingKey);
    // console.debug(message);

  }

  // Fabriquer un repertoire pour la collection figee. Creer hard links pour
  // tous les fichiers de la collection.
  _creerRepertoireHardlinks(message, nomCollection, pathCollection) {
    // console.debug("Creer repertoire hard links");

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
          let extension = fileDoc.extension;
          let encrypte = securite == '3.protege' || securite == '4.secure';

          // console.debug("Fichier creer link hard");
          // console.debug(fuuid);
          // console.debug(encrypte);
          // console.debug(extension);

          if(fuuid) {
            const pathFichier = pathConsignation.trouverPathLocal(fuuid, encrypte, {extension});
            // console.debug("Creer hard link pour " + pathFichier);

            // Nom du fichier:
            // Si 1.public ou 2.prive: nomFichier (extension deja inclue)
            // Si 3.protege: nomFichier.mgs1 (2e extension)
            // Si 4.secure: fuuid.mgs1
            let nomFichier;
            if(securite === '1.public') {
              // Nom tel quel, pas de changement (risque de collision)
              nomFichier = fileDoc.nom;
            } else {
              const fichierSplit = fileDoc.nom.split('.');
              const extension = fichierSplit[fichierSplit.length-1];
              const nomFichierPrefixe = fileDoc.nom.substring(0, fileDoc.nom.length - extension.length - 1);
              // console.debug("Nom fichier " + fileDoc.nom + ", extension " + extension + ", pref " + nomFichier);

              if(securite === '2.prive') {
                nomFichier = nomFichierPrefixe + '.' + fileDoc.fuuid + "." + extension;
              } else if(securite === '3.protege') {
                nomFichier = nomFichierPrefixe + '.' + fileDoc.fuuid + "." + extension + '.mgs1';
              } else if(securite === '4.secure') {
                nomFichier = fileDoc.fuuid + '.mgs1';
              }
            }

            // console.debug("Nom fichier: " + nomFichier + "(securite " + securite + ")");

            // const nomFichier = ((securite!='4.secure')?fileDoc.nom+'.':'') + fileDoc.fuuid + (encrypte?'.mgs1':'');
            const newPathFichier = path.join(pathCollection, nomFichier);
            fs.link(pathFichier, newPathFichier, e=>{
              if(e) {
                reject(e);
                return;
              }

              if(idx < documents.length) {
                // Continuer boucle
                linkDocsLoop(documents);
              } else{
                resolve();
              }
            })
          } else {
            // Pas un fichier, on skip pour le moment
            if(idx < documents.length) {
              // Continuer boucle
              linkDocsLoop(documents);
            } else{
              resolve();
            }
          }
        };
        // console.debug("Creer seed hard links");
        // console.debug(message.documents);
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
    const fichierTorrent = pathConsignation.formatPathFichierTorrent(uuidTorrent, false, {extension: 'torrent'});
    const pathTorrentConsigne = pathConsignation.trouverPathLocal(uuidTorrent, crypte, {extension: 'torrent'});

    const transactionTorrent = {
      'catalogue': message.documents,
      'securite': securite,
      'uuid-collection': message['uuid_source_figee'],
      'uuid': uuidTorrent,
      'uuid-torrent': uuidTorrent,
      'etiquettes': message.etiquettes,
      'nom': message.nom,
    }

    var transactionCopie = Object.assign({}, transactionTorrent);

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
      createdBy: 'create-torrent/millegrilles 1.22',
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
        // Calculer la taille et sha256 pour l'enregistrement du fichier
        transactionCopie.taille = torrent.length;
        var sha256 = crypto.createHash('sha256');
        sha256.update(torrent);
        transactionCopie.sha256 = sha256.digest('hex');

        // console.debug("Creation torrent, taille fichier " + transactionCopie.taille + ", sha256 " + transactionCopie.sha256);

        fs.writeFile(fichierTorrent, torrent, err=>{
          if (err) {
            reject(err);
            return;
          }

          // console.debug("Fichier torrent cree");

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

              // console.debug("Creation hard link torrent: " + pathTorrentConsigne);
            });

          })

          resolve({fichierTorrent, transactionTorrent: transactionCopie});
        });
      });
    });
  }

  _transmettreTransactionTorrent(transaction) {
    return this.mq.transmettreTransactionFormattee(transaction, domaineNouveauTorrent);
  }

  _transmettreEvenementTorrent(hashString, evenement, opts) {
    const transactionSeeding = {
      'hashstring-torrent': hashString,
      evenement,
    };
    if(opts) {
      transactionSeeding.opts = opts;
    }

    // this.mq.emettreEvenement(transactionSeeding, evenementTorrent)
    // .catch(err=>{
    //   console.error("Erreur transmission evenement");
    //   console.error(err);
    // });
  }

  _seederTorrent(pathFichierTorrent, uuidCollection) {
    const pathCollection = path.join('/torrents/seeding', uuidCollection);

    const opts = {
      'download-dir': pathCollection,
    }

    const promise = new Promise((resolve, reject) => {

      transmission.addFile(pathFichierTorrent, opts, (err, arg)=>{
        if(err) {
          console.error("Erreur seeding torrent " + pathFichierTorrent);
          reject(err);
        }
        // console.log("Seeding torrent : " + pathFichierTorrent);
        // console.log(arg);

        // Transmettre transaction pour confirmer ajout du torrent.
        // Inclue aussi la hashString
        const transactionSeeding = {
          'uuid-collection': uuidCollection,
          'hashstring-torrent': arg.hashString,
          'operation-torrent': 'seeding',
        };

        this.mq.transmettreTransactionFormattee(transactionSeeding, domaineSeedingTorrent)
        .then(()=>{
            this._transmettreEvenementTorrent(arg.hashString, 'seeding', {'uuid-collection': uuidCollection});
            resolve(arg);
        })
        .catch(err=>{
          reject(err);
        })

      });

    });

    return promise;
  }

  supprimerTorrent(routingKey, message, opts) {
    const torrentHashList = message.hashlist;
    // console.debug("Supprimer torrent " + torrentHashList);
    const {replyTo, correlationId} = opts.properties;

    const deleteFolder = true;

    transmission.remove(torrentHashList, deleteFolder, (err, arg)=>{
      if(err) {
        console.error("Erreur suppression torrents");
        console.error(err);
        return;
      }

      // console.debug("Torrents supprimes");
      // console.debug(torrentHashList);
      // console.debug(arg);

      // Transmettre reponse a la demande de seeding
      const reponse = {
        torrentHashList, 'seeding': false,
      }
      this.mq.transmettreReponse(reponse, replyTo, correlationId)
      .catch(err=>{
        console.error("Erreur transmission reponse etat torrent");
        console.error(err);
      })
      this._transmettreEvenementTorrent(torrentHashList, 'Supprimer');
    });
  }

  supprimerTousTorrents(routingKey, message, opts) {
    console.debug("Supprimer tous les torrents");
    const mq = this.mq;
    const {replyTo, correlationId} = opts.properties;

    transmission.active((err, result) => {
      if (err){
        console.error(err);
      } else {
        console.debug("Nombre torrents actifs supprimer: " + result.torrents.length);

        function prochain(compteur) {
          if(result.torrents.length > compteur) {
            var torrentId = result.torrents[compteur].id;
            transmission.remove(torrentId, (err, result)=>{
              console.debug("Torrent arrete, id: " + torrentId);
              prochain(compteur+1);
            });
          } else {
            console.debug("Torrents arretes");
            const reponse = {
              'seeding': false,
            }
            mq.transmettreReponse(reponse, replyTo, correlationId)
            .catch(err=>{
              console.error("Erreur transmission reponse etat torrent");
              console.error(err);
            })
          }
        };

        // Lancer la boucle
        prochain(0);
      }
    });
  }

  sommaireTransmission(routingKey, message, opts) {
    const correlationId = opts.properties.correlationId;
    const replyTo = opts.properties.replyTo;
    // console.debug("Sommaire transmission, repondre a Q " + replyTo + ", correlationId " + correlationId);

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
        // console.warn("sommaireTransmission: Erreur connexion")
        // console.error(err);
        transmettreReponse({erreur: "Erreur d'access a transmission"});
        return;
      }

      // console.log("Reponse transmission.sessionStats");
      // console.log(reponse);
      reponseCumulee['sessionStats'] = reponse;
      transmettreReponse(reponseCumulee);
    });

  }

  etatTorrent(routingKey, message, opts) {
    const correlationId = opts.properties.correlationId;
    const replyTo = opts.properties.replyTo;
    // console.debug("Etat transmission, repondre a Q " + replyTo + ", correlationId " + correlationId);

    const listeHashstrings = message['hashstrings'];
    // console.debug("Liste hashstrings a verifier:");
    // console.debug(listeHashstrings);

    var reponseCumulee = {};

    const transmettreReponse = (reponse) => {
      // Transmettre reponse
      this.mq.transmettreReponse(reponse, replyTo, correlationId)
      .catch(err=>{
        console.error("Erreur transmission reponse etat torrents");
        console.error(listeHashstrings);
        console.error(err);
      })
    }

    // Interroger Transmission
    transmission.get(listeHashstrings, (err, reponse)=>{
      if(err) {
        // console.error("Erreur d'access a transmission");
        // console.error(err);
        transmettreReponse({erreur: "Erreur d'access a transmission"});
        return;
      }

      // console.log("Reponse transmission.all");
      // console.log(reponse);
      reponseCumulee['torrents'] = reponse.torrents;

      transmettreReponse(reponseCumulee);
    });

  }

}

module.exports = {TorrentMessages};
