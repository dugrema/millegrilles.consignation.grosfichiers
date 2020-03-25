const fs = require('fs');
const readdirp = require('readdirp');
const path = require('path');
const uuidv1 = require('uuid/v1');
const crypto = require('crypto');
const lzma = require('lzma-native');
const { spawn } = require('child_process');

const {uuidToDate} = require('./UUIDUtils');
const rabbitMQ = require('./rabbitMQ');
const transformationImages = require('./transformationImages');

const MAP_MIMETYPE_EXTENSION = require('./mimetype_ext.json');
const MAP_EXTENSION_MIMETYPE = require('./ext_mimetype.json');

class PathConsignation {

  constructor() {
    this.idmg = process.env.MG_IDMG || 'sansnom';
    this.consignationPath = process.env.MG_CONSIGNATION_PATH ||
      path.join('/var/opt/millegrilles/mounts/consignation');

    // Path utilisable localement
    this.consignationPathLocal = path.join(this.consignationPath, '/local');
    this.consignationPathSeeding = path.join(this.consignationPath, '/torrents/seeding');
    this.consignationPathManagedTorrents = path.join(this.consignationPath, '/torrents/torrentfiles');
    this.consignationPathBackup = path.join(this.consignationPath, '/backup');
    this.consignationPathBackupHoraire = path.join(this.consignationPathBackup, '/horaire');
    this.consignationPathBackupArchives = path.join(this.consignationPathBackup, '/archives');
    this.consignationPathBackupStaging = path.join(this.consignationPathBackup, '/staging');
  }

  // Retourne le path du fichier
  // Type est un dict {mimetype, extension} ou une des deux valeurs doit etre fournie
  trouverPathLocal(fichierUuid, encrypte, type) {
    let pathFichier = this._formatterPath(fichierUuid, encrypte, type);
    return path.join(this.consignationPathLocal, pathFichier);
  }

  // Trouve un fichier existant lorsque l'extension n'est pas connue
  async trouverPathFuuidExistant(fichierUuid, encrypte) {
    let pathFichier = this._formatterPath(fichierUuid, encrypte, {});
    let pathRepertoire = path.join(this.consignationPathLocal, path.dirname(pathFichier));
    console.debug("Aller chercher fichiers du repertoire " + pathRepertoire);

    var {err, fichier} = await new Promise((resolve, reject)=>{
      fs.readdir(pathRepertoire, (err, files)=>{
        if(err) return reject(err);

        // console.debug("Liste fichiers dans " + pathRepertoire);
        // console.debug(files);

        const fichiersTrouves = files.filter(file=>{
          // console.debug("File trouve : ");
          // console.debug(file);
          return file.startsWith(fichierUuid);
        });

        if(fichiersTrouves.length == 1) {
          // On a trouve un seul fichier qui correspond au uuid, OK
          const fichierPath = path.join(pathRepertoire, fichiersTrouves[0]);
          console.debug("Fichier trouve : " + fichier);
          return resolve({fichier: fichierPath})
        } else if(fichiersTrouves.length == 0) {
          // Aucun fichier trouve
          return reject("Fichier UUID " + fichierUuid + " non trouve.");
        } else {
          // On a trouver plusieurs fichiers qui correspondent au UUID
          // C'est une erreur
          return reject("Plusieurs fichiers trouves pour fuuid " + fichierUuid);
        }

      });
    })
    .catch(err=>{
      return({err});
    });

    return {fichier};
  }

  trouverPathBackupHoraire(heureBackup) {
    let year = heureBackup.getUTCFullYear();
    let month = heureBackup.getUTCMonth() + 1; if(month < 10) month = '0'+month;
    let day = heureBackup.getUTCDate(); if(day < 10) day = '0'+day;
    let hour = heureBackup.getUTCHours(); if(hour < 10) hour = '0'+hour;

    let pathBackup =
      path.join(
        this.consignationPathBackup,
        'horaire',
        ""+year, ""+month, ""+day, ""+hour);

    return pathBackup;
  }

  formatPathFichierTorrent(nomCollection) {
    return path.join(this.consignationPathManagedTorrents, nomCollection + '.torrent');
  }

  formatPathTorrentStagingCollection(nomCollection) {
    return path.join(this.consignationPathTorrentStaging, nomCollection);
  }

  _formatterPath(fichierUuid, encrypte, type) {
    // Extrait la date du fileUuid, formatte le path en fonction de cette date.
    let timestamp = uuidToDate.extract(fichierUuid.replace('/', ''));
    // console.debug("uuid: " + fichierUuid + ". Timestamp " + timestamp);

    let extension = encrypte?'mgs1':type.extension;
    let nomFichier;
    if(extension) {
      extension = extension.toLowerCase() // Troujours lowercase
      nomFichier = fichierUuid + '.' + extension;
    } else {
      nomFichier = fichierUuid;
    }

    let year = timestamp.getUTCFullYear();
    let month = timestamp.getUTCMonth() + 1; if(month < 10) month = '0'+month;
    let day = timestamp.getUTCDate(); if(day < 10) day = '0'+day;
    let hour = timestamp.getUTCHours(); if(hour < 10) hour = '0'+hour;
    let minute = timestamp.getUTCMinutes(); if(minute < 10) minute = '0'+minute;
    let fuuide =
      path.join(""+year, ""+month, ""+day, ""+hour, ""+minute, nomFichier);

    return fuuide;
  }

}

const pathConsignation = new PathConsignation();

class TraitementFichier {

  traiterPut(req) {
    // Sauvegarde le fichier dans le repertoire de consignation local.

    const promise = new Promise((resolve, reject) => {

      try {
        // Le nom du fichier au complet, incluant path, est fourni dans fuuide.
        let headers = req.headers;
        // console.debug(headers);
        let fileUuid = headers.fileuuid;
        let encrypte = headers.encrypte === "true";
        let extension = path.parse(headers.nomfichier).ext.replace('.', '');
        let mimetype = headers.mimetype;
        let nouveauPathFichier = pathConsignation.trouverPathLocal(fileUuid, encrypte, {extension, mimetype});
        // let nouveauPathFichier = path.join(pathConsignation.consignationPathLocal, fuuide);

        // Creer le repertoire au besoin, puis deplacer le fichier (rename)
        let pathRepertoire = path.dirname(nouveauPathFichier);
        // console.debug("Path a utiliser: " + pathRepertoire + ", complet: " + nouveauPathFichier + ", extension: " + extension);
        fs.mkdir(pathRepertoire, { recursive: true }, (err)=>{
          // console.debug("Path cree: " + pathRepertoire);
          // console.debug(err);

          if(!err) {
            // console.debug("Ecriture fichier " + nouveauPathFichier);
            var sha256 = crypto.createHash('sha256');
            let writeStream = fs.createWriteStream(nouveauPathFichier, {flag: 'wx', mode: 0o440});
            writeStream.on('finish', async data=>{
              // console.debug("Fin transmission");
              // console.debug(data);

              // Comparer hash a celui du header
              let sha256Hash = sha256.digest('hex');
              // console.debug("Hash fichier remote : " + sha256Hash);

              let messageConfirmation = {
                fuuid: fileUuid,
                sha256: sha256Hash
              };

              // Verifier si on doit generer des thumbnails/preview
              if(!encrypte) {
                if(mimetype.split('/')[0] === 'image') {
                  try {
                    // console.debug("Creation preview image")
                    var imagePreviewInfo = await traiterImage(nouveauPathFichier);
                    messageConfirmation.thumbnail = imagePreviewInfo.thumbnail;
                    messageConfirmation.fuuid_preview = imagePreviewInfo.fuuidPreviewImage;
                    messageConfirmation.mimetype_preview = imagePreviewInfo.mimetypePreviewImage;
                    // console.debug("Info image, preview = " + messageConfirmation.fuuid_preview)
                  } catch (err) {
                    console.error("Erreur creation thumbnail/previews");
                    console.error(err);
                  }
                } else if(mimetype.split('/')[0] === 'video') {
                  // On genere uniquement le thumbnail - le processus va
                  // faire un appel async pour le re-encoder
                  console.debug("Traitement video");
                  var imagePreviewInfo = await traiterVideo(nouveauPathFichier);
                  messageConfirmation.thumbnail = imagePreviewInfo.thumbnail;
                  messageConfirmation.fuuid_preview = imagePreviewInfo.fuuidPreviewImage;
                  messageConfirmation.mimetype_preview = imagePreviewInfo.mimetypePreviewImage;
                  console.debug("Fuuid preview video: " + messageConfirmation.fuuid_preview)
                }
              }

              rabbitMQ.transmettreTransactionFormattee(
                messageConfirmation,
                'millegrilles.domaines.GrosFichiers.nouvelleVersion.transfertComplete')
              .then( msg => {
                console.log("Recu confirmation de nouvelleVersion transfertComplete");
                // console.log(msg);
              })
              .catch( err => {
                console.error("Erreur message");
                console.error(err);
              });

              console.log("Fichier ecrit: " + nouveauPathFichier);
              resolve({sha256Hash});
            })
            .on('error', err=>{
              console.error("Erreur sauvegarde fichier: " + nouveauPathFichier);
              reject(err);
            })

            req.on('data', chunk=>{
              // Mettre le sha256 directement dans le pipe donne le mauvais
              // resultat. L'update (avec digest plus bas) fonctionne correctement.
              sha256.update(chunk);

              // console.log('-------------');
              // process.stdout.write(chunk);
              // console.log('-------------');
            })
            .pipe(writeStream); // Traitement via event callbacks

          } else {
            reject(err);
          }

        });

      } catch (err) {
        console.error("Erreur traitement fichier " + req.headers.fuuide);
        reject(err);
      }

    });

    return promise;
  }

  // PUT pour un fichier de backup
  async traiterPutBackup(req) {
    return await new Promise(async (resolve, reject) => {
      const timestampBackup = new Date(req.body.timestamp_backup * 1000);
      if(!timestampBackup) {return reject("Il manque le timestamp du backup");}

      let pathRepertoire = pathConsignation.trouverPathBackupHoraire(timestampBackup);
      let fichiersTransactions = req.files.transactions;
      let fichierCatalogue = req.files.catalogue[0];

      // console.debug("Path a utiliser: " + pathRepertoire);

      // Deplacer les fichiers de backup vers le bon repertoire /backup
      // console.debug("Deplacers fichiers recus");
      var fichiersDomaines;
      try {
        fichiersDomaines = await traiterFichiersBackup(fichiersTransactions, fichierCatalogue, pathRepertoire);
      } catch(err) {
        // console.error("ERREUR 2!");
        // console.error(err);
        return reject(err);
      }

      // Creer les hard links pour les grosfichiers
      const fuuidDict = JSON.parse(req.body.fuuid_grosfichiers);
      if(fuuidDict && Object.keys(fuuidDict).length > 0) {
        // console.debug("Traitement grosfichiers");
        // console.debug(fuuidDict);

        const pathBackupGrosFichiers = path.join(pathRepertoire, 'grosfichiers');
        await new Promise((resolve, reject)=>{
          fs.mkdir(pathBackupGrosFichiers, { recursive: true, mode: 0o770 }, (err)=>{
            if(err) return reject(err);
            resolve();
          });
        });

        for(const fuuid in fuuidDict) {
          const paramFichier = fuuidDict[fuuid];
          // console.debug("Creer hard link pour fichier " + fuuid);

          const {err, fichier} = await pathConsignation.trouverPathFuuidExistant(fuuid);
          if(err) {
            console.error("Erreur extraction fichier " + fuuid + " pour backup");
            console.error(err);
          } else {
            if(fichier) {
              // console.debug("Fichier " + fuuid + " trouve");
              // console.debug(fichier);

              const nomFichier = path.basename(fichier);
              const pathFichierBackup = path.join(pathBackupGrosFichiers, nomFichier);

              await new Promise((resolve, reject)=>{
                fs.link(fichier, pathFichierBackup, e=>{
                  if(e) return reject(e);
                  resolve();
                });
              })
              .catch(err=>{
                console.error("Erreur link grosfichier backup : " + fichier);
                console.error(err);
              });

            } else {
              console.warn("Fichier " + fuuid + "  non trouve");
            }
          }

        }
      }

      // console.debug("PUT backup Termine");
      resolve({fichiersDomaines});

    })

  }

  async genererListeBackupsHoraire(req) {

    const domaine = req.body.domaine;
    const pathRepertoire = path.join(pathConsignation.consignationPathBackup, 'horaire');
    // console.debug("Path repertoire backup");
    // console.debug(pathRepertoire);

    const prefixeCatalogue = domaine + "_catalogue";
    const prefixeTransactions = domaine + "_transactions";

    var settings = {
      type: 'files',
      fileFilter: [
        prefixeCatalogue + '_*.json.xz',
        prefixeTransactions + '_*.json.xz',
      ],
    }

    const {err, backupsHoraire} = await new Promise((resolve, reject)=>{
      // const fichiersCatalogue = [];
      // const fichiersTransactions = [];

      const backupsHoraire = {};

      readdirp(
        pathRepertoire,
        settings,
      )
      .on('data', entry=>{
        // console.debug(entry);

        const heureBackup = entry.path.split('/').slice(0, 4).join('');
        var entreeBackup = backupsHoraire[heureBackup];
        if(!entreeBackup) {
          entreeBackup = {};
          backupsHoraire[heureBackup] = entreeBackup;
        }

        if(entry.basename.startsWith(prefixeCatalogue)) {
          entreeBackup.catalogue = entry.path;
        } else if(entry.basename.startsWith(prefixeTransactions)) {
          entreeBackup.transactions = entry.path;
        }
      })
      .on('error', err=>{
        reject({err});
      })
      .on('end', ()=>{
        // console.debug("Fini");
        resolve({backupsHoraire});
      });

    });

    if(err) throw err;

    // Trier les catalgues et transactions par date (tri naturel)
    // catalogues.sort();
    // transactions.sort();

    // return {catalogues, transactions};
    return {backupsHoraire};

  }

  async getStatFichierBackup(pathFichier, aggregation) {
    const fullPathFichier = path.join(pathConsignation.consignationPathBackup, aggregation, pathFichier);

    const {err, size} = await new Promise((resolve, reject)=>{
      fs.stat(fullPathFichier, (err, stat)=>{
        if(err) reject({err});
        resolve({size: stat.size})
      })
    });

    if(err) throw(err);

    return {size, fullPathFichier};
  }

  async sauvegarderJournalQuotidien(journal) {
    const {domaine, securite, jour} = journal;

    const dateJournal = new Date(jour*1000);
    var repertoireBackup = pathConsignation.trouverPathBackupHoraire(dateJournal);
    // Remonter du niveau heure a jour
    repertoireBackup = path.dirname(repertoireBackup);
    let year = dateJournal.getUTCFullYear();
    let month = dateJournal.getUTCMonth() + 1; if(month < 10) month = '0'+month;
    let day = dateJournal.getUTCDate(); if(day < 10) day = '0'+day;
    const dateFormattee = "" + year + month + day;

    const nomFichier = domaine + "_catalogue_" + dateFormattee + "_" + securite + ".json.xz";

    const fullPathFichier = path.join(repertoireBackup, nomFichier);

    // console.debug("Path fichier journal quotidien " + fullPathFichier);
    var compressor = lzma.createCompressor();
    var output = fs.createWriteStream(fullPathFichier);
    compressor.pipe(output);

    const promiseSauvegarde = new Promise((resolve, reject)=>{
      output.on('close', ()=>{
        resolve();
      });
      output.on('error', err=>{
        reject(err);
      })
    });

    compressor.write(JSON.stringify(journal));
    compressor.end();
    await promiseSauvegarde;

    // console.debug("Fichier cree : " + fullPathFichier);
    return {path: fullPathFichier};
  }

  async sauvegarderJournalMensuel(journal) {
    const {domaine, securite, mois} = journal;

    const dateJournal = new Date(mois*1000);
    var repertoireBackup = pathConsignation.consignationPathBackupArchives;

    let year = dateJournal.getUTCFullYear();
    let month = dateJournal.getUTCMonth() + 1; if(month < 10) month = '0'+month;
    const dateFormattee = "" + year + month;

    const nomFichier = domaine + "_catalogue_" + dateFormattee + "_" + securite + ".json.xz";

    const fullPathFichier = path.join(repertoireBackup, nomFichier);

    // console.debug("Path fichier journal mensuel " + fullPathFichier);
    var compressor = lzma.createCompressor();
    var output = fs.createWriteStream(fullPathFichier);
    compressor.pipe(output);

    const promiseSauvegarde = new Promise((resolve, reject)=>{
      output.on('close', ()=>{
        resolve();
      });
      output.on('error', err=>{
        reject(err);
      })
    });

    compressor.write(JSON.stringify(journal));
    compressor.end();
    await promiseSauvegarde;

    const sha512Journal = await utilitaireFichiers.calculerSHAFichier(fullPathFichier);

    // console.debug("Fichier cree : " + fullPathFichier);
    return {pathJournal: fullPathFichier, sha512: sha512Journal};
  }

  async creerHardLinksBackupStaging() {

    // S'assurer que les repertoires sous /staging existent
    const pathStaging = pathConsignation.consignationPathBackupStaging;
    let listeSousRepertoires = ['horaire', 'quotidien', 'mensuel', 'annuel'];
    var errMkdir = await new Promise((resolve, reject)=>{

      // Nettoyer (supprimer) repertoire staging/
      fs.rmdir(pathStaging, { recursive: true }, err=>{
        if(err) return reject(err);

        function creerRepertoires(idx) {
          if(idx === listeSousRepertoires.length) return resolve();
          let sousRepertoire = path.join(pathStaging, listeSousRepertoires[idx]);
          fs.mkdir(sousRepertoire, {recursive: true, mode: 0o770}, err=>{
            if(err) reject(err);
            else creerRepertoires(idx+1);
          })
        };
        creerRepertoires(0);

      });

    });
    if(errMkdir) throw errMkdir;

    // Creer hard-link pour tous les fichiers courants sous backup/horaire
    // vers backup/staging/horaire
    const pathBackupHoraire = pathConsignation.consignationPathBackupHoraire;
    const commandeHardLinkHoraire = spawn('cp', ['-rl', pathBackupHoraire, pathStaging]);
    commandeHardLinkHoraire.stderr.on('data', data=>{
      console.error(`Erreur nettoyage repertoires : ${data}`);
    })

    const resultatHardLinkHoraire = await new Promise(async (resolve, reject) => {
      commandeHardLinkHoraire.on('close', async code =>{
        if(code != 0) {
          return reject(code);
        }
        return resolve();
      })
    })
    .catch(err=>{
      return({err});
    });

    // Faire liste des archives et creer hard links sous staging/ approprie.
    const pathBackupArchives = pathConsignation.consignationPathBackupArchives;

    const resultatHardLinksArchives = await new Promise((resolve, reject)=>{
      fs.readdir(pathBackupArchives, (err, files)=>{
        if(err) return reject(err);

        async function createHardLink(idx) {
          if(idx === files.length) {
            // Creation hard links terminee
            const pathStagingHoraire = path.join(pathStaging, 'horaire');
            // console.debug(`Creation hard links terminee, lectures archives sous ${pathStagingHoraire}`);

            // Faire la liste des fichiers extraits - sera utilisee pour creer
            // l'ordre de traitement des fichiers pour importer les transactions
            try {
              const listeCatalogues = await genererListeCatalogues(pathStagingHoraire);
              console.debug("Liste catalogues horaire");
              console.debug(listeCatalogues);

              return resolve({listeCatalogues});
            } catch (err) {
              return reject({err});
            }

          } else {

            const nomFichier = files[idx];
            const fichier = path.join(pathBackupArchives, nomFichier);
            // console.debug(`Fichier archive, creer hard link : ${fichier}`);

            // Verifier si c'est une archive annuelle, mensuelle ou quotidienne
            const dateFichier = nomFichier.split('_')[1];

            var pathStagingFichier = null;
            if(dateFichier.length == 8) {
              // Fichier quotidien
              pathStagingFichier = path.join(pathStaging, 'quotidien', nomFichier);
            } else if(dateFichier.length == 6) {
              // Fichier mensuel
              pathStagingFichier = path.join(pathStaging, 'mensuel', nomFichier);
            } else if(dateFichier.length == 4) {
              // Fichier annuel
              pathStagingFichier = path.join(pathStaging, 'annuel', nomFichier);
            } else {
              console.warning(`Fichier non reconnu: ${fichier}`)
            }

            if(pathStagingFichier) {
              // console.debug(`Creer link ${pathStagingFichier}`);
              fs.link(fichier, pathStagingFichier, err=>{
                if(err) {
                  // Erreur existe deja est OK
                  if(err.code !== 'EEXIST') {
                    return reject({err});
                  }
                }

                createHardLink(idx+1);
              });
            } else {
              createHardLink(idx+1);
            }
          }

        };
        createHardLink(0);

      });
    });

    const {err} = resultatHardLinksArchives;
    if(err) throw err;

    return {horaire: resultatHardLinksArchives.listeCatalogues};

  }

  async extraireTarStaging() {

    const pathStaging = pathConsignation.consignationPathBackupStaging;
    const listeRepertoires = [
      path.join(pathStaging, 'annuel'),
      path.join(pathStaging, 'mensuel'),
      path.join(pathStaging, 'quotidien'),
      path.join(pathStaging, 'horaire'),
    ]

    // console.debug(`Path staging ${pathStaging}`);
    const catalogues = {};

    for(let idxRep in listeRepertoires) {
      if(idxRep == 3) continue;

      const repertoire = listeRepertoires[idxRep];
      // console.debug(`Extraire archives tar.xz sous ${repertoire}`)
      var resultatArchives = await new Promise((resolve, reject)=>{
        fs.readdir(repertoire, (err, files)=>{
          if(err) return reject({err});

          async function extraireTar(idx) {
            if(idx >= files.length-1) {

              var pathDestination = listeRepertoires[1+parseInt(idxRep)]; // Extraction granularite plus fine

              // Generer rapport de catalogues
              const cataloguesNiveau = await genererListeCatalogues(pathDestination);
              const niveau = path.basename(pathDestination);

              return resolve({niveau, catalogues: cataloguesNiveau});
            }

            const fichier = path.join(repertoire, files[idx]);

            if(fichier.endsWith('.tar.xz')) {
              // console.debug("Nom Fichier : " + fichier);
              var pathDestination = listeRepertoires[1+parseInt(idxRep)]; // Extraction granularite plus fine
              if(idxRep < 2) {
                // console.debug("File actuelle : " + files[idx]);
              } else {
                // Path horaire, il faut ajouter AAAA/MM/JJ au path

                var dateFichier = fichier.split('_')[1];
                var annee = dateFichier.slice(0, 4);
                var mois = dateFichier.slice(4, 6);
                var jour = dateFichier.slice(6, 8);
                pathDestination = path.join(pathDestination, annee, mois, jour);
                await new Promise((resolve, reject)=>{
                  fs.mkdir(pathDestination, {recursive: true, mode: 0o770}, err=>{
                    if(err) reject(err);
                    else resolve();
                  })
                })
              }

              // console.debug(`Extraire fichier ${fichier} vers ${pathDestination}`)
              await utilitaireFichiers.extraireTarFile(fichier, pathDestination);
            }
            extraireTar(idx+1);
          }
          extraireTar(0);
        });
      });

      if(resultatArchives.err) {
        throw resultatArchives.err;
      }

      catalogues[resultatArchives.niveau] = resultatArchives.catalogues;
    }

    return catalogues;
  }

}

class UtilitaireFichiers {

  async calculerSHAFichier(pathFichier, opts) {
    if(!opts) opts = {};

    let fonctionHash = opts.fonctionHash || 'sha3-512';

    // Calculer SHA512 sur fichier de backup
    const sha = crypto.createHash(fonctionHash);
    const readStream = fs.createReadStream(pathFichier);

    const resultatSha = await new Promise(async (resolve, reject)=>{
      readStream.on('data', chunk=>{
        sha.update(chunk);
      })
      readStream.on('end', ()=>{
        const resultat = sha.digest('hex');
        resolve({sha: resultat});
      });
      readStream.on('error', err=> {
        reject({err});
      });

      readStream.read();
    });

    if(resultatSha.err) {
      throw resultatSha.err;
    } else {
      return resultatSha.sha;
    }
  }

  async supprimerFichiers(fichiers, repertoire) {
    // console.debug(`Supprimer fichiers sous ${repertoire}`);
    // console.debug(fichiers);

    var resultat = await new Promise((resolve, reject)=>{

      var compteur = 0;
      function supprimer(idx) {
        if(idx == fichiers.length) {
          return resolve({});
        }

        let fichier = fichiers[idx];

        if(repertoire) {
          fichier = path.join(repertoire, fichier);
        }

        // console.debug(`Supprimer ${fichier}`);

        fs.unlink(fichier, err=>{
          if(err) {
            return reject(err);
          }
          else {
            supprimer(idx+1)
          }
        });

      }; supprimer(0);

    })
    .catch(err=>{
      return({err});
    });

    if(resultat.err) {
      throw resultat.err;
    }
  }

  async supprimerRepertoiresVides(repertoireBase) {
    const masqueRecherche = path.join(repertoireBase, '*');

    const commandeBackup = spawn('/bin/sh', ['-c', `find ${masqueRecherche} -type d -empty -delete`]);
    commandeBackup.stderr.on('data', data=>{
      console.error(`Erreur nettoyage repertoires : ${data}`);
    })

    const resultatNettoyage = await new Promise(async (resolve, reject) => {
      commandeBackup.on('close', async code =>{
        if(code != 0) {
          return reject(code);
        }
        return resolve();
      })
    })
    .catch(err=>{
      return({err});
    });
  }

  async extraireTarFile(fichierTar, destination, opts) {
    const commandeBackup = spawn('/bin/sh', ['-c', `cd ${destination}; tar -x --skip-old-files -Jf ${fichierTar}`]);
    commandeBackup.stderr.on('data', data=>{
      console.error(`Ouverture fichier tar : ${data}`);
    })

    const errResultatNettoyage = await new Promise(async (resolve, reject) => {
      commandeBackup.on('close', async code =>{
        if(code != 0) {
          return reject(new Error(`Erreur backup code ${code}`));
        }
        return resolve();
      })
    })
    .catch(err=>{
      return(err);
    });

    if(errResultatNettoyage) throw errResultatNettoyage;
  }

}

async function traiterFichiersBackup(fichiersTransactions, fichierCatalogue, pathRepertoire) {
  const pathTransactions = path.join(pathRepertoire, 'transactions');
  const pathCatalogues = path.join(pathRepertoire, 'catalogues');

  const {err, resultatHash} = await new Promise((resolve, reject)=>{

    // Creer tous les repertoires requis pour le backup
    fs.mkdir(pathTransactions, { recursive: true, mode: 0o770 }, (err)=>{
      if(err) return reject(err);
      // console.debug("Path cree " + pathTransactions);

      fs.mkdir(pathCatalogues, { recursive: true, mode: 0o770 }, (err)=>{
        if(err) return reject(err);
        // console.debug("Path cree " + pathCatalogues);
        resolve();
      })

    });
  })
  .then(async () => await new Promise((resolve, reject) => {

    // Deplacer le fichier de catalogue du backup

    const nomFichier = fichierCatalogue.originalname;
    const nouveauPath = path.join(pathCatalogues, nomFichier);

    fs.rename(fichierCatalogue.path, nouveauPath, err=>{
      if(err) return reject(err);
      resolve();
    });

  }))
  .then(async () => {
    // console.debug("Debut copie fichiers backup transaction");

    const resultatHash = {};

    async function _fctDeplacerFichier(pos) {
      if(pos === fichiersTransactions.length) {
        return {resultatHash};
      }

      const fichierDict = fichiersTransactions[pos];
      const nomFichier = fichierDict.originalname;
      const nouveauPath = path.join(pathTransactions, nomFichier);

      // Calculer SHA512 sur fichier de backup
      const sha512 = crypto.createHash('sha3-512');
      const readStream = fs.createReadStream(fichierDict.path);

      const resultatSha512 = await new Promise(async (resolve, reject)=>{
        readStream.on('data', chunk=>{
          sha512.update(chunk);
        })
        readStream.on('end', ()=>{
          const resultat = sha512.digest('hex');
          // console.debug("Resultat SHA512 fichier :");
          // console.debug(resultat);
          resolve({sha512: resultat});
        });
        readStream.on('error', err=> {
          reject({err});
        });

        // Commencer lecture stream
        // console.debug("Debut lecture fichier pour SHA512");
        readStream.read();
      });

      if(resultatSha512.err) {
        throw resultatSha512.err;
      } else {
        resultatHash[nomFichier] = resultatSha512.sha512;
      }

      // console.debug("Sauvegarde " + nouveauPath);
      fs.rename(fichierDict.path, nouveauPath, err=>{
        if(err) throw err;

        _fctDeplacerFichier(pos+1); // Loop
      });

    };

    await _fctDeplacerFichier(0);  // Lancer boucle

    return {resultatHash};

  })
  .catch(err=>{
    // Retourner l'erreur via barriere await pour faire un throw
    return {err};
  });

  if(err)  {
    throw err;
  }

  return resultatHash;

}

// Extraction de thumbnail et preview pour images
async function traiterImage(pathImage) {
  var fuuidPreviewImage = uuidv1();
  var pathPreviewImage = pathConsignation.trouverPathLocal(fuuidPreviewImage, false, {extension: 'jpg'});

  let pathRepertoire = path.dirname(pathPreviewImage);
  var thumbnail = null;

  // console.debug("Path a utiliser: " + pathRepertoire + ", complet: " + nouveauPathFichier + ", extension: " + extension);
  return await new Promise((resolve, reject) => {
    fs.mkdir(pathRepertoire, { recursive: true }, async (err)=>{
      if(err) reject(err);

      try {
        thumbnail = await transformationImages.genererThumbnail(pathImage);
        await transformationImages.genererPreview(pathImage, pathPreviewImage);
        // console.debug("2. thumbnail/preview prets")
        resolve({thumbnail, fuuidPreviewImage, mimetypePreviewImage: 'image/jpeg'});
      } catch(err) {
        console.error("Erreur traitement image thumbnail/preview");
        console.error(err);
        reject(err);
      }
    })
  })

  // return {thumbnail, fuuidPreviewImage, mimetypePreviewImage: 'image/jpeg'};
}

// Extraction de thumbnail, preview et recodage des videos pour le web
async function traiterVideo(pathVideo, sansTranscodage) {
  var fuuidPreviewImage = uuidv1();

  // Extraire un preview pleine resolution du video, faire un thumbnail
  var pathPreviewImage = pathConsignation.trouverPathLocal(fuuidPreviewImage, false, {extension: 'jpg'});
  console.debug("Path video " + pathVideo)
  console.debug("Path preview " + pathPreviewImage)

  return await new Promise((resolve, reject)=>{
    fs.mkdir(path.dirname(pathPreviewImage), { recursive: true }, async err =>{
      if(err) reject(err);

      await transformationImages.genererPreviewVideoPromise(pathVideo, pathPreviewImage);
      var thumbnail = await transformationImages.genererThumbnail(pathPreviewImage);

      // Generer une nouvelle version downsamplee du video en mp4 a 480p, 3Mbit/s
      if(!sansTranscodage) {

      }

      resolve({thumbnail, fuuidPreviewImage, mimetypePreviewImage: 'image/jpeg'});
    })
  })

}

async function genererListeCatalogues(repertoire) {
  // Faire la liste des fichiers extraits - sera utilisee pour creer
  // l'ordre de traitement des fichiers pour importer les transactions
  const settingsReaddirp = {
    type: 'files',
    fileFilter: [
       '*_catalogue_*.json.xz',
    ],
  }

  const {err, listeCatalogues} = await new Promise((resolve, reject)=>{
    const listeCatalogues = [];
    // console.debug("Lister catalogues sous " + repertoire);

    readdirp(
      repertoire,
      settingsReaddirp,
    )
    .on('data', entry=>{
      // console.debug('Catalogue trouve');
      // console.debug(entry);
      listeCatalogues.push(entry.path)
    })
    .on('error', err=>{
      reject({err});
    })
    .on('end', ()=>{
      // console.debug("Fini");
      // console.debug(listeCatalogues);
      resolve({listeCatalogues});
    });
  });

  if(err) throw err;

  // console.debug("Resultat catalogues");
  // console.debug(listeCatalogues);
  return listeCatalogues;

}

const traitementFichier = new TraitementFichier();
const utilitaireFichiers = new UtilitaireFichiers();

module.exports = {traitementFichier, pathConsignation, utilitaireFichiers};
