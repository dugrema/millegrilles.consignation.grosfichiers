const fs = require('fs');
const readdirp = require('readdirp');
const path = require('path');
const uuidv1 = require('uuid/v1');
const crypto = require('crypto');
const lzma = require('lzma-native');
const { spawn } = require('child_process');
const readline = require('readline');

const {uuidToDate} = require('./UUIDUtils');
const rabbitMQ = require('./rabbitMQ');
const transformationImages = require('./transformationImages');
const {pki, ValidateurSignature} = require('./pki');

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

}

// Classe qui s'occupe du staging d'archives et fichiers de backup
// Prepare et valide le contenu du repertoire staging/
class RestaurateurBackup {

  constructor(opts) {
    if(!opts) opts = {};

    // Configuration optionnelle
    this.pathBackupHoraire = opts.backupHoraire || pathConsignation.consignationPathBackupHoraire;
    this.pathBackupArchives = opts.archives || pathConsignation.consignationPathBackupArchives;
    this.pathStaging = opts.staging || pathConsignation.consignationPathBackupStaging;

    // Liste des niveaux d'aggregation en ordre annuel vers horaire
    this.listeAggregation = [
      'annuel',
      'mensuel',
      'quotidien',
      'horaire'
    ];

    this.pathAggregationStaging = {
      horaire: opts.stagingBackupHoraire || path.join(this.pathStaging, 'horaire'),
      quotidien: opts.stagingBackupQuotidien || path.join(this.pathStaging, 'quotidien'),
      mensuel: opts.stagingBackupMensuel || path.join(this.pathStaging, 'mensuel'),
      annuel: opts.stagingBackupAnnuel || path.join(this.pathStaging, 'annuel'),
    }

    // Conserve la plus recente information de backup horaire par domaine/securite
    // Cle: domaine/securite, e.g. "millegrilles.domaines.SenseursPassifs/2.prive"
    // Valeur: Valeur de hachage de l'entete dans le catalogue, e.g. :
    // {
    //     "hachage_entete": "5uPgda0G9u/rxN89PT38Y6noxpX90TM7x5F30zcNHxi5AwMwrIqblqf+llmVU7tbJYQphhD/Q4UNvSUS13vybA==",
    //     "uuid-transaction": "dccc3059-6ef5-11ea-8ed2-00155d011f09"
    // }
    this.chaineBackupHoraire = {};
    this.validateurSignature = new ValidateurSignature();

  }

  // Methode qui lance une restauration complete d'une MilleGrille
  // Le consignateur de fichiers va extraire, valider et re-transmettre
  // toutes les transactions de tous les domaines.
  async restaurationComplete() {
    console.debug("Debut restauration complete");

    // Creer hard-links pour archives existantes sous staging/
    const rapportHardLinks = await this.creerHardLinksBackupStaging();

    // Extraire et verifier tous les fichiers d'archives vers staging/horaire
    const rapportTarExtraction = await this.extraireTarParNiveaux();

    // Verifier le contenu horaire
    const rapportVerificationHoraire = await this.parcourirCataloguesHoraire(
      (pathCourant, fichierCatalogue, catalogue) => {
        return this.verifierBackupHoraire(pathCourant, fichierCatalogue, catalogue);
      }
    );

    // Retransmettre toutes les transactions
    const rapportRetransmissionTransactions = await this.parcourirCataloguesHoraire(
      (pathCourant, fichierCatalogue, catalogue) => {
        return this.resoumettreTransactions(pathCourant, fichierCatalogue, catalogue);
      }
    );


    const rapports = {
      rapportHardLinks,
      rapportTarExtraction,
      rapportVerificationHoraire
    }

    // Verifie les SHA des archives pour chaque archive a partir des catalogues
    console.debug("Fin restauration complete");
    return {rapports};
  }

  async creerHardLinksBackupStaging() {

    // Extraire variables du scope _this_ pour fonctions async
    const {
      listeAggregation, pathBackupHoraire, pathBackupArchives,
      pathStaging, pathAggregationStaging,
    } = this;

    // S'assurer que les repertoires sous /staging existent
    var errMkdir = await new Promise((resolve, reject)=>{

      const listeStagingDirs = Object.values(this.pathAggregationStaging);

      // Nettoyer (supprimer) repertoire staging/
      fs.rmdir(pathStaging, { recursive: true }, err=>{
        if(err) return reject(err);

        function creerRepertoires(idx) {
          if(idx === listeStagingDirs.length) return resolve();
          let sousRepertoire = listeStagingDirs[idx];
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
    const resultatHardLinkHoraire = await new Promise(async (resolve, reject) => {

      const commandeHardLinkHoraire = spawn(
        '/bin/sh',
        ['-c', `cp -rl ${pathBackupHoraire}/* ${pathAggregationStaging.horaire}/`]
      );
      commandeHardLinkHoraire.stderr.on('data', data=>{
        console.error(`Erreur nettoyage repertoires : ${data}`);
      })

      commandeHardLinkHoraire.on('close', async code =>{
        if(code != 0) {
          return reject(code);
        }
        return resolve();
      });

    })
    .catch(err=>{
      return({err});
    });

    // Faire liste des archives et creer hard links sous staging/ approprie.
    const errResultatHardLinksArchives = await new Promise((resolve, reject)=>{
      fs.readdir(pathBackupArchives, (err, files)=>{
        if(err) return reject(err);

        async function createHardLink(idx) {
          if(idx === files.length) {
            // Creation hard links terminee
            // console.debug(`Creation hard links terminee, lectures archives sous ${pathAggregationStaging.horaire}`);

            // Faire la liste des fichiers extraits - sera utilisee pour creer
            // l'ordre de traitement des fichiers pour importer les transactions
            // try {
            //   const listeCatalogues = await genererListeCatalogues(pathAggregationStaging.horaire);
            //   console.debug("Liste catalogues horaire");
            //   console.debug(listeCatalogues);
            //
            //   return resolve({listeCatalogues});
            // } catch (err) {
            //   return reject({err});
            // }
            resolve();

          } else {

            const nomFichier = files[idx];
            const fichier = path.join(pathBackupArchives, nomFichier);
            // console.debug(`Fichier archive, creer hard link : ${fichier}`);

            // Verifier si c'est une archive annuelle, mensuelle ou quotidienne
            const dateFichier = nomFichier.split('_')[1];

            var pathStagingFichier = null;
            if(dateFichier) {
              if(dateFichier.length == 8) {
                // Fichier quotidien
                pathStagingFichier = path.join(pathAggregationStaging.quotidien, nomFichier);
              } else if(dateFichier.length == 6) {
                // Fichier mensuel
                pathStagingFichier = path.join(pathAggregationStaging.mensuel, nomFichier);
              } else if(dateFichier.length == 4) {
                // Fichier annuel
                pathStagingFichier = path.join(pathAggregationStaging.annuel, nomFichier);
              }
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
              console.warning(`Fichier non reconnu: ${fichier}`)
              createHardLink(idx+1);
            }
          }

        };
        createHardLink(0);

      });
    });

    if(errResultatHardLinksArchives) throw errResultatHardLinksArchives;

    return {};

  }

  // Extrait tous les fichiers .tar.xz d'un repertoire vers la destination
  // Ceci inclue les niveaux annuels, mensuels et quotidiens
  async extraireTarParNiveaux() {

    // Extraire variables du scope _this_ pour fonctions async
    const {listeAggregation, pathAggregationStaging} = this;

    // console.debug(`Path staging ${this.pathStaging}`);
    const catalogues = {};

    for(let idxRep in listeAggregation) {
      const niveauSource = listeAggregation[idxRep];
      const repertoireSource = pathAggregationStaging[niveauSource];

      if(niveauSource === 'horaire') break; // On ne fait pas le niveau horaire ici

      // Niveau et repertoire destination (prochain niveau d'aggregation)
      const niveauDestination = listeAggregation[parseInt(idxRep) + 1];
      const repertoireDestination = pathAggregationStaging[niveauDestination];
      console.debug(`Niveau ${niveauSource} extrait vers ${niveauDestination}`);
      console.debug(`Extraire et valider archives tar.xz sous ${repertoireSource} vers ${repertoireDestination}`)

      var resultatArchives = await this.extraireTarRepertoire(repertoireSource, repertoireDestination, niveauDestination);

      if(resultatArchives.err) {
        throw resultatArchives.err;
      }

      catalogues[resultatArchives.niveau] = resultatArchives.erreursTraitement;
    }

    return catalogues;
  }

  async extraireTarRepertoire(repertoireSource, repertoireDestination, niveauDestination) {

    // Extraire variables du scope _this_ pour fonctions async
    const {extraireCatalogueStaging, verifierContenuCatalogueStaging, verifierContenuCatalogueQuotidienStaging} = this;

    const validateurSignature = new ValidateurSignature();

    return new Promise((resolve, reject)=>{

      const erreursTraitement = [];
      const cataloguesHorairesParJour = {}; // Conserve les catalogues horaires rencontres

      fs.readdir(repertoireSource, (err, files)=>{
        if(err) return reject({err});

        async function __extraireTar(idx) {
          if(idx === files.length) {
            // return resolve({niveau});
            console.debug("Extraction TAR terminee, erreurs:");
            console.debug(erreursTraitement);
            return resolve({erreursTraitement});
          }

          const fichierArchiveSource = path.join(repertoireSource, files[idx]);
          var pathDestination = repertoireDestination;

          if(fichierArchiveSource.endsWith('.tar.xz')) {
            // console.debug("Nom Fichier : " + fichierArchiveSource);
            if(niveauDestination === 'horaire') {
              // Path horaire, il faut ajouter AAAA/MM/JJ au path

              var dateFichier = fichierArchiveSource.split('_')[1];
              var annee = dateFichier.slice(0, 4);
              var mois = dateFichier.slice(4, 6);
              var jour = dateFichier.slice(6, 8);

              pathDestination = path.join(repertoireDestination, annee, mois, jour);

              await new Promise((resolve, reject)=>{
                fs.mkdir(pathDestination, {recursive: true, mode: 0o770}, err=>{
                  if(err) reject(err);
                  else resolve();
                })
              });

            }

            // console.debug(`Extraire fichier ${fichier} vers ${pathDestination}`)
            await utilitaireFichiers.extraireTarFile(fichierArchiveSource, pathDestination);

            // Verifier le catalogue de l'archive et nettoyer
            const catalogue = await extraireCatalogueStaging(fichierArchiveSource, pathDestination);

            // Verifier la signature du catalogue
            const fingerprintFeuille = catalogue['en-tete'].certificat;

            var certificatsValides = false;
            if(!validateurSignature.isChainValid()) {
              // Charger la chaine de certificats
              const chaineFingerprints = catalogue.certificats_chaine_catalogue;
              const chaineCertificatsNonVerifies = chaineFingerprints.reduce((liste, fingerprint)=>{
                // Charger le certificate avec PKI, cert store
                liste.push(catalogue.certificats_pem[fingerprint]);
                return liste;
              }, [])

              // Ajouter cert CA de cette chaine
              validateurSignature.ajouterCertificatCA(chaineCertificatsNonVerifies[chaineCertificatsNonVerifies.length-1]);
              if(validateurSignature.verifierChaine(chaineCertificatsNonVerifies)) {
                certificatsValides = true;
              }
            }

            // Verifier chaine et signature du catalogue
            if(certificatsValides) {
              try {
                var certValide = await validateurSignature.verifierSignature(catalogue);
                if(!certValide) {
                  erreursTraitement.push({
                    nomFichier: fichierArchiveSource,
                    message: "Signature du catalogue invalide"
                  })
                }
                // console.debug(`Catalogue valide (valide=${certValide}) : ${fichierArchiveSource}`);
              } catch (err) {
                console.error(`Cataloge archive invalide : ${fichierArchiveSource}`);
                console.error(err);
                erreursTraitement.push({
                  nomFichier: fichierArchiveSource,
                  message: "Erreur de verification du catalogue",
                  err,
                })
              }
            } else {
              erreursTraitement.push({
                nomFichier: fichierArchiveSource,
                message: "Certificats du catalogue invalide, signature non verifiee"
              })
            }

            let erreurs;
            if(niveauDestination === 'horaire') {
              // Catalogue quotidien, la destination est horaire et utilise un
              // format de repertoire different (avec la date)
              erreurs = await verifierContenuCatalogueQuotidienStaging(catalogue, pathDestination);
            } else {
              erreurs = await verifierContenuCatalogueStaging(catalogue, pathDestination);
            }
            if(erreurs.erreursArchives) {
              erreursTraitement.push(...erreurs.erreursArchives);  // Concatener erreurs
            }

            // Supprimer l'archive originale
            await utilitaireFichiers.supprimerFichiers([files[idx]], repertoireSource);
          }
          __extraireTar(idx+1);
        }
        __extraireTar(0);
      });
    })
  }

  // Verifie la signature d'un catalogue est le hachage des fichiers
  async extraireCatalogueStaging(nomArchive, pathDestination) {
    // const pathStaging = this.pathStaging;
    var nomCatalogue = path.basename(nomArchive);
    nomCatalogue = nomCatalogue.replace('.tar.xz', '.json.xz').split('_');
    nomCatalogue[1] = 'catalogue_' + nomCatalogue[1];
    nomCatalogue = nomCatalogue.join('_');

    var pathArchive = path.dirname(nomArchive);
    const pathCatalogue = path.join(pathDestination, nomCatalogue);

    // console.debug(`Path catalogue ${pathCatalogue}`);

    // Charger le JSON du catalogue en memoire
    var input = fs.createReadStream(pathCatalogue);
    const promiseChargement = new Promise((resolve, reject)=>{
      var decompressor = lzma.createDecompressor();
      var contenu = '';
      input.pipe(decompressor);
      decompressor.on('data', chunk=>{
        contenu = contenu + chunk;
      });
      decompressor.on('end', ()=>{
        var catalogue = JSON.parse(contenu);
        resolve(catalogue);
      });
      decompressor.on('error', err=>{
        reject(err);
      })
    });
    input.read();

    const catalogue = await promiseChargement;

    return catalogue;
  }

  // Verifier la chaine
  async verifierContenuCatalogueStaging(catalogue, pathFichiersStaging) {
    const dictFichiers = catalogue.fichiers_quotidien || catalogue.fichiers_mensuels;

    const erreursArchives = [];
    const dictErreurs = {erreursArchives};

    for(let sousRep in dictFichiers) {
      const infoFichier = dictFichiers[sousRep];
      const nomFichier = infoFichier.archive_nomfichier;
      const sha3_512 =  infoFichier.archive_sha3_512;

      // Ajouter 0 au sousRep pour le path du repertoire
      const pathFichier = path.join(pathFichiersStaging, nomFichier);
      // console.debug(`Verifier SHA3_512 fichier ${pathFichier}`);

      try {
        const shaCalcule = await utilitaireFichiers.calculerSHAFichier(pathFichier, {fonctionHash: 'sha3-512'});
        if(sha3_512 !== shaCalcule) {
          erreursArchives.push({nomFichier, message: "Hachage invalide"});
        }
      } catch(err) {
        console.warn(`Erreur verification SHA3_512 fichier ${pathFichier}`);
        console.warn(err);
        erreursArchives.push({nomFichier, err, message: 'Erreur de verification hachage du fichier'});
      }
    }

    return dictErreurs;
  }

  async verifierContenuCatalogueQuotidienStaging(catalogue, pathFichiersStaging) {
    const dictFichiers = catalogue.fichiers_horaire;

    const erreursArchives = [];
    const dictErreurs = {erreursArchives};

    const fichiersAVerifier = [];

    // Faire la liste des catalogues et transactions
    for(let sousRep in dictFichiers) {
      const infoFichiers = dictFichiers[sousRep];

      // Ajouter 0 au sousRep pour le path du repertoire
      if(sousRep.length == 1) sousRep = '0' + sousRep;

      fichiersAVerifier.push({
        nomFichier: infoFichiers.catalogue_nomfichier,
        hachage: infoFichiers.catalogue_sha3_512,
        sousRepertoire: path.join(sousRep, 'catalogues')
      });
      fichiersAVerifier.push({
        nomFichier: infoFichiers.transactions_nomfichier,
        hachage: infoFichiers.transactions_sha3_512,
        sousRepertoire: path.join(sousRep, 'transactions')
      });

    }

    // Pour GrosFichiers, faire la liste de tous les fichiers
    for(let fuuid in catalogue.fuuid_grosfichiers) {
      const {extension, securite, sha256, heure} = catalogue.fuuid_grosfichiers[fuuid];

      if(sha256) {  // Note : le hachage est optionnel pour fichiers generes
        var nomFichier = fuuid;
        if(securite === '3.protege' || securite === '4.secure') {
          nomFichier = nomFichier + '.mgs1';
        } else {
          nomFichier = nomFichier + '.' + extension;
        }

        // Ajouter 0 au sousRep pour le path du repertoire
        if(heure.length == 1) heure = '0' + heure;

        fichiersAVerifier.push({
          fonctionHash: 'sha256',
          nomFichier,
          fuuid,
          hachage: sha256,
          sousRepertoire: path.join(heure, 'grosfichiers'),
        })
      }
    }

    // Verifier les fichiers, concatener les erreurs dans le rapport
    for(let idxFichier in fichiersAVerifier) {
      const {nomFichier, hachage, sousRepertoire, fuuid} = fichiersAVerifier[idxFichier];
      const fonctionHash = fichiersAVerifier[idxFichier].fonctionHash || 'sha3-512';

      const pathFichier = path.join(pathFichiersStaging, sousRepertoire, nomFichier);

      // console.debug(`Verifier hachage fichier ${pathFichier}, ${fuuid}`);

      try {
        const shaCalcule = await utilitaireFichiers.calculerSHAFichier(pathFichier, {fonctionHash});
        if(hachage !== shaCalcule) {
          console.warn(`Hachage ${pathFichier} est invalide`);
          const messageErreur = {nomFichier, message: "Hachage invalide"};
          if(fuuid) messageErreur.fuuid = fuuid;
          erreursArchives.push(messageErreur);
        }
      } catch(err) {
        console.warn(`Erreur verification hachage fichier horaire ${pathFichier}`);
        console.warn(err);
        const messageErreur = {nomFichier, err, message: "Erreur de verification du hachage du fichier"};
        if(fuuid) messageErreur.fuuid = fuuid;
        erreursArchives.push(messageErreur);
      }
    }

    return dictErreurs;
  }

  // Methode qui parcourt tous les repertoires horaires et invoque verifierBackupHoraire()
  async parcourirCataloguesHoraire(fonctionTraitement) {
    const pathStagingHoraire = this.pathAggregationStaging.horaire;

    // Faire une fonction qui permet de parcourir les repertoires horaire
    // et verifier les catalogues un a la fois.
    async function parcourirRecursif(obj, level, pathCourant) {

      const erreurs = [];

      if(level === 4) { // 4 niveaux de sous-repertoires
        // console.debug(pathCourant);

        const catalogues = await new Promise(async (resolve, reject)=>{
          const pathCataloguesHoraire = path.join(pathCourant, 'catalogues');
          fs.readdir(pathCataloguesHoraire, async (err, catalogues)=>{
            if(err) return reject(err);
            // console.debug("Catalogues horaire")
            // console.debug(catalogues);
            resolve(catalogues);
          });
        });

        for(let idxCatalogue in catalogues) {
          const fichierCatalogue = catalogues[idxCatalogue]

          // Ouvrir le catalogue
          // Charger le JSON du catalogue en memoire
          const pathCatalogue = path.join(pathCourant, 'catalogues', fichierCatalogue);
          var input = fs.createReadStream(pathCatalogue);
          const promiseChargement = new Promise((resolve, reject)=>{
            var decompressor = lzma.createDecompressor();
            var contenu = '';
            input.pipe(decompressor);
            decompressor.on('data', chunk=>{
              contenu = contenu + chunk;
            });
            decompressor.on('end', ()=>{
              var catalogue = JSON.parse(contenu);
              resolve(catalogue);
            });
            decompressor.on('error', err=>{
              reject(err);
            })
          });
          input.read();

          const catalogue = await promiseChargement;

          const erreursBackup = await fonctionTraitement(pathCourant, fichierCatalogue, catalogue);
          erreurs.push(...erreursBackup); // Concatener toutes les erreurs
        }

      } else {
        // Parcourir prochain niveau de repertoire
        var erreursCumulees = await new Promise((resolve, reject)=>{
          fs.readdir(pathCourant, async (err, valeurDateTriee)=>{
            if(err) return reject(err);
            valeurDateTriee.sort();

            // Filtrer les repertoires/fichiers, on garde juste l'annee (level 0) et les mois, jours, heur (levels>0)
            if(level === 0) valeurDateTriee = valeurDateTriee.filter(valeur=>valeur.length===4);
            else valeurDateTriee = valeurDateTriee.filter(valeur=>valeur.length===2);

            var erreursRecursif = [];
            for(let valeurDateIdx in valeurDateTriee) {
              const valeurDateStr = valeurDateTriee[valeurDateIdx];
              const erreurEtape = await parcourirRecursif(obj, level+1, path.join(pathCourant, valeurDateStr));
              erreursRecursif.push(...erreurEtape);
            }
            resolve(erreursRecursif);
          })
        });
        erreurs.push(...erreursCumulees);

      }

      return erreurs;

    };

    var erreurs = await parcourirRecursif(this, 0, pathStagingHoraire);
    return {erreurs};

  }

  // Verifier la signature des catalogues et les chaines de backup horaire.
  // Verifie aussi les fichiers de transaction et autres (e.g. grosfichiers)
  async verifierBackupHoraire(pathCourant, fichierCatalogue, catalogue) {
    // console.debug(pathCourant);
    const erreurs = [];

    // Verifier la signature du catalogue
    const fingerprintFeuille = catalogue['en-tete'].certificat;
    var certificatsValides = false;
    if(!this.validateurSignature.isChainValid()) {
      // Charger la chaine de certificats
      const chaineFingerprints = catalogue.certificats_chaine_catalogue;
      const chaineCertificatsNonVerifies = chaineFingerprints.reduce((liste, fingerprint)=>{
        // Charger le certificate avec PKI, cert store
        liste.push(catalogue.certificats_pem[fingerprint]);
        return liste;
      }, [])

      // Ajouter cert CA de cette chaine
      this.validateurSignature.ajouterCertificatCA(chaineCertificatsNonVerifies[chaineCertificatsNonVerifies.length-1]);
      if(this.validateurSignature.verifierChaine(chaineCertificatsNonVerifies)) {
        certificatsValides = true;
      }
    }

    if(certificatsValides) {
      try {
        var signatureValide = await this.validateurSignature.verifierSignature(catalogue);
        if(!signatureValide) {
          console.error("Erreur signature catalogue invalide : " + fichierCatalogue);
          erreurs.push({nomFichier: fichierCatalogue, message: "Erreur signature catalogue invalide"});
        }
      } catch(err) {
        console.error("Erreur verification signature catalogue " + fichierCatalogue);
        console.error(err);
        erreurs.push({nomFichier: fichierCatalogue, message: "Erreur verification signature catalogue"});
      }
    } else {
      console.error("Erreur verification catalogue, certificats invalides : " + fichierCatalogue);
      erreurs.push({nomFichier: fichierCatalogue, message: "Erreur verification catalogue, certificats invalides"});
    }

    const cleChaine = catalogue.domaine + '/' + catalogue.securite;
    var noeudPrecedent = this.chaineBackupHoraire[cleChaine];

    // Comparer valeur precedente, rapporter erreur
    if(noeudPrecedent && catalogue.backup_precedent) {
      if(noeudPrecedent['uuid-transaction'] !== catalogue.backup_precedent['uuid-transaction']) {
        console.error("Mismatch UUID durant chainage pour la comparaison du catalogue " + fichierCatalogue);
        erreurs.push({nomFichier: fichierCatalogue, message: "Mismatch UUID durant chainage pour la comparaison du catalogue"});
      }
      if(noeudPrecedent['hachage_entete'] !== catalogue.backup_precedent.hachage_entete) {
        console.error("Mismatch hachage durant chainage pour la comparaison du catalogue " + fichierCatalogue);
        // console.debug(noeudPrecedent.hachage_entete);
        // console.debug(catalogue.backup_precedent.hachage_entete);
        erreurs.push({nomFichier: fichierCatalogue, message: "Hachage entete chaine horaire invalide"});
      }
    } else if(noeudPrecedent || catalogue.backup_precedent) {
      // Mismatch, il manque un des deux elements de chainage pour
      // la comparaison. Rapporter l'erreur.
      console.error("Mismatch, il manque un des deux elements de chainage pour la comparaison du catalogue " + fichierCatalogue);
    }

    // Calculer SHA3_512 pour entete courante
    const hachageEnteteCourante = pki.hacherTransaction(catalogue['en-tete'], {hachage: 'sha3-512'});
    var noeudCourant = {
      'hachage_entete': hachageEnteteCourante,
      'uuid-transaction': catalogue['en-tete']['uuid-transaction'],
    }
    // console.debug("Hachage calcule");
    // console.debug(noeudCourant);

    // Verifier les fichiers identifies dans le catalogue
    const fichiersAVerifier = [{
      nomFichier: catalogue.transactions_nomfichier,
      hachage: catalogue.transactions_sha3_512,
      sousRepertoire: path.join(pathCourant, 'transactions'),
      fonctionHachage: 'sha3-512',
    }];
    for(let fuuid in catalogue.fuuid_grosfichiers) {
      const {extension, securite, sha256} = catalogue.fuuid_grosfichiers[fuuid];
      var nomFichier = fuuid;
      if(securite === '3.protege' || securite === '4.secure') {
        nomFichier = nomFichier + '.mgs1';
      } else {
        nomFichier = nomFichier + '.' + extension;
      }
      fichiersAVerifier.push({
        nomFichier,
        hachage: sha256,
        fuuid,
        sousRepertoire: path.join(pathCourant, 'grosfichiers'),
        fonctionHachage: 'SHA256',
      });
    }

    // Verifier les fichiers, concatener les erreurs dans le rapport
    for(let idxFichier in fichiersAVerifier) {
      const {nomFichier, hachage, fonctionHachage, sousRepertoire, fuuid} = fichiersAVerifier[idxFichier];
      const fonctionHash = fonctionHachage || 'sha3-512';

      const pathFichier = path.join(sousRepertoire, nomFichier);

      // console.debug(`Verifier hachage fichier ${pathFichier}`);

      try {
        const {heure, domaine, securite} = catalogue;
        const shaCalcule = await utilitaireFichiers.calculerSHAFichier(pathFichier, {fonctionHash});
        if(hachage !== shaCalcule) {
          console.warn(`Hachage ${pathFichier} est invalide`);
          const messageErreur = {nomFichier, fichierCatalogue, heure, domaine, securite, errcode: 'digest.invalid', message: "Hachage invalide"};
          if(fuuid) messageErreur.fuuid = fuuid;
          erreurs.push(messageErreur);
        }
      } catch(err) {
        console.warn(`Erreur verification hachage fichier horaire ${pathFichier}`);
        console.warn(err);
        const messageErreur = {nomFichier, err, fichierCatalogue, heure, domaine, securite, errcode: 'digest.error', message: "Erreur de verification du hachage du fichier"};
        if(fuuid) messageErreur.fuuid = fuuid;
        erreurs.push(messageErreur);
      }
    }

    this.chaineBackupHoraire[cleChaine] = noeudCourant;

    return erreurs;
  }

  // Soumet les transactions en parcourant les backup horaires en ordre
  async resoumettreTransactions(pathCourant, fichierCatalogue, catalogue) {
    console.debug(`Resoumettre transactions ${fichierCatalogue}`);
    const erreurs = [];

    const pathTransactions = path.join(pathCourant, 'transactions', catalogue.transactions_nomfichier)

    // Charger le JSON du catalogue en memoire
    var input = fs.createReadStream(pathTransactions);
    var decompressor = lzma.createDecompressor();
    input.pipe(decompressor);

    const rl = readline.createInterface({
      input: decompressor,
      crlfDelay: Infinity
    });

    for await (const transaction of rl) {
      console.debug("C: " + transaction);
      await rabbitMQ.restaurerTransaction(transaction);
    }

    // Retransmettre le catalogue horaire lui-meme
    await rabbitMQ.restaurerTransaction(JSON.stringify(catalogue));

    return erreurs;
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

    // Tenter de faire un move via rename
    fs.rename(fichierCatalogue.path, nouveauPath, err=>{
      if(err) {
        if(err.code === 'EXDEV') {
          // Rename non supporte, faire un copy et supprimer le fichier
          fs.copyFile(fichierCatalogue.path, nouveauPath, errCopy=>{
            // Supprimer ancien fichier
            fs.unlink(fichierCatalogue.path, errUnlink=>{
              if(errUnlink) {
                console.error("Erreur suppression calalogue uploade " + fichierCatalogue.path);
              }
            });

            if(errCopy) return reject(errCopy);

            resolve();
          })
        }

        // Erreur irrecuperable
        return reject(err);
      }
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
        if(err) {
          if(err.code === 'EXDEV') {
            // Rename non supporte, faire un copy et supprimer le fichier
            fs.copyFile(fichierCatalogue.path, nouveauPath, errCopy=>{
              // Supprimer ancien fichier
              fs.unlink(fichierCatalogue.path, errUnlink=>{
                if(errUnlink) {
                  console.error("Erreur suppression calalogue uploade " + fichierCatalogue.path);
                }
              });

              if(errCopy) throw errCopy;

              _fctDeplacerFichier(pos+1); // Loop
            })
          } else {
            throw err;
          }
        } else {
          _fctDeplacerFichier(pos+1); // Loop
        }
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

// Instances

const traitementFichier = new TraitementFichier();
const utilitaireFichiers = new UtilitaireFichiers();

module.exports = {traitementFichier, pathConsignation, utilitaireFichiers, RestaurateurBackup};
