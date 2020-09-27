const debug = require('debug')('millegrilles:util:backup')
const fs = require('fs')
const readdirp = require('readdirp')
const path = require('path')
const uuidv1 = require('uuid/v1')
const crypto = require('crypto')
const lzma = require('lzma-native')
const { spawn } = require('child_process')
const readline = require('readline')
const tar = require('tar')
const moment = require('moment')
const tmp = require('tmp-promise')

const {uuidToDate} = require('./UUIDUtils')
const transformationImages = require('./transformationImages')
const {pki, ValidateurSignature} = require('./pki')
const { calculerHachageFichier } = require('./utilitairesHachage')
const {PathConsignation, extraireTarFile, supprimerFichiers} = require('./traitementFichier')
const {traiterFichiersBackup, traiterGrosfichiers} = require('./processFichiersBackup')

const MAP_MIMETYPE_EXTENSION = require('./mimetype_ext.json')
const MAP_EXTENSION_MIMETYPE = require('./ext_mimetype.json')

class TraitementFichierBackup {

  constructor(rabbitMQ) {
    this.rabbitMQ = rabbitMQ
    const idmg = rabbitMQ.pki.idmg
    this.pathConsignation = new PathConsignation({idmg})
  }

  // PUT pour un fichier de backup
  async traiterPutBackup(req) {

    debug("Body PUT traiterPutBackup : %O", req.body)

    const pathConsignation = new PathConsignation({idmg: req.autorisationMillegrille.idmg})

    const timestampBackup = new Date(req.body.timestamp_backup * 1000);
    if(!timestampBackup) {return reject("Il manque le timestamp du backup");}

    let pathRepertoire = pathConsignation.trouverPathBackupHoraire(timestampBackup)
    let fichiersTransactions = req.files.transactions;
    let fichierCatalogue = req.files.catalogue[0];

    // Deplacer les fichiers de backup vers le bon repertoire /backup
    const fichiersDomaines = await traiterFichiersBackup(fichiersTransactions, fichierCatalogue, pathRepertoire)

    // Transmettre cles du fichier de transactions
    if(req.body.transaction_maitredescles) {
      const transactionMaitreDesCles = JSON.parse(req.body.transaction_maitredescles)
      debug("Transmettre cles du fichier de transactions : %O", transactionMaitreDesCles)
      this.rabbitMQ.transmettreEnveloppeTransaction(transactionMaitreDesCles)
    }

    // Creer les hard links pour les grosfichiers
    const fuuidDict = JSON.parse(req.body.fuuid_grosfichiers)
    if(fuuidDict && Object.keys(fuuidDict).length > 0) {
      await traiterGrosfichiers(pathConsignation, pathRepertoire, fuuidDict)
    }

    return {fichiersDomaines}
  }

  async genererListeBackupsHoraire(req) {

    const pathConsignation = new PathConsignation({idmg: req.autorisationMillegrille.idmg})

    const domaine = req.body.domaine;
    const pathRepertoire = path.join(pathConsignation.consignationPathBackup, 'horaire');
    // debug("Path repertoire backup");
    // debug(pathRepertoire);

    const prefixeCatalogue = domaine + "_catalogue";
    const prefixeTransactions = domaine + "_transactions";

    var settings = {
      type: 'files',
      fileFilter: [
        prefixeCatalogue + '_*.json.xz',
        prefixeTransactions + '_*.jsonl.xz',
        prefixeTransactions + '_*.jsonl.xz.mgs1',
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
        // debug(entry);

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
        // debug("Fini");
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

    const fullPathFichier = path.join(this.pathConsignation.consignationPathBackup, aggregation, pathFichier);

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
    var repertoireBackup = this.pathConsignation.trouverPathBackupHoraire(dateJournal);
    // Remonter du niveau heure a jour
    repertoireBackup = path.dirname(repertoireBackup);

    const dateFormattee = formatterDateString(dateJournal).slice(0, 8)  // Retirer heures

    const nomFichier = domaine + "_catalogue_" + dateFormattee + "_" + securite + ".json.xz";

    const fullPathFichier = path.join(repertoireBackup, nomFichier);

    // debug("Path fichier journal quotidien " + fullPathFichier);
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

    // debug("Fichier cree : " + fullPathFichier);
    return {path: fullPathFichier, nomFichier, dateFormattee};
  }

  async sauvegarderJournalAnnuel(journal) {
    const {domaine, securite, annee} = journal

    const dateJournal = new Date(annee*1000)
    var repertoireBackup = this.pathConsignation.consignationPathBackupArchives

    let year = dateJournal.getUTCFullYear();
    const dateFormattee = "" + year;

    const nomFichier = domaine + "_catalogue_" + dateFormattee + "_" + securite + ".json.xz";

    const fullPathFichier = path.join(repertoireBackup, 'quotidiennes', domaine, nomFichier);

    // debug("Path fichier journal mensuel " + fullPathFichier);
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

    const sha512Journal = await calculerHachageFichier(fullPathFichier);

    // debug("Fichier cree : " + fullPathFichier);
    return {pathJournal: fullPathFichier, hachage: sha512Journal, dateFormattee};
  }

  async getStatFichierBackup(pathFichier, aggregation) {

    const fullPathFichier = path.join(this.pathConsignation.consignationPathBackup, aggregation, pathFichier);

    const {err, size} = await new Promise((resolve, reject)=>{
      fs.stat(fullPathFichier, (err, stat)=>{
        if(err) reject({err});
        resolve({size: stat.size})
      })
    });

    if(err) throw(err);

    return {size, fullPathFichier};
  }

  async getFichierTarBackupComplet(req, res) {

    const tmpFichierBackup = await tmp.file({mode:0o600, prefix: 'backup-download-', postfix: '.tar'})
    const fichierBackup = tmpFichierBackup.path
    debug("Fichier TMP backup\n%s", fichierBackup)

    const dateFormattee = moment().format('YYYY-MM-DD_hhmm')
    const idmg = req.autorisationMillegrille.idmg
    const downloadFileName = 'backup_' + idmg + '_' + dateFormattee + '.tar'

    const pathBackup = this.pathConsignation.consignationPathBackup

    // Creer une archive .tar de backup avec les repertoires de fichiers
    // horaire, archive et instantanne
    debug("Creer archive utilisant path %s", pathBackup)

    // Trouver les repertoires existants pour ajouter au tar
    const repertoires = ['horaire', 'archives', 'instantanne']

    try {

      // Faire la liste des sous-repertoires a filtrer
      const files = await new Promise((resolve, reject)=>{
        fs.readdir(pathBackup, (err, files)=>{
          if(err) reject(err)
          files.filter(item=>{
            return repertoires.includes(item)
          })
          resolve(files)
        })
      })

      await tar.create( // or tar.create
        {
          cwd: pathBackup,
          file: fichierBackup,
        },
        files
      )

      // Creer outputstream de reponse
      res.set('Content-Type', 'application/tar')
      res.set('Content-Disposition', 'attachment; filename="' + downloadFileName + '"')
      res.status(200)

      const readStream = fs.createReadStream(fichierBackup)

      await new Promise((resolve, reject)=>{
        readStream.on('close', _=>{
          resolve()
        })
        readStream.on('error', err=>{
          console.error("Erreur transfert\n%O", err)
          reject(err)
        })
        readStream.pipe(res)
      })

    } catch(err) {
      console.error("Erreur traitement TAR file\n%O", err)
      res.sendStatus(500)
    } finally {
      fs.unlink(fichierBackup, err=>{
        if(err) console.error("Erreur unlink " + fichierBackup)
      })
      res.end('')  // S'assurer que le stream est ferme
    }

  }

}

// Classe qui s'occupe du staging d'archives et fichiers de backup
// Prepare et valide le contenu du repertoire staging/
class RestaurateurBackup {

  constructor(mq, pki, opts) {
    if(!opts) opts = {}
    this.mq = mq
    this.pki = pki

    const idmg = pki.idmg;
    this.pathConsignation = new PathConsignation({idmg});

    // Configuration optionnelle
    this.pathBackupHoraire = opts.backupHoraire || this.pathConsignation.consignationPathBackupHoraire;
    this.pathBackupArchives = opts.archives || this.pathConsignation.consignationPathBackupArchives;
    this.pathStaging = opts.staging || this.pathConsignation.consignationPathBackupStaging;

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
    debug("Debut restauration complete")

    // S'assurer que les repertoires destination de base existent
    await this.creerRepertoires()

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
    debug("Fin restauration complete");
    return {rapports};
  }

  // S'assurer que les repertoires de base (horaire, archives) existent
  async creerRepertoires() {
    return new Promise((resolve, reject) => {
      fs.mkdir(this.pathBackupHoraire, err=>{
        if(err && err.code !== 'EEXIST') reject(err)
        fs.mkdir(this.pathBackupArchives, err=>{
          if(err && err.code !== 'EEXIST') reject(err)
          resolve()
        })
      })
    })
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
            // debug(`Creation hard links terminee, lectures archives sous ${pathAggregationStaging.horaire}`);

            // Faire la liste des fichiers extraits - sera utilisee pour creer
            // l'ordre de traitement des fichiers pour importer les transactions
            // try {
            //   const listeCatalogues = await genererListeCatalogues(pathAggregationStaging.horaire);
            //   debug("Liste catalogues horaire");
            //   debug(listeCatalogues);
            //
            //   return resolve({listeCatalogues});
            // } catch (err) {
            //   return reject({err});
            // }
            resolve();

          } else {

            const nomFichier = files[idx];
            const fichier = path.join(pathBackupArchives, nomFichier);
            // debug(`Fichier archive, creer hard link : ${fichier}`);

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
              // debug(`Creer link ${pathStagingFichier}`);
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

    // debug(`Path staging ${this.pathStaging}`);
    const catalogues = {};

    for(let idxRep in listeAggregation) {
      const niveauSource = listeAggregation[idxRep];
      const repertoireSource = pathAggregationStaging[niveauSource];

      if(niveauSource === 'horaire') break; // On ne fait pas le niveau horaire ici

      // Niveau et repertoire destination (prochain niveau d'aggregation)
      const niveauDestination = listeAggregation[parseInt(idxRep) + 1];
      const repertoireDestination = pathAggregationStaging[niveauDestination];
      debug(`Niveau ${niveauSource} extrait vers ${niveauDestination}`);
      debug(`Extraire et valider archives tar.xz sous ${repertoireSource} vers ${repertoireDestination}`)

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
            debug("Extraction TAR terminee, erreurs:");
            debug(erreursTraitement);
            return resolve({erreursTraitement});
          }

          const fichierArchiveSource = path.join(repertoireSource, files[idx]);
          var pathDestination = repertoireDestination;

          if(fichierArchiveSource.endsWith('.tar.xz')) {
            // debug("Nom Fichier : " + fichierArchiveSource);
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

            // debug(`Extraire fichier ${fichier} vers ${pathDestination}`)
            await extraireTarFile(fichierArchiveSource, pathDestination);

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
                // debug(`Catalogue valide (valide=${certValide}) : ${fichierArchiveSource}`);
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
            await supprimerFichiers([files[idx]], repertoireSource);
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

    // debug(`Path catalogue ${pathCatalogue}`);

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
      // debug(`Verifier SHA3_512 fichier ${pathFichier}`);

      try {
        const shaCalcule = await calculerHachageFichier(pathFichier, {fonctionHash: 'sha3-512'});
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

      // debug(`Verifier hachage fichier ${pathFichier}, ${fuuid}`);

      try {
        const shaCalcule = await calculerHachageFichier(pathFichier, {fonctionHash});
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
        // debug(pathCourant);

        const catalogues = await new Promise(async (resolve, reject)=>{
          const pathCataloguesHoraire = path.join(pathCourant, 'catalogues');
          fs.readdir(pathCataloguesHoraire, async (err, catalogues)=>{
            if(err) return reject(err);
            // debug("Catalogues horaire")
            // debug(catalogues);
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
    // debug(pathCourant);
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
        // debug(noeudPrecedent.hachage_entete);
        // debug(catalogue.backup_precedent.hachage_entete);
        erreurs.push({nomFichier: fichierCatalogue, message: "Hachage entete chaine horaire invalide"});
      }
    } else if(noeudPrecedent || catalogue.backup_precedent) {
      // Mismatch, il manque un des deux elements de chainage pour
      // la comparaison. Rapporter l'erreur.
      console.error("Mismatch, il manque un des deux elements de chainage pour la comparaison du catalogue " + fichierCatalogue);
    }

    // Calculer SHA3_512 pour entete courante
    const hachageEnteteCourante = this.pki.hacherTransaction(catalogue['en-tete'], {hachage: 'sha3-512'});
    var noeudCourant = {
      'hachage_entete': hachageEnteteCourante,
      'uuid-transaction': catalogue['en-tete']['uuid-transaction'],
    }
    // debug("Hachage calcule");
    // debug(noeudCourant);

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

      // debug(`Verifier hachage fichier ${pathFichier}`);

      try {
        const {heure, domaine, securite} = catalogue;
        const shaCalcule = await calculerHachageFichier(pathFichier, {fonctionHash});
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
    debug(`Resoumettre transactions ${fichierCatalogue}`);
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
      debug("Resoumettre transaction\n%s" + transaction);
      await this.mq.restaurerTransaction(transaction);
    }

    // Retransmettre le catalogue horaire lui-meme
    await this.mq.restaurerTransaction(JSON.stringify(catalogue));

    return erreurs;
  }

}

function formatterDateString(date) {
  let year = date.getUTCFullYear();
  let month = date.getUTCMonth() + 1; if(month < 10) month = '0'+month;
  let day = date.getUTCDate(); if(day < 10) day = '0'+day;
  let hour = date.getUTCHours(); if(hour < 10) hour = '0'+hour;
  const dateFormattee = "" + year + month + day + hour;
  return dateFormattee
}

// Instances

module.exports = {TraitementFichierBackup, RestaurateurBackup, formatterDateString};
