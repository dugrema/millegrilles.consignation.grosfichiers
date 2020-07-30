const fs = require('fs');
const readdirp = require('readdirp');
const path = require('path');
const uuidv1 = require('uuid/v1');
const crypto = require('crypto');
const lzma = require('lzma-native');
const { spawn } = require('child_process');
const readline = require('readline');

const {uuidToDate} = require('./UUIDUtils');
const transformationImages = require('./transformationImages');
const {pki, ValidateurSignature} = require('./pki');

const MAP_MIMETYPE_EXTENSION = require('./mimetype_ext.json');
const MAP_EXTENSION_MIMETYPE = require('./ext_mimetype.json');

class PathConsignation {

  constructor(opts) {
    if(!opts) opts = {};

    const idmg = opts.idmg || process.env.MG_IDMG;

    // Methode de selection du path
    // Les overrides sont via env MG_CONSIGNATION_PATH ou parametre direct opts.consignationPath
    // Si IDMG fournit, formatte path avec /var/opt/millegrilles/IDMG
    // Sinon utilise un path generique sans IDMG
    var consignationPath = process.env.MG_CONSIGNATION_PATH || opts.consignationPath;
    if(!consignationPath) {
      if(idmg) {
        consignationPath = path.join('/var/opt/millegrilles', idmg, '/mounts/consignation');
      } else {
        consignationPath = '/var/opt/millegrilles/hebergement/consignation';
      }
    }
    console.info("Path fichiers : %s", consignationPath)

    // var consignationPath = opts.consignationPath || '/var/opt/millegrilles/mounts/consignation';

    this.consignationPath = consignationPath;

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
    // console.debug("Aller chercher fichiers du repertoire " + pathRepertoire);

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
          // console.debug("Fichier trouve : " + fichierPath);
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

// const pathConsignation = new PathConsignation();

class TraitementFichier {

  constructor(rabbitMQ) {
    this.rabbitMQ = rabbitMQ;
    const idmg = rabbitMQ.pki.idmg;
    this.pathConsignation = new PathConsignation({idmg});
    this.utilitaireFichiers = new UtilitaireFichiers();
  }

  traiterPut(req) {
    // Sauvegarde le fichier dans le repertoire de consignation local.

    const pathConsignation = new PathConsignation({idmg: req.autorisationMillegrille.idmg})

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
                    var imagePreviewInfo = await traiterImage(pathConsignation, nouveauPathFichier);
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
                  var imagePreviewInfo = await traiterVideo(pathConsignation, nouveauPathFichier);
                  messageConfirmation.thumbnail = imagePreviewInfo.thumbnail;
                  messageConfirmation.fuuid_preview = imagePreviewInfo.fuuidPreviewImage;
                  messageConfirmation.mimetype_preview = imagePreviewInfo.mimetypePreviewImage;
                  console.debug("Fuuid preview video: " + messageConfirmation.fuuid_preview)
                }
              }

              this.rabbitMQ.transmettreTransactionFormattee(
                messageConfirmation,
                'millegrilles.domaines.GrosFichiers.nouvelleVersion.transfertComplete')
              .then( msg => {
                // console.log("Recu confirmation de nouvelleVersion transfertComplete");
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

// Extraction de thumbnail et preview pour images
async function traiterImage(pathConsignation, pathImage) {
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
async function traiterVideo(pathConsignation, pathVideo, sansTranscodage) {
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

module.exports = {TraitementFichier, PathConsignation, UtilitaireFichiers};
