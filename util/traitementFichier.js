const fs = require('fs');
const path = require('path');
const uuidv1 = require('uuid/v1');
const {uuidToDate} = require('./UUIDUtils');
const crypto = require('crypto');
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

  trouverPathBackup(heureBackup) {
    let year = heureBackup.getUTCFullYear();
    let month = heureBackup.getUTCMonth() + 1; if(month < 10) month = '0'+month;
    let day = heureBackup.getUTCDate(); if(day < 10) day = '0'+day;
    let hour = heureBackup.getUTCHours(); if(hour < 10) hour = '0'+hour;

    let pathBackup =
      path.join(
        this.consignationPathBackup,
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

      let pathRepertoire = pathConsignation.trouverPathBackup(timestampBackup);
      let fichiersTransactions = req.files.transactions;
      let fichierCatalogue = req.files.catalogue[0];

      console.debug("Path a utiliser: " + pathRepertoire);

      // Deplacer les fichiers de backup vers le bon repertoire /backup
      console.debug("Deplacers fichiers recus");
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
        console.debug("Traitement grosfichiers");
        console.debug(fuuidDict);

        const pathBackupGrosFichiers = path.join(pathRepertoire, 'grosfichiers');
        await new Promise((resolve, reject)=>{
          fs.mkdir(pathBackupGrosFichiers, { recursive: true, mode: 0o770 }, (err)=>{
            if(err) return reject(err);
            resolve();
          });
        });

        for(const fuuid in fuuidDict) {
          const paramFichier = fuuidDict[fuuid];
          console.debug("Creer hard link pour fichier " + fuuid);

          const {err, fichier} = await pathConsignation.trouverPathFuuidExistant(fuuid);
          if(err) {
            console.error("Erreur extraction fichier " + fuuid + " pour backup");
            console.error(err);
          } else {
            if(fichier) {
              console.debug("Fichier " + fuuid + " trouve");
              console.debug(fichier);

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

      console.debug("PUT backup Termine");
      resolve({fichiersDomaines});

    })

  }

}

async function traiterFichiersBackup(fichiersTransactions, fichierCatalogue, pathRepertoire) {
  const pathTransactions = path.join(pathRepertoire, 'transactions');
  const pathCatalogues = path.join(pathRepertoire, 'catalogues');

  const {err, resultatHash} = await new Promise((resolve, reject)=>{

    // Creer tous les repertoires requis pour le backup
    fs.mkdir(pathTransactions, { recursive: true, mode: 0o770 }, (err)=>{
      if(err) return reject(err);
      console.debug("Path cree " + pathTransactions);

      fs.mkdir(pathCatalogues, { recursive: true, mode: 0o770 }, (err)=>{
        if(err) return reject(err);
        console.debug("Path cree " + pathCatalogues);
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
    console.debug("Debut copie fichiers backup transaction");

    const resultatHash = {};

    async function _fctDeplacerFichier(pos) {
      if(pos === fichiersTransactions.length) {
        return {resultatHash};
      }

      const fichierDict = fichiersTransactions[pos];
      const nomFichier = fichierDict.originalname;
      const nouveauPath = path.join(pathTransactions, nomFichier);

      // Calculer SHA512 sur fichier de backup
      const sha512 = crypto.createHash('sha512');
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

      console.debug("Sauvegarde " + nouveauPath);
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

const traitementFichier = new TraitementFichier();

module.exports = {traitementFichier, pathConsignation};
