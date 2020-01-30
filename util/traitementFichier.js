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
  }

  // Retourne le path du fichier
  // Type est un dict {mimetype, extension} ou une des deux valeurs doit etre fournie
  trouverPathLocal(fichierUuid, encrypte, type) {
    let pathFichier = this._formatterPath(fichierUuid, encrypte, type);
    return path.join(this.consignationPathLocal, pathFichier);
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

    let year = timestamp.getUTCFullYear();
    let month = timestamp.getUTCMonth() + 1; if(month < 10) month = '0'+month;
    let day = timestamp.getUTCDate(); if(day < 10) day = '0'+day;
    let hour = timestamp.getUTCHours(); if(hour < 10) hour = '0'+hour;
    let minute = timestamp.getUTCMinutes(); if(minute < 10) minute = '0'+minute;
    let fuuide =
      path.join(""+year, ""+month, ""+day, ""+hour, ""+minute,
        fichierUuid + '.' + extension);

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
              if(!encrypte && mimetype.split('/')[0] === 'image') {
                try {
                  console.debug("Creation preview image")
                  var imagePreviewInfo = await traiterImage(nouveauPathFichier);
                  messageConfirmation.thumbnail = imagePreviewInfo.thumbnail;
                  messageConfirmation.fuuid_preview = imagePreviewInfo.fuuidPreviewImage;
                  messageConfirmation.mimetype_preview = imagePreviewInfo.mimetypePreviewImage;
                  console.debug("Info image, preview = " + messageConfirmation.fuuid_preview)
                } catch (err) {
                  console.error("Erreur creation thumbnail/previews");
                  console.error(err);
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

              // console.log("Fichier ecrit: " + nouveauPathFichier);
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

// Extraction de thumbnail et preview pour images
async function traiterImage(pathImage) {
  var fuuidPreviewImage = uuidv1();
  var pathPreviewImage = pathConsignation.trouverPathLocal(fuuidPreviewImage, false, {extension: 'jpg'});
  var thumbnail = null;
  try {
    thumbnail = await transformationImages.genererThumbnail(pathImage);
    await transformationImages.genererPreview(pathImage, pathPreviewImage);
    console.debug("2. thumbnail/preview prets")
  } catch(err) {
    console.error("Erreur traitement image thumbnail/preview");
    console.error(err);
  }
  return {thumbnail, fuuidPreviewImage, mimetypePreviewImage: 'image/jpeg'};
}

// Extraction de thumbnail, preview et recodage des videos pour le web
async function traiterVideo(pathVideo) {
  var fuuidPreviewImage = uuidv1();

  // Extraire un preview pleine resolution du video, faire un thumbnail
  var pathPreviewImage = pathConsignation.trouverPathLocal(fuuidPreviewImage, false, {extension: 'jpg'});
  await transformationImages.genererPreviewVideoPromise(pathVideo, pathPreviewImage);
  var thumbnail = await transformationImages.genererThumbnail(pathPreviewImage);

  // Generer une nouvelle version downsamplee du video en mp4 a 480p, 3Mbit/s

  return {thumbnail, fuuidPreviewImage, mimetypePreviewImage: 'image/jpeg'};
}

const traitementFichier = new TraitementFichier();

module.exports = {traitementFichier, pathConsignation};
