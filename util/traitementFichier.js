const fs = require('fs');
const path = require('path');
const {uuidToDate} = require('./UUIDUtils');

class PathConsignation {

  constructor() {
    this.nomMillegrille = process.env.MG_NOM_MILLEGRILLE || 'sansnom';
    this.consignationPath = process.env.MG_CONSIGNATION_PATH ||
      path.join('/opt/millegrilles', this.nomMillegrille, 'consignation');

    // Path utilisable localement
    this.consignationPathLocal = path.join(this.consignationPath, '/local');
    this.consignationPathTiers = path.join(this.consignationPath, '/tiers');
  }

  trouverPathLocal(fichierUuid) {
    let pathFichier = this.formatterPath(fichierUuid, false);
    return path.join(this.consignationPathLocal, pathFichier);
  }

  formatterPath(fichierUuid, encrypte) {
    // Extrait la date du fileUuid, formatte le path en fonction de cette date.
    let timestamp = uuidToDate.extract(fichierUuid.replace('/', ''));
    // console.debug("uuid: " + fichierUuid + ". Timestamp " + timestamp);

    let extension = encrypte?'mgs1':'dat';

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
        let fileUuid = headers.fileuuid;
        let encrypte = headers.encrypte === "true";
        let fuuide = pathConsignation.formatterPath(fileUuid, encrypte);
        let nouveauPathFichier = path.join(pathConsignation.consignationPathLocal, fuuide);

        // Creer le repertoire au besoin, puis deplacer le fichier (rename)
        let pathRepertoire = path.dirname(nouveauPathFichier);
        console.debug("Path a utiliser: " + pathRepertoire);
        fs.mkdir(pathRepertoire, { recursive: true }, (err)=>{
          // console.debug("Path cree: " + pathRepertoire);
          // console.debug(err);

          if(!err) {
            // console.debug("Ecriture fichier " + nouveauPathFichier);
            let writeStream = fs.createWriteStream(nouveauPathFichier, {flag: 'wx'});
            writeStream.on('finish', ()=>{
              // console.log("Fichier ecrit: " + nouveauPathFichier);
              resolve();
            })
            .on('error', err=>{
              console.error("Erreur sauvegarde fichier: " + nouveauPathFichier);
              reject(err);
            })

            req.pipe(writeStream); // Traitement via event callbacks

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

const traitementFichier = new TraitementFichier();

module.exports = {traitementFichier, pathConsignation};
