const fs = require('fs');
const path = require('path');

const pathconsignation = '/tmp/consignation_local';

class TraitementFichier {

  traiterPut(headers, fichier) {
    const promise = new Promise((resolve, reject) => {

      try {
        // Le nom du fichier au complet, incluant path, est fourni dans fuuide.
        let fileUuid = headers.fileuuid;
        let encrypte = headers.encrypte === "true";
        let fuuide = this.formatterPath(fileUuid, encrypte);
        let nouveauPathFichier = path.join(pathconsignation, fuuide);

        if(!fs.existsSync(nouveauPathFichier)) {
          // Creer le repertoire au besoin, puis deplacer le fichier (rename)
          fs.mkdirSync(path.dirname(nouveauPathFichier), { recursive: true });
          fs.rename(fichier.path, nouveauPathFichier, (err)=>{
            if(!err) {
              resolve(nouveauPathFichier);
            } else {
              reject(err);
            }
          });
        } else {
          console.error('Fichier existe deja: ' + nouveauPathFichier);
          reject('Fichier existe deja: ' + nouveauPathFichier);
        }
      } catch (err) {
        console.error("Erreur traitement fichier " + headers.fuuide);
        reject(err);
      }

    });

    return promise;
  }

  formatterPath(fileUuid, encrypte) {
    // Extrait la date du fileUuid, formatte le path en fonction de cette date.
    let timestamp = uuidToDate.extract(fileUuid);
    // console.debug("Timestamp " + timestamp);

    let extension = encrypte?'mgs1':'dat';

    let year = timestamp.getUTCFullYear();
    let month = timestamp.getUTCMonth() + 1; if(month < 10) month = '0'+month;
    let day = timestamp.getUTCDate(); if(day < 10) day = '0'+day;
    let hour = timestamp.getUTCHours(); if(hour < 10) hour = '0'+hour;
    let minute = timestamp.getUTCMinutes(); if(minute < 10) day = '0'+minute;
    let fuuide =
      '/' + year + '/' + month + '/' + day + '/' +
      hour + '/' + minute + '/' +
      fileUuid + '.' + extension;

    return fuuide;
  }

}

const traitementFichier = new TraitementFichier();
module.exports = traitementFichier;
