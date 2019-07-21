const fs = require('fs');
const path = require('path');
const {uuidToDate} = require('./UUIDUtils');

const pathconsignation = '/tmp/consignation_local';

class TraitementFichier {

  traiterPut(req) {
    const promise = new Promise((resolve, reject) => {

      try {
        // Le nom du fichier au complet, incluant path, est fourni dans fuuide.
        let headers = req.headers;
        let fileUuid = headers.fileuuid;
        let encrypte = headers.encrypte === "true";
        let fuuide = this.formatterPath(fileUuid, encrypte);
        let nouveauPathFichier = path.join(pathconsignation, fuuide);

        // Creer le repertoire au besoin, puis deplacer le fichier (rename)
        let pathRepertoire = path.dirname(nouveauPathFichier);
        fs.mkdir(pathRepertoire, { recursive: true }, (err)=>{
          console.debug("Path cree: " + pathRepertoire);
          console.debug(err);

          if(!err) {
            console.debug("Ecriture fichier " + nouveauPathFichier);
            let writeStream = fs.createWriteStream(nouveauPathFichier, {flag: 'wx'});
            writeStream.on('finish', ()=>{
              console.log("Fichier ecrit: " + nouveauPathFichier);
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
