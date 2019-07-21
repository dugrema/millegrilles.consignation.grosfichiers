const fs = require('fs');
const path = require('path');

const pathconsignation = '/tmp/consignation_local';

class TraitementFichier {

  traiterPut(headers, fichier) {
    const promise = new Promise((resolve, reject) => {

      try {
        // Le nom du fichier au complet, incluant path, est fourni dans fuuide.
        let nouveauPathFichier = path.join(pathconsignation, headers.fuuide);

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

}

const traitementFichier = new TraitementFichier();
module.exports = traitementFichier;
