const fs = require('fs')
const path = require('path')
const crypto = require('crypto');


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

            return resolve();
          })
        } else {
         // Erreur irrecuperable
          return reject(err);
        }
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

      // console.debug("Sauvegarde " + nouveauPath);
      fs.rename(fichierDict.path, nouveauPath, err=>{
        // console.debug("Copie fichier " + fichierCatalogue.path);
        if(err) {
          if(err.code === 'EXDEV') {
            // Rename non supporte, faire un copy et supprimer le fichier
            fs.copyFile(fichierDict.path, nouveauPath, errCopy=>{
              console.debug("Copie complete : " + nouveauPath);

              // Supprimer ancien fichier
              fs.unlink(fichierDict.path, errUnlink=>{
                if(errUnlink) {
                  console.error("Erreur suppression transactions uploade " + fichierDict.path);
                }
              });

              if(errCopy) {
                console.error("Erreur copie fichier " + fichierDict.path);
                throw errCopy;
              }

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

    // console.debug(`Debug deplacement fichiers ${pathRepertoire}`);

    await _fctDeplacerFichier(0);  // Lancer boucle

    // console.debug(`Fin deplacement fichiers ${pathRepertoire}`);

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

async function traiterGrosfichiers(pathConsignation, pathRepertoire, fuuidDict) {
  // debug("Traitement grosfichiers");
  // debug(fuuidDict);

  const pathBackupGrosFichiers = path.join(pathRepertoire, 'grosfichiers');
  const {erreurMkdir} = await new Promise((resolve, reject)=>{
    fs.mkdir(pathBackupGrosFichiers, { recursive: true, mode: 0o770 }, (erreurMkdir)=>{
      if(erreurMkdir) {
        console.error("Erreur mkdir grosfichiers : " + pathBackupGrosFichiers);
        return reject({erreurMkdir});
      }
      resolve({});
    });
  })
  .catch(err=>{
    return {erreurMkdir: err};
  });
  if(erreurMkdir) return reject(erreurMkdir);

  for(const fuuid in fuuidDict) {
    const paramFichier = fuuidDict[fuuid];
    // debug("Creer hard link pour fichier " + fuuid);

    const {err, fichier} = await pathConsignation.trouverPathFuuidExistant(fuuid);
    if(err) {
      console.error("Erreur extraction fichier " + fuuid + " pour backup");
      console.error(err);
    } else {
      if(fichier) {
        // debug("Fichier " + fuuid + " trouve");
        // debug(fichier);

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

module.exports = {traiterFichiersBackup, traiterGrosfichiers}
