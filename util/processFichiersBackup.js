const fs = require('fs')
const path = require('path')
const crypto = require('crypto');
const { calculerHachageFichier } = require('./utilitairesHachage')

async function traiterFichiersBackup(fichiersTransactions, fichierCatalogue, pathRepertoire) {
  const pathTransactions = path.join(pathRepertoire, 'transactions');
  const pathCatalogues = path.join(pathRepertoire, 'catalogues');

  await new Promise((resolve, reject)=>{

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

  await new Promise((resolve, reject) => {

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

  })

  // Lancer appel recursif pour deplacer et calculer hachage des fichiers
  const resultatHachage = await _fctDeplacerFichier(pathTransactions, fichiersTransactions)

  return resultatHachage
}

async function _fctDeplacerFichier(pathTransactions, fichiersTransactions, pos) {
  // Fonction recursive qui deplace les fichiers
  if(!pos) pos = 0

  if(pos === fichiersTransactions.length) {
    // Fin de recursion
    return {}
  }

  const fichierDict = fichiersTransactions[pos]
  const nomFichier = fichierDict.originalname
  const nouveauPath = path.join(pathTransactions, nomFichier)

  const hachage = await calculerHachageFichier(fichierDict.path)

  // console.debug("Sauvegarde " + nouveauPath);
  await new Promise((resolve, reject)=>{
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
                return reject(errUnlink)
              }
            });

            if(errCopy) {
              console.error("Erreur copie fichier " + fichierDict.path);
              return reject(errCopy)
            }

            // _fctDeplacerFichier(pos+1); // Loop
            return resolve()
          })
        } else {
          // throw err;
          return reject(err)
        }
      } else {
        // _fctDeplacerFichier(pos+1); // Loop
        return resolve()
      }
    });
  })

  // Appel recursif
  const hachageDict = await _fctDeplacerFichier(pathTransactions, fichiersTransactions, pos+1) // Loop

  // Combiner dict produits recursivement
  return {[nomFichier]: hachage, ...hachageDict}

};

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
