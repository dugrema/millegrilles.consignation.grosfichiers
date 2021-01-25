const debug = require('debug')('millegrilles:util:processFichiersBackup')
const fs = require('fs')
const readdirp = require('readdirp')
const path = require('path')
const crypto = require('crypto');
const { calculerHachageFichier } = require('./utilitairesHachage')

async function traiterFichiersBackup(fichiersTransactions, fichierCatalogue, pathRepertoire) {

  // Meme repertoire pour toutes les transactions et catalogues horaire
  const pathTransactions = pathRepertoire
  const pathCatalogues = pathRepertoire

  await new Promise((resolve, reject)=>{

    // Creer tous les repertoires requis pour le backup
    fs.mkdir(pathTransactions, { recursive: true, mode: 0o770 }, (err)=>{
      if(err) return reject(err);

      fs.mkdir(pathCatalogues, { recursive: true, mode: 0o770 }, (err)=>{
        if(err) return reject(err);
        resolve();
      })

    });
  })

  // Deplacer le fichier de catalogue du backup
  const nomFichier = fichierCatalogue.originalname
  const nouveauPath = path.join(pathCatalogues, nomFichier)
  debug("Copier catalogue %s -> %s", fichierCatalogue.path, nouveauPath)
  await deplacerFichier(fichierCatalogue.path, nouveauPath)

  // Lancer appel recursif pour deplacer et calculer hachage des fichiers
  const resultatHachage = {}
  for(let i in fichiersTransactions) {
    const fichierTransaction = fichiersTransactions[i]
    //const hachage = await _fctDeplacerFichier(pathTransactions, fichierTransaction)

    const nouveauPath = path.join(pathTransactions, fichierTransaction.originalname)
    const hachage = await calculerHachageFichier(fichierTransaction.path)
    await deplacerFichier(fichierTransaction.path, nouveauPath)

    resultatHachage[fichierTransaction.originalname] = hachage
  }

  return resultatHachage
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

async function traiterFichiersApplication(
  amqpdao, transactionCatalogue, transactionMaitreDesCles, fichierApplication, pathBackupApplication) {
  debug("traiterFichiersApplication, fichier tmp : %s\npath destination : %s", fichierApplication, pathBackupApplication)

  const baseFolder = path.dirname(pathBackupApplication)

  const nomApplication = transactionCatalogue.application

  // Verifier hachage de l'archive de backup
  const hachageCalcule = await calculerHachageFichier(fichierApplication.path)
  const hachageRecu = transactionCatalogue.archive_hachage
  if(hachageCalcule !== hachageRecu) {
    console.error("Hachage recu: %s\nCalcule: %s", hachageRecu, hachageCalcule)
    throw new Error("Mismatch hachage archive")
  }

  await new Promise((resolve, reject)=>{
    fs.mkdir(pathBackupApplication, { recursive: true, mode: 0o770 }, (erreurMkdir)=>{
      if(erreurMkdir) {
        console.error("Erreur mkdir : " + pathBackupApplication)
        return reject({erreurMkdir})
      }
      debug("Repertoire archive application cree : %s", pathBackupApplication)
      resolve()
    })
  })

  // Transmettre la transaction de maitredescles
  debug("Transmettre cles du fichier de backup application : %O", transactionMaitreDesCles)
  await amqpdao.transmettreEnveloppeTransaction(transactionMaitreDesCles)

  // Sauvegarder fichiers application
  await sauvegarderFichiersApplication(transactionCatalogue, fichierApplication, pathBackupApplication)

  debug("Transmettre catalogue backup application : %O", transactionCatalogue)
  await amqpdao.transmettreEnveloppeTransaction(transactionCatalogue)

  await rotationArchiveApplication(transactionCatalogue, pathBackupApplication)
}

async function rotationArchiveApplication(transactionCatalogue, pathBackupApplication) {
  debug("Effectuer rotation des archives d'application")

  const settingsReaddirp = {
    type: 'files',
    fileFilter: [
       '*.json',
    ],
  }

  const listeCatalogues = await new Promise((resolve, reject)=>{
    const listeCatalogues = [];
    readdirp(pathBackupApplication, settingsReaddirp)
    .on('data', entry=>{ listeCatalogues.push(entry.fullPath) })
    .on('error', err=>{ reject(err) })
    .on('end', ()=>{

      const promisesCatalogue = listeCatalogues.map(async pathCatalogue => {
        debug("Charger catalogue : %O", pathCatalogue)
        return new Promise((resolve, reject) => {
          const catalogue = fs.readFile(pathCatalogue, (err, data)=>{
            if(err) return reject(err)
            return resolve(JSON.parse(data))
          })
        })
      })
      const catalogues = Promise.all(promisesCatalogue)

      resolve(catalogues)
    })
  })

  // Faire le tri des catalogues en ordre descendant - on garde les N plus recents
  listeCatalogues.sort((a,b)=>{
    return b['en-tete'].estampille - a['en-tete'].estampille
  })
  debug("Liste catalogues trouves : %O", listeCatalogues.map(item=>item.catalogue_nomfichier))

  // Supprimer les vieux fichiers
  for(let idx in listeCatalogues) {
    if(idx < 2) continue  // On garde 2 backups

    const catalogue = listeCatalogues[idx]
    const archivePath = catalogue.archive_nomfichier
    const cataloguePath = catalogue.catalogue_nomfichier

    await new Promise((resolve, reject)=>{
      debug("Supprimer archive %s", pathBackupApplication)
      fs.unlink(path.join(pathBackupApplication, archivePath), err=>{
        if(err) console.error("rotationArchiveApplication: Erreur suppression fichier %s", archivePath)
        debug("Supprimer catalogue %s", pathBackupApplication)
        fs.unlink(path.join(pathBackupApplication, cataloguePath), err=>{
          if(err) console.error("rotationArchiveApplication: Erreur suppression fichier %s", cataloguePath)
          resolve()
        })
      })
    })

  }

}

async function sauvegarderFichiersApplication(transactionCatalogue, fichierApplication, pathBackupApplication) {
  const nomArchive = transactionCatalogue.archive_nomfichier
  const pathArchive = path.join(pathBackupApplication, nomArchive)

  const nomCatalogue = transactionCatalogue.catalogue_nomfichier
  const pathCatalogue = path.join(pathBackupApplication, nomCatalogue)

  debug("Sauvegarder fichiers backup application\nArchive : %s\nCatalogue :%s", nomArchive, pathCatalogue)

  // Deplacer fichier archive
  await deplacerFichier(fichierApplication.path, pathArchive)

  // Sauvegarder catalogue transaction
  const catalogueJson = JSON.stringify(transactionCatalogue)
  const writeStream = fs.createWriteStream(pathCatalogue)
  writeStream.write(catalogueJson)

}

async function deplacerFichier(src, dst) {
  debug("Deplacer fichier de %s a %s", src, dst)
  return new Promise((resolve, reject) => {
    fs.rename(src, dst, err=>{
      if(err) {
        if(err.code === 'EXDEV') {
          // Rename non supporte, faire un copy et supprimer le fichier
          fs.copyFile(src, dst, errCopy=>{
            // Supprimer ancien fichier
            fs.unlink(src, errUnlink=>{
              if(errUnlink) {
                console.error("Erreur deplacement, src non supprimee " + src)
              }
            })
            if(errCopy) return reject(errCopy);
            return resolve()
          })
        } else {
         // Erreur irrecuperable
          return reject(err)
        }
      }
      resolve()
    })
  })
}

module.exports = {traiterFichiersBackup, traiterGrosfichiers, traiterFichiersApplication}
