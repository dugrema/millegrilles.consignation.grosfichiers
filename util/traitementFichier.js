const debug = require('debug')('millegrilles:fichiers:traitementFichier')
const fs = require('fs')
const fsPromises = require('fs/promises')
const readdirp = require('readdirp')
const path = require('path')
const crypto = require('crypto')
const { spawn } = require('child_process')
const tmp = require('tmp-promise')
const tar = require('tar')

class PathConsignation {

  constructor(opts) {
    if(!opts) opts = {};

    // Methode de selection du path
    // Les overrides sont via env MG_CONSIGNATION_PATH ou parametre direct opts.consignationPath
    // Si IDMG fournit, formatte path avec /var/opt/millegrilles/IDMG
    // Sinon utilise un path generique sans IDMG
    var consignationPath = process.env.MG_CONSIGNATION_PATH || opts.consignationPath
    if(!consignationPath) {
      consignationPath = '/var/opt/millegrilles/consignation';
    }

    this.consignationPathDownloadStaging = path.join(consignationPath, 'downloadStaging');
    this.consignationPathUploadStaging = path.join(consignationPath, 'uploadStaging');

    this.consignationPath = consignationPath;

    // Path utilisable localement
    this.consignationPathLocal = path.join(this.consignationPath, 'grosfichiers');
    this.consignationPathCorbeille = path.join(this.consignationPath, 'corbeille', 'grosfichiers');
  }

  // Retourne le path du fichier
  // Type est un dict {mimetype, extension} ou une des deux valeurs doit etre fournie
  trouverPathLocal(fichierUuid) {
    let pathFichier = this._formatterPath(fichierUuid)
    return path.join(this.consignationPathLocal, pathFichier);
  }

  trouverPathRelatif(fichierUuid, opts) {
    return this._formatterPath(fichierUuid, opts)
  }

  // Trouve un fichier existant lorsque l'extension n'est pas connue
  async trouverPathFuuidExistant(fichierUuid) {
    let pathFichier = this._formatterPath(fichierUuid, {});
    let pathRepertoire = path.join(this.consignationPathLocal, path.dirname(pathFichier));
    // console.debug("Aller chercher fichiers du repertoire " + pathRepertoire);

    const fichier = await new Promise((resolve, reject)=>{
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

    return fichier;
  }

  trouverPathBackupHoraire(domaine, partition) {
    const partitionDomaine = partition?domaine + '.' + partition:domaine
    return path.join(this.consignationPathBackup, 'domaines', partitionDomaine, 'horaire')
  }

  trouverPathBackupSnapshot(domaine, partition) {
    const partitionDomaine = partition?domaine + '.' + partition:domaine
    return path.join(this.consignationPathBackup, 'domaines', partitionDomaine, 'snapshot')
  }

  trouverPathBackupDomaine(domaine, partition) {
    const partitionDomaine = partition?domaine + '.' + partition:domaine
    return path.join(this.consignationPathBackup, 'domaines', partitionDomaine)
  }

  getPathBackupDomaines() {
    return path.join(this.consignationPathBackup, 'domaines')
  }

  trouverPathBackupApplication(nomApplication) {
    const pathBackup = path.join(
      this.consignationPathBackupApplications, nomApplication )
    return pathBackup
  }

  _formatterPath(fichierUuid, opts) {
    opts = opts || {}

    // var format = 'mgs3'
    // if(opts.format) {
    //   format = opts.format
    // } else if(opts.mimetype) {
    //   format = MAP_MIMETYPE_EXTENSION[opts.mimetype] || 'bin'
    // }

    // // Format du fichier (type de chiffrage)
    // const extension = '.' + format

    const niveau1 = fichierUuid.slice(0,8)
    const niveau2 = fichierUuid.slice(8,11)

    // return path.join(niveau1, niveau2, fichierUuid + extension)
    return path.join(niveau1, niveau2, fichierUuid)
  }

}

async function supprimerFichiers(fichiers, repertoire, opts) {
  opts = opts || {}
  var promises = fichiers.map(item=>{
    return new Promise((resolve, reject)=>{
      const fichier = path.join(repertoire, item);
      fs.unlink(fichier, err=>{
        if(err && !opts.noerror) {
          console.error("traitementFichiers.supprimerFichiers: Erreur suppression fichier : %O", err)
        }
        resolve()
      })
    })
  })

  debug("%d fichiers a supprimer", promises.length)

  // Attendre que tous les fichiers soient supprimes
  return Promise.all(promises)
}

async function supprimerRepertoiresVides(repertoireBase) {
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

async function extraireTarFile(fichierTar, destination, opts) {
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

async function deplacerFichier(req, nouveauPathFichier) {
  const fichier = req.file
  const pathRepertoire = path.dirname(nouveauPathFichier);

  await fsPromises.mkdir(pathRepertoire, {recursive: true})
  try {
    await fsPromises.rename(fichier.path, nouveauPathFichier)
  } catch(err) {
    console.warn("WARN - Rename (move) fichier vers destination %s echec, on copie", nouveauPathFichier)
    await fsPromises.copyFile(fichier.path, nouveauPathFichier)
    await fsPromises.unlink(fichier.path)
  }
}

// Instances

module.exports = {
  PathConsignation,
  extraireTarFile, supprimerRepertoiresVides, supprimerFichiers,
}
