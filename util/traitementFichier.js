const debug = require('debug')('millegrilles:fichiers:traitementFichier')
const fs = require('fs')
const readdirp = require('readdirp')
const path = require('path')
const uuidv1 = require('uuid/v1')
const crypto = require('crypto')
const lzma = require('lzma-native')
const { spawn } = require('child_process')
const readline = require('readline')
const tmp = require('tmp-promise')
const tar = require('tar')

const { uuidToDate } = require('./UUIDUtils')
const transformationImages = require('./transformationImages')
const { pki, ValidateurSignature } = require('./pki')
const { calculerHachageFichier } = require('./utilitairesHachage')

const MAP_MIMETYPE_EXTENSION = require('./mimetype_ext.json')
const MAP_EXTENSION_MIMETYPE = require('./ext_mimetype.json')

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

    this.consignationPath = consignationPath;

    // Path utilisable localement
    this.consignationPathLocal = path.join(this.consignationPath, 'grosfichiers');
    this.consignationPathBackup = path.join(this.consignationPath, 'backup');
    this.consignationPathBackupStaging = path.join(this.consignationPathBackup, 'staging');
  }

  // Retourne le path du fichier
  // Type est un dict {mimetype, extension} ou une des deux valeurs doit etre fournie
  trouverPathLocal(fichierUuid) {
    let pathFichier = this._formatterPath(fichierUuid)
    return path.join(this.consignationPathLocal, pathFichier);
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

  trouverPathBackupHoraire(domaine) {
    return path.join(this.consignationPathBackup, 'domaines', domaine, 'horaire')
  }

  trouverPathBackupSnapshot(domaine) {
    return path.join(this.consignationPathBackup, 'domaines', domaine, 'snapshot')
  }

  trouverPathBackupDomaine(domaine) {
    return path.join(this.consignationPathBackup, 'domaines', domaine)
  }

  getPathBackupDomaines() {
    return path.join(this.consignationPathBackup, 'domaines')
  }

  trouverPathBackupApplication(nomApplication) {
    const pathBackup = path.join(
      this.consignationPathBackup, 'applications', nomApplication )
    return pathBackup
  }

  _formatterPath(fichierUuid) {
    // Extrait la date du fileUuid, formatte le path en fonction de cette date.
    // let timestamp = uuidToDate.extract(fichierUuid.replace('/', ''));
    // console.debug("uuid: " + fichierUuid + ". Timestamp " + timestamp);

    const extension = 'mgs1'
    let nomFichier = fichierUuid + '.' + extension;

    return nomFichier;
  }

}

// const pathConsignation = new PathConsignation();

class TraitementFichier {

  constructor(rabbitMQ) {
    this.rabbitMQ = rabbitMQ;
    const idmg = rabbitMQ.pki.idmg;
    this.pathConsignation = new PathConsignation({idmg});
  }

  async traiterPut(req) {
    try {
      // Sauvegarde le fichier dans le repertoire de consignation local.
      const pathConsignation = new PathConsignation({idmg: req.autorisationMillegrille.idmg})

      // Le nom du fichier au complet, incluant path, est fourni dans fuuide.
      const transactionFichier = JSON.parse(req.body['transaction-fichier'])
      const transactionChiffrage = JSON.parse(req.body['transaction-chiffrage'])

      // Valider la signature de la transaction
      // Injecter le certificat recu pour s'assurer qu'il est distribue
      transactionFichier['_certificat'] = req.certificat
      transactionChiffrage['_certificat'] = req.certificat

      const transactionValideFichier = await this.rabbitMQ.pki.verifierSignatureMessage(transactionFichier)
      const transactionValideChiffrage = await this.rabbitMQ.pki.verifierSignatureMessage(transactionChiffrage)
      const hachageTransactionChiffrage = await this.rabbitMQ.pki.hacherTransaction(transactionChiffrage)
      const hachageTransactionFichier = await this.rabbitMQ.pki.hacherTransaction(transactionFichier)

      const hachageTransactionChiffrageEntete = transactionChiffrage['en-tete']['hachage_contenu']
      const hachageTransactionFichierEntete = transactionFichier['en-tete']['hachage_contenu']

      debug("Hachage transaction chiffrage calcule : %O\nHachage chiffrage fichier entete : %O",
        hachageTransactionChiffrage, hachageTransactionChiffrageEntete)
      debug("Hachage transaction fichier calcule : %O\nHachage transaction fichier entete : %O",
        hachageTransactionFichier, hachageTransactionFichierEntete)

      const hachageValideChiffrage = hachageTransactionChiffrage === hachageTransactionChiffrageEntete
      const hachageValideFichier = hachageTransactionFichier === hachageTransactionFichierEntete

      if( ! (transactionValideFichier && transactionValideChiffrage && hachageValideChiffrage && hachageValideFichier) ) {
        throw new Error(`Erreur validation transactions. Fichier signature:${transactionValideFichier}/hachage:${hachageValideFichier}. Chiffrage signature:${transactionValideChiffrage}/hachage:${hachageValideChiffrage}`)
      }

      // console.debug(headers);
      const fuuid = transactionFichier.fuuid
      const encrypte = transactionFichier.securite === '3.protege'
      const extension = path.parse(transactionFichier.nom_fichier).ext.replace('.', '')
      const mimetype = transactionFichier.mimetype

      let nouveauPathFichier = pathConsignation.trouverPathLocal(fuuid, encrypte, {extension, mimetype});

      // Creer le repertoire au besoin, puis deplacer le fichier (rename)
      const hachage = await calculHachage(req, transactionFichier.hachage)

      // Transmettre les transactions et deplacer le fichier
      await deplacerFichier(req, nouveauPathFichier)
      debug("Transmettre transaction chiffrage")
      await this.rabbitMQ.transmettreEnveloppeTransaction(transactionChiffrage)
      debug("Transmettre transaction fichier")
      await this.rabbitMQ.transmettreEnveloppeTransaction(transactionFichier)

      return({hachage})
    } finally {
      // Cleanup fichier upload
      const fichierPath = req.file.path
      fs.unlink(fichierPath, err=>{
        if(err) console.error("Erreur unlink sous multer " + fichierPath)
      })
    }
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

async function calculHachage(req, hachage) {
  return new Promise((resolve, reject)=>{
    // Deplacer le fichier
    const fichier = req.file

    const fichierReadStream = fs.createReadStream(fichier.path)
    const digester = crypto.createHash('sha512');

    fichierReadStream.on('data', data=>{
      debug("DATA ! %d", data.length)
      digester.update(data)
    })

    fichierReadStream.on('end', data=>{
      if(data) {
        digester.update(data)
      }
      const digest = 'sha512_b64:' + digester.digest('base64')
      debug("Digest calcule sur fichier %s", digest)

      // Verifier que le digest calcule correspond a celui recu
      if(digest !== hachage) {
        return reject(new Error(`Hachage fichier invalide pour fuuid : ${fichier.path}\n${digest}\n${hachage}`))
      }
      resolve(digest)
    })
  })
}

async function deplacerFichier(req, nouveauPathFichier) {
  await new Promise((resolve, reject)=>{
    const fichier = req.file
    const pathRepertoire = path.dirname(nouveauPathFichier);

    fs.mkdir(pathRepertoire, {recursive: true}, err=>{
      if(err) return reject(err)

      fs.rename(fichier.path, nouveauPathFichier, err => {
        if(err) return reject(err)
        resolve()
      })

    })
  })
}

async function streamListeFichiers(req, res, next) {
  // Transmet une liste de fichiers sous format .tar

  const tmpFichierBackup = await tmp.file({mode:0o600, prefix: 'backup-download-', postfix: '.tar'})
  const fichierBackup = tmpFichierBackup.path

  try {
    debug("Fichier TMP backup\n%s", fichierBackup)

    // Faire la liste des sous-repertoires a filtrer
    const pathBackup = res.pathRacineFichiers
    const fichiers = res.listeFichiers

    // Creer une archive .tar de backup avec les repertoires de fichiers
    // horaire, archive et instantanne
    debug("streamListeFichiers: Creer archive utilisant path %s", pathBackup)

    const downloadFileName = 'backup.tar'

    await tar.create( // or tar.create
      {
        cwd: pathBackup,
        file: fichierBackup,
      },
      fichiers
    )

    // Creer outputstream de reponse
    res.set('Content-Type', 'application/tar')
    res.set('Content-Disposition', 'attachment; filename="' + downloadFileName + '"')
    res.status(200)

    const readStream = fs.createReadStream(fichierBackup)

    await new Promise((resolve, reject)=>{
      readStream.on('close', _=>{
        resolve()
      })
      readStream.on('error', err=>{
        console.error("Erreur transfert\n%O", err)
        reject(err)
      })
      readStream.pipe(res)
    })

  } catch(err) {
    console.error("Erreur traitement TAR file\n%O", err)
    res.sendStatus(500)
  } finally {
    fs.unlink(fichierBackup, err=>{
      if(err) console.error("Erreur unlink " + fichierBackup)
    })
    res.end('')  // S'assurer que le stream est ferme
  }
}

async function getFichiersDomaine(domaine, pathRepertoireBackup, opts) {
  if(!opts) opts = {}

  // Ajuster le filtre de fichiers
  const filterArchives = [
    `${domaine}_*.tar`,
    `${domaine}.*_*.tar`,
  ]
  const filterHoraire = [
    `${domaine}_catalogue_*.json.xz`,
    `${domaine}_transactions_*.jsonl.xz`,
    `${domaine}_transactions_*.jsonl.xz.mgs1`,
    `${domaine}.*_catalogue_*.json.xz`,
    `${domaine}.*_transactions_*.jsonl.xz`,
    `${domaine}.*_transactions_*.jsonl.xz.mgs1`,
  ]
  var fileFilter = []
  if( ! opts.exclureHoraire ) {
    fileFilter = [...fileFilter, ...filterHoraire]
  }
  if( ! opts.exclureArchives ) {
    fileFilter = [...fileFilter, ...filterArchives]
  }

  var settings = {
    type: 'files',
    fileFilter,
  }

  debug("Setings fichiers : %O", settings)

  return new Promise((resolve, reject)=>{
    // const fichiersCatalogue = [];
    // const fichiersTransactions = [];

    const fichiersBackup = []

    readdirp(
      pathRepertoireBackup,
      settings,
    )
    .on('data', entry=>{
      // debug(entry)

      // Extraire le type de fichier (catalogue, transaction, fichier) et date
      const nomFichierParts = entry.basename.split('_')
      var sousdomaine = '', typeFichier = '', dateFichier = ''

      if(nomFichierParts.length === 3) {
        sousdomaine = nomFichierParts[0]
        dateFichier = nomFichierParts[1]
        if(dateFichier.length === 4) typeFichier = 'annuel'
        if(dateFichier.length === 8) typeFichier = 'quotidien'
      } else if(nomFichierParts.length === 4) {
        sousdomaine = nomFichierParts[0]
        typeFichier = nomFichierParts[1]
        dateFichier = nomFichierParts[2]
      }

      if(typeFichier) {
        const entreeBackup = {
          ...entry,
          sousdomaine, typeFichier, dateFichier
        }

        fichiersBackup.push(entreeBackup)
      } else {
        debug("Skip fichier %s", entry.path)
      }

    })
    .on('error', err=>{
      reject({err});
    })
    .on('end', ()=>{
      // debug("Fini");
      resolve(fichiersBackup);
    });

  });

  if(err) throw err;
}

async function getGrosFichiersHoraire(pathRepertoireBackup) {

  var settings = {
    type: 'files',
  }

  debug("Path backup horaire : %s\nSetings fichiers : %O", pathRepertoireBackup, settings)

  return new Promise((resolve, reject)=>{
    // const fichiersCatalogue = [];
    // const fichiersTransactions = [];

    const fichiersBackup = []

    readdirp(
      pathRepertoireBackup,
      settings,
    )
    .on('data', entry=>{
      try {
        // Le fichiers doit etre sous un repertoire */grosfichiers/*
        const splitPath = entry.path.split('/').filter(item=>item==='grosfichiers')
        if(splitPath.length === 1) {
          fichiersBackup.push(entry)
        }
      } catch(err) {
        console.error("Erreur path fichiers : %s", ''+err)
      }
    })
    .on('error', err=>{
      reject({err});
    })
    .on('end', ()=>{
      resolve(fichiersBackup);
    });

  });

  if(err) throw err;
}

// Instances

module.exports = {
  TraitementFichier, PathConsignation,
  extraireTarFile, supprimerRepertoiresVides, supprimerFichiers,
  streamListeFichiers, getFichiersDomaine, getGrosFichiersHoraire,
}
