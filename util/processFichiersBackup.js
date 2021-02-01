const debug = require('debug')('millegrilles:util:processFichiersBackup')
const fs = require('fs')
const readdirp = require('readdirp')
const path = require('path')
const crypto = require('crypto');
const lzma = require('lzma-native')
const tar = require('tar')
const parse = require('tar-parse')
const { calculerHachageFichier, calculerHachageData } = require('./utilitairesHachage')
const { formatterDateString } = require('@dugrema/millegrilles.common/lib/js_formatters')
const { supprimerFichiers, supprimerRepertoiresVides } = require('./traitementFichier')

async function traiterFichiersBackup(amqpdao, pathConsignation, fichierTransactions, fichierCatalogue, fichierMaitrecles) {
  // Meme repertoire pour toutes les transactions et catalogues horaire
  debug("fichiersTransactions : %O\n%O", fichierTransactions, fichierCatalogue)

  try {
    // Charger le fichier de catalogue pour obtenir information de domaine, heure
    const catalogue = await chargerLzma(fichierCatalogue.path)

    debug("Catalogue backup a traiter : %O", catalogue)

    const erreurValidation = await validerBackup(amqpdao, catalogue, fichierTransactions, fichierMaitrecles)

    if(erreurValidation) return erreurValidation // Erreur de validation

    // Valider transaction maitre des cles
    // Transmettre cles du fichier de transactions
    if(fichierMaitrecles) {
      const transactionMaitreDesCles = await chargerLzma(fichierMaitrecles.path)
      try {
        await amqpdao.transmettreEnveloppeTransaction(transactionMaitreDesCles)
      } catch(err) {
        return {
          err: 'Erreur transmission transaction maitre des cles',
          err_msg: err,
          err_serveur: true,
        }
      }
    }

    const nomFichierCatalogue = fichierCatalogue.originalname,
          pathFichierCatalogue = fichierCatalogue.path,
          pathFichierTransactions = fichierTransactions.path

    // Determiner si c'est un backup regulier ou un snapshot
    var repertoireDestination, nouveauPathCatalogue, nouveauPathTransactions, cleanupSnapshot
    if(catalogue.snapshot) {
      debug("Backup mode snapshot")
      repertoireDestination = pathConsignation.trouverPathBackupSnapshot(catalogue.domaine)
      nouveauPathCatalogue = path.join(repertoireDestination, 'catalogue.json.xz')
      nouveauPathTransactions = path.join(repertoireDestination, 'transactions.jsonl.xz.mgs1')
    } else {
      repertoireDestination = pathConsignation.trouverPathBackupHoraire(catalogue.domaine)
      nouveauPathCatalogue = path.join(repertoireDestination, nomFichierCatalogue)
      nouveauPathTransactions = path.join(repertoireDestination, fichierTransactions.originalname)
      cleanupSnapshot = () => {
        return new Promise((resolve, reject)=>{
          const repertoireSnapshot = pathConsignation.trouverPathBackupSnapshot(catalogue.domaine)
          fs.rmdir(repertoireSnapshot, {recursive: true}, err=>{
            if(err) {
              console.error("Erreur suppression snapshot sous %s : %O", repertoireSnapshot, err)
              return reject(err)
            }
            resolve()
          })
        })
      }
    }

    await new Promise((resolve, reject)=>{
      // Creer tous les repertoires requis pour le backup
      fs.mkdir(repertoireDestination, { recursive: true, mode: 0o770 }, (err)=>{
        if(err) return reject(err)
        resolve()
      })
    })

    // Deplacer le fichier de catalogue du backup
    debug("Copier catalogue %s -> %s", pathFichierCatalogue, nouveauPathCatalogue)
    await deplacerFichier(pathFichierCatalogue, nouveauPathCatalogue)
    await deplacerFichier(pathFichierTransactions, nouveauPathTransactions)

    if(cleanupSnapshot) await cleanupSnapshot()

    return {ok: true}
  } catch(err) {
    return {err: "Erreur traitement generique", err_msg: err}
  } finally {
    // Cleanup fichiers uploades
    fs.unlink(fichierTransactions.path, ()=>{})
    fs.unlink(fichierCatalogue.path, ()=>{})
  }
}

async function validerBackup(amqpdao, catalogue, fichierTransactions, fichierMaitrecles) {
  // Valider le fichier de transactions
  const hachageTransactions = await calculerHachageFichier(fichierTransactions.path)
  if(catalogue.hachage_transactions !== hachageTransactions) {
    debug("Erreur comparaison hachage transactions du catalogue (%s) et calcule (%s)", catalogue.hachage_transactions, hachageTransactions)
    return {
      err: "Mismatch sur le hachage des transactions",
      [fichierTransactions.originalname]: hachageTransactions
    }
  }

  // Valider le catalogue
  if( ! await amqpdao.pki.verifierSignatureMessage(catalogue) ) {
    return {
      err: "Signature catalogue invalide",
    }
  }

  // Valider transaction maitre des cles
  // Transmettre cles du fichier de transactions
  if(fichierMaitrecles) {
    const transactionMaitreDesCles = await chargerLzma(fichierMaitrecles.path)

    debug("Transmettre cles du fichier de transactions : %O", transactionMaitreDesCles)
    if( ! await amqpdao.pki.verifierSignatureMessage(transactionMaitreDesCles) ) {
      return {
        err: "La signature de la transaction maitre des cles est invalide",
      }
    }
    try {
      await amqpdao.transmettreEnveloppeTransaction(transactionMaitreDesCles)
    } catch(err) {
      return {
        err: 'Erreur transmission transaction maitre des cles',
        err_msg: err,
        err_serveur: true,
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

  await rotationArchiveApplication(pathBackupApplication)
}

async function rotationArchiveApplication(pathBackupApplication) {
  // Fait la rotation des archives dans un repertoire d'application
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

async function genererBackupQuotidien(mq, pathConsignation, catalogue) {
  debug("Generer backup quotidien : %O", catalogue);

  const repertoireBackup = pathConsignation.trouverPathBackupDomaine(catalogue.domaine)

  try {
    const informationArchive = await traiterBackupQuotidien(mq, pathConsignation, catalogue)
    const { fichiersInclure, pathRepertoireBackup } = informationArchive
    delete informationArchive.fichiersInclure // Pas necessaire pour la transaction
    delete informationArchive.pathRepertoireBackup // Pas necessaire pour la transaction

    // Finaliser le backup en retransmettant le journal comme transaction
    // de backup quotidien. Met le flag du document quotidien a false
    debug("Transmettre journal backup quotidien comme transaction de backup quotidien")
    await mq.transmettreEnveloppeTransaction(catalogue)

    // Generer transaction pour journal mensuel. Inclue SHA512 et nom de l'archive quotidienne
    debug("Transmettre transaction informationArchive :\n%O", informationArchive)
    await mq.transmettreTransactionFormattee(informationArchive, 'Backup.archiveQuotidienneInfo')

    // Effacer les fichiers transferes dans l'archive quotidienne
    const fichiersASupprimer = fichiersInclure.filter(item=>item.startsWith('horaire/'))
    await nettoyerRepertoireBackupHoraire(pathConsignation, catalogue.domaine, fichiersASupprimer)

    return informationArchive

  } catch (err) {
    console.error("genererBackupQuotidien: Erreur creation backup quotidien:\n%O", err)
  }

}

async function nettoyerRepertoireBackupHoraire(pathConsignation, domaine, fichiersASupprimer) {
  debug("Supprimer fichiers backup %O", fichiersASupprimer)

  const repertoireBackup = pathConsignation.trouverPathBackupDomaine(domaine)
  await supprimerFichiers(fichiersASupprimer, repertoireBackup)

  try {
    const repertoireBackupHoraire = pathConsignation.trouverPathBackupHoraire(domaine)
    await new Promise((resolve, reject)=>{
      fs.rmdir(repertoireBackupHoraire, err=>{
        if(err) return reject(err)
        debug("Repertoire horaire supprime : %s", repertoireBackupHoraire)
        resolve()
      })
    })
  } catch(err) {
    console.error("Erreur suppression repertoire de backup horaire: %O", err)
  }
}

async function genererBackupAnnuel(mq, pathConsignation, catalogue) {
  debug("Generer backup annuel : %O", catalogue);

  return new Promise( async (resolve, reject) => {

    try {
      const informationArchive = await traiterBackupAnnuel(mq, pathConsignation, catalogue)

      debug("Journal annuel sauvegarde : %O", informationArchive)

      // Finaliser le backup en retransmettant le journal comme transaction
      // de backup quotidien
      await mq.transmettreEnveloppeTransaction(catalogue)

      // Generer transaction pour journal annuel. Inclue SHA512 et nom de l'archive mensuelle
      const {fichiersInclure} = informationArchive

      delete informationArchive.fichiersInclure
      delete informationArchive.pathRepertoireBackup

      debug("Transmettre transaction avec information \n%O", informationArchive)
      await mq.transmettreTransactionFormattee(informationArchive, 'Backup.archiveAnnuelleInfo')

      const domaine = catalogue.domaine
      const pathArchivesQuotidiennes = pathConsignation.trouverPathBackupDomaine(domaine)

      const fichiersSupprimer = fichiersInclure.filter((item, idx)=>idx>0)
      await supprimerFichiers(fichiersSupprimer, pathArchivesQuotidiennes)

      return resolve(informationArchive)

    } catch (err) {
      console.error("Erreur creation backup annuel")
      console.error(err)
      reject(err)
    }

  })

}

// Genere un fichier de backup quotidien qui correspond au catalogue
async function traiterBackupQuotidien(mq, pathConsignation, catalogue) {
  debug("genererBackupQuotidien : catalogue \n%O", catalogue)

  const {domaine, securite} = catalogue
  const jourBackup = new Date(catalogue.jour * 1000)
  const repertoireBackup = pathConsignation.trouverPathBackupDomaine(domaine)
  const repertoireBackupHoraire = pathConsignation.trouverPathBackupHoraire(domaine)

  const listeCataloguesHoraires = await genererListeCatalogues(repertoireBackupHoraire)
  debug("Liste de catalogues sous %s : %O", repertoireBackupHoraire, listeCataloguesHoraires)

  // Faire liste des fichiers de catalogue et transactions a inclure dans le tar quotidien
  const fichiersInclure = []

  // Charger l'information de tous les catalogues horaire correspondants au
  // backup quotidien. Valide le hachage des fichiers de catalogue et de
  // transaction.
  // for(let heureStr in catalogue.fichiers_horaire) {
  for(let idx in listeCataloguesHoraires) {
    const catalogueNomFichier = listeCataloguesHoraires[idx]

    // Charger backup horaire. Valide le hachage des transactions
    const infoHoraire = await chargerBackupHoraire(pathConsignation, domaine, catalogueNomFichier)
    debug("Preparer backup horaire : %O", infoHoraire)

    const heureBackup = new Date(infoHoraire.catalogue.heure*1000)
    if(heureBackup.getUTCFullYear() === jourBackup.getUTCFullYear() &&
        heureBackup.getUTCMonth() === jourBackup.getUTCMonth() &&
        heureBackup.getUTCDate() !== jourBackup.getUTCDate()
      ) {
      debug("backup jour %s ne correspond pas au backup horaire %s", jourBackup, heureBackup )
      // Passer au prochain fichier
      continue
    }

    var heureStr = ''+heureBackup.getUTCHours()
    if(heureStr.length == 1) heureStr = '0' + heureStr; // Ajouter 0 devant heure < 10

    let infoFichier = catalogue.fichiers_horaire[heureStr]
    if(infoFichier) {
      // debug("Preparer backup heure %s :\n%O", heureStr, infoFichier)

      // Verifier hachage du catalogue horaire (si present dans le catalogue quotidien)
      if(infoFichier.catalogue_hachage && infoFichier.catalogue_hachage !== infoHoraire.hachageCatalogue) {
        // throw new Error(`Hachage catalogue ${pathCatalogue} mismatch : calcule ${infoHoraire.hachageCatalogue}`)
        console.warning(`Hachage catalogue ${pathCatalogue} mismatch : calcule ${infoHoraire.hachageCatalogue}. On regenere valeurs avec fichiers locaux.`)
      }
    } else {
      debug("Catalogue quotidien recu n'a pas backup horaire %s, information generee a partir des fichiers locaux", heureStr)
      infoFichier = {
        catalogue_nomfichier: catalogueNomFichier,
        transactions_nomfichier: infoHoraire.catalogue.transactions_nomfichier,
        transactions_hachage: infoHoraire.catalogue.transactions_hachage,
      }
      catalogue.fichiers_horaire[heureStr] = infoFichier
    }

    // Conserver information manquante dans le catalogue quotidien
    infoFichier.catalogue_hachage = infoHoraire.hachageCatalogue
    infoFichier.hachage_entete = calculerHachageData(infoHoraire.catalogue['en-tete'].hachage_contenu)
    infoFichier.uuid_transaction = infoHoraire.catalogue['en-tete'].uuid_transaction

    // Conserver path des fichiers relatif au path horaire Utilise pour l'archive tar.
    fichiersInclure.push(path.relative(repertoireBackup, infoHoraire.pathCatalogue))
    fichiersInclure.push(path.relative(repertoireBackup, infoHoraire.pathTransactions));
  }

  // Sauvegarder journal quotidien, sauvegarder en format .json.xz
  if( ! catalogue['_signature'] ) {
    debug("Regenerer signature du catalogue horaire, entete precedente : %O", catalogue['en-tete'])
    // Journal est dirty, on doit le re-signer
    // const domaine = catalogue['en-tete'].domaine
    delete catalogue['en-tete']
    mq.formatterTransaction(domaine, catalogue)
  }

  var resultat = await sauvegarderCatalogueQuotidien(pathConsignation, catalogue)
  debug("Resultat sauvegarder catalogue quotidien : %O", resultat)
  const pathCatalogue = resultat.path
  const nomCatalogue = path.basename(pathCatalogue)
  fichiersInclure.unshift(nomCatalogue)  // Inserer comme premier fichier dans le .tar, permet traitement stream

  // Creer nom du fichier d'archive
  const infoArchiveQuotidienne = await genererTarArchiveQuotidienne(
    pathConsignation, domaine, jourBackup, fichiersInclure)

  // Supprimer les fichiers horaires, catalogue quotidien
  const pathBackup = pathConsignation.trouverPathBackupDomaine(domaine)
  var promisesUnlink = fichiersInclure.map(item=>{
    const pathFichier = path.join(pathBackup, item)
    return new Promise((resolve, reject)=>{
      fs.unlink(pathFichier, err=>{
        if(err) return reject(err)
        resolve()
      })
    })
  })
  await Promise.all(promisesUnlink)

  const informationArchive = {
    archive_hachage: infoArchiveQuotidienne.hachageArchive,
    archive_nomfichier: infoArchiveQuotidienne.nomArchive,
    jour: catalogue.jour,
    domaine: catalogue.domaine,
    securite: catalogue.securite,

    fichiersInclure,
    pathRepertoireBackup: repertoireBackup,
    catalogue,
  }

  return informationArchive

}

async function verifierGrosfichiersBackup(pathConsignation, infoGrosfichiers) {

  // Verifier presence et hachage de chaque grosfichier
  var resultat = []
  for(let fuuid in infoGrosfichiers) {
    const infoFichier = infoGrosfichiers[fuuid]
    var nomFichier = null
    try {
      const fichier = await pathConsignation.trouverPathFuuidExistant(fuuid)
      nomFichier = path.basename(fichier)

      debug("Verification grosfichier %s\n%O", fichier, infoFichier)

      // Verifier le hachage des fichiers a inclure
      const fonctionHash = infoFichier.hachage.split(':')[0] || 'sha512_b64'
      const hachageCalcule = await calculerHachageFichier(fichier, {fonctionHash});
      if(infoFichier.hachage) {
        if(hachageCalcule !== infoFichier.hachage) {
          debug("Erreur verification hachage grosfichier\nCatalogue : %s\nCalcule : %s", infoFichier.hachage, hachageCalcule)
          resultat.push({fuuid, nomFichier, err: 'Hachage mismatch', hachage: hachageCalcule});
        } else {
          debug("Hachage grosfichier OK : %s => %s ", hachageCalcule, nomFichier)
          resultat.push({fuuid, nomFichier, hachage: hachageCalcule});
        }
      } else {
        resultat.push({fuuid, nomFichier, err: 'Hachage absent du catalogue', hachage: hachageCalcule});
      }

    } catch (err) {
      if(err.err) err = err.err
      const reponseFichier = {fuuid, nomFichier, err}
      if(infoFichier.hachage) reponseFichier.hachage = infoFichier.hachage
      resultat.push(reponseFichier);
    }
  }

  return resultat
}

async function genererTarArchiveQuotidienne(pathConsignation, domaine, dateJour, fichiersInclure) {
  // Genere un fichier .tar d'une archive quotidienne avec tous les catalogues et transactions
  // - pathRepertoireArchive: str path du repertoire output de l'archive
  // - domaine: str sous-domaine du backup
  // - dateJour: Date() du contenu de fichier de backup
  // - securite: str '3.protege'
  // - fichiersInclure: list Path des fichiers a inclure en ordre

  const repertoireBackupQuotidien = pathConsignation.trouverPathBackupDomaine(domaine)
  debug("Path repertoire ackup : %s", repertoireBackupQuotidien)

  const dateFormattee = formatterDateString(dateJour).slice(0, 8)  // Retirer heures
  var nomArchive = [domaine, dateFormattee + '.tar'].join('_')
  const pathArchiveQuotidienne = path.join(repertoireBackupQuotidien, nomArchive)
  debug("Path archive quotidienne : %s", pathArchiveQuotidienne)

  var fichiersInclureStr = fichiersInclure.join('\n');
  debug(`Fichiers quotidien inclure relatif a ${repertoireBackupQuotidien} : \n${fichiersInclure}`);

  // Creer nouvelle archive quotidienne
  await tar.c(
    {
      file: pathArchiveQuotidienne,
      cwd: repertoireBackupQuotidien,
    },
    fichiersInclure
  )

  // Calculer le SHA512 du fichier d'archive
  const hachageArchive = await calculerHachageFichier(pathArchiveQuotidienne)

  return {hachageArchive, pathArchiveQuotidienne, nomArchive}
}

async function chargerBackupHoraire(pathConsignation, domaine, nomFichierCatalogue) {
  // Charger et verifier un backup horaire pour un domaine
  // - dateHeure: objet Date
  // - nomFichierCatalogue: str, nom du fichier de catalogue
  // - nomFichierTransactions: str, nom du fichier de transactions (e.g. DOMAINE_transactions_DATE_SECURITE.jsonl.xz)

  var repertoireBackup = pathConsignation.trouverPathBackupHoraire(domaine)

  // Verifier SHA512
  const pathCatalogue = path.join(repertoireBackup, nomFichierCatalogue)
  const hachageCatalogue = await calculerHachageFichier(pathCatalogue)
  // if( ! infoFichier.catalogue_hachage ) {
  //   delete catalogue['_signature']  // Signale qu'on doit regenerer entete et signature du catalogue (dirty)
  //
  //   console.warn("genererBackupQuotidien: Hachage manquant pour catalogue horaire %s, on cree l'entree au vol", fichierCatalogue)
  // Il manque de l'information pour l'archive quotidienne, on insere les valeurs maintenant
  const catalogue = await new Promise((resolve, reject)=>{
    fs.readFile(pathCatalogue, (err, data)=>{
      if(err) return reject(err)
      try {
        lzma.LZMA().decompress(data, (data, err)=>{
          if(err) return reject(err)
          const catalogue = JSON.parse(data)
          // console.info("Catalogue horaire %s charge : %O", pathCatalogue, catalogue)
          resolve(catalogue)
        })
      } catch(err) {
        reject(err)
      }
    })
  })
  // } else if(sha512Catalogue != infoFichier.catalogue_hachage) {
  //   throw `Fichier catalogue ${fichierCatalogue} ne correspond pas au hachage : ${sha512Catalogue}`
  // }

  let pathTransactions = path.join(repertoireBackup, catalogue.transactions_nomfichier);
  const hachageTransactions = await calculerHachageFichier(pathTransactions);
  if(hachageTransactions !== catalogue.transactions_hachage) {
    throw `Fichier transaction ${pathTransactions} hachage ${hachageTransactions}
    ne correspond pas au hachage du catalogue : ${catalogue.transactions_hachage}`;
  }

  return {
    catalogue,
    hachageCatalogue,
    pathCatalogue,
    pathTransactions,
  }
}

// Genere un fichier tar de backup annuel avec toutes les archives quotidiennes
// du domaine pour l'annee
async function traiterBackupAnnuel(mq, pathConsignation, catalogue) {

  const {domaine, securite} = catalogue
  const anneeBackup = new Date(catalogue.annee * 1000)

  const pathRepertoireBackup = pathConsignation.trouverPathBackupDomaine(domaine)

  const fichiersInclure = []

  // Trier la liste des jours pour ajout dans le fichier .tar
  const listeJoursTriee = Object.keys(catalogue.fichiers_quotidien)
  listeJoursTriee.sort()
  debug("Liste jours tries : %O", listeJoursTriee)

  for(const idx in listeJoursTriee) {
    const jourStr = listeJoursTriee[idx]
    debug("Traitement jour %s", jourStr)
    let infoFichier = catalogue.fichiers_quotidien[jourStr]
    debug("Verifier fichier backup quotidien %s :\n%O", jourStr, infoFichier)

    let fichierArchive = infoFichier.archive_nomfichier

    // Verifier SHA512
    const pathFichierArchive = path.join(pathRepertoireBackup, fichierArchive)
    const hachageArchive = await calculerHachageFichier(pathFichierArchive)
    if(hachageArchive != infoFichier.archive_hachage) {
      throw new Error(`Fichier archive ${fichierArchive} ne correspond pas au hachage : ${hachageArchive}`)
    }

    fichiersInclure.push(fichierArchive)
  }

  // Sauvegarder journal annuel, sauvegarder en format .json.xz
  var resultat = await sauvegarderCatalogueAnnuel(pathConsignation, catalogue)
  debug("Resultat preparation catalogue annuel : %O", resultat)
  const pathCatalogue = resultat.path
  const nomCatalogue = path.basename(pathCatalogue)
  fichiersInclure.unshift(nomCatalogue)  // Ajouter le catalogue comme premiere entree du .tar

  debug("Path repertoire archives \nannuelles : %s", pathRepertoireBackup)
  await new Promise((resolve, reject)=>{
    fs.mkdir(pathRepertoireBackup, { recursive: true, mode: 0o770 }, err=>{
      if(err) return reject(err);
      resolve();
    })
  })

  // Creer nom du fichier d'archive - se baser sur le nom du catalogue quotidien
  var nomArchive = [domaine, resultat.dateFormattee + '.tar'].join('_')
  const pathArchiveAnnuelle = path.join(pathRepertoireBackup, nomArchive)
  debug("Path archive annuelle : %s", pathArchiveAnnuelle)

  debug('Archive annuelle inclure : %O', fichiersInclure)

  // Creer nouvelle archive annuelle
  await tar.c(
    {
      file: pathArchiveAnnuelle,
      cwd: pathRepertoireBackup,
    },
    fichiersInclure
  )

  // Calculer le hachage du fichier d'archive
  const hachageArchive = await calculerHachageFichier(pathArchiveAnnuelle)

  const informationArchive = {
    archive_hachage: hachageArchive,
    archive_nomfichier: nomArchive,
    annee: catalogue.annee,
    domaine,
    securite,

    fichiersInclure,
    pathRepertoireBackup,
    catalogue,
  }

  return informationArchive

}

async function sauvegarderCatalogueQuotidien(pathConsignation, catalogue) {
  const {domaine, securite, jour} = catalogue

  const dateJournal = new Date(jour*1000)
  var repertoireBackup = pathConsignation.trouverPathBackupHoraire(domaine)

  // Remonter du niveau heure a jour
  repertoireBackup = path.dirname(repertoireBackup);

  const dateFormattee = formatterDateString(dateJournal).slice(0, 8)  // Retirer heures
  const nomFichier = domaine + "_catalogue_" + dateFormattee  + ".json.xz"
  const fullPathFichier = path.join(repertoireBackup, nomFichier)

  await sauvegarderLzma(fullPathFichier, catalogue)

  // debug("Fichier cree : " + fullPathFichier);
  return {path: fullPathFichier, nomFichier, dateFormattee}
}

async function sauvegarderCatalogueAnnuel(pathConsignation, catalogue) {
  const {domaine, securite, annee} = catalogue

  const dateJournal = new Date(annee*1000)
  var repertoireBackup = pathConsignation.trouverPathBackupDomaine(domaine)

  let year = dateJournal.getUTCFullYear();
  const dateFormattee = "" + year

  const nomFichier = domaine + "_catalogue_" + dateFormattee + ".json.xz";

  const fullPathFichier = path.join(repertoireBackup, nomFichier)

  // debug("Path fichier journal mensuel " + fullPathFichier);
  await sauvegarderLzma(fullPathFichier, catalogue)

  return {path: fullPathFichier, nomFichier, dateFormattee}
}

async function chargerLzma(fichier) {
  return new Promise((resolve, reject)=>{
    fs.readFile(fichier, (err, data)=>{
      if(err) return reject(err)
      try {
        lzma.LZMA().decompress(data, (data, err)=>{
          if(err) return reject(err)
          const catalogue = JSON.parse(data)
          resolve(catalogue)
        })
      } catch(err) {
        reject(err)
      }
    })
  })
}

async function sauvegarderLzma(fichier, contenu) {
  var compressor = lzma.createCompressor()
  var output = fs.createWriteStream(fichier)
  compressor.pipe(output)

  const promiseSauvegarde = new Promise((resolve, reject)=>{
    output.on('close', ()=>{resolve()})
    output.on('error', err=>{reject(err)})
  })

  compressor.write(JSON.stringify(contenu))
  compressor.end()
  return promiseSauvegarde
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

async function genererListeCatalogues(repertoire) {
  // Faire la liste des fichiers extraits - sera utilisee pour creer
  // l'ordre de traitement des fichiers pour importer les transactions
  const settingsReaddirp = {
    type: 'files',
    fileFilter: [
       '*.json.xz',
    ],
  }

  const {err, listeCatalogues} = await new Promise((resolve, reject)=>{
    const listeCatalogues = [];
    // console.debug("Lister catalogues sous " + repertoire);

    readdirp(
      repertoire,
      settingsReaddirp,
    )
    .on('data', entry=>{
      // console.debug('Catalogue trouve');
      // console.debug(entry);
      listeCatalogues.push(entry.path)
    })
    .on('error', err=>{
      reject({err});
    })
    .on('end', ()=>{
      // console.debug("Fini");
      // console.debug(listeCatalogues);
      resolve({listeCatalogues});
    });
  });

  if(err) throw err;

  // console.debug("Resultat catalogues");
  // console.debug(listeCatalogues);
  return listeCatalogues;

}

async function trouverArchivesQuotidiennes(repertoire) {
  // Faire la liste des fichiers extraits - sera utilisee pour creer
  // l'ordre de traitement des fichiers pour importer les transactions
  const settingsReaddirp = {
    type: 'files',
    fileFilter: [
       '*.tar',
    ],
  }

  const pathCatalogues = await new Promise((resolve, reject)=>{
    const pathCatalogues = [];
    // console.debug("Lister catalogues sous " + repertoire);

    readdirp(
      repertoire,
      settingsReaddirp,
    )
    .on('data', entry=>{
      // console.debug('Catalogue trouve');
      // console.debug(entry);
      pathCatalogues.push(path.join(repertoire, entry.path))
    })
    .on('error', err=>{
      reject(err);
    })
    .on('end', ()=>{
      // console.debug("Fini");
      // console.debug(listeCatalogues);
      resolve(pathCatalogues)
    })
  })

  const cataloguesQuotidiens = {}
  for(let idx in pathCatalogues) {
    const pathCatalogue = pathCatalogues[idx]
    var catalogues = await extraireCatalogues(pathCatalogue)
    catalogues = catalogues.forEach(item=>{
      if(item.jour) {
        const jourFormatte = formatterDateString(new Date(item.jour*1000)).slice(0, 8)  // Retirer heures
        cataloguesQuotidiens[jourFormatte] = item
      }
    })
  }

  debug("trouverArchivesQuotidiennes: Resultat catalogues %O", cataloguesQuotidiens);
  return cataloguesQuotidiens;

}

async function extraireCatalogues(pathArchive) {

  const catalogues = await new Promise((resolve, reject)=>{
    var promises = []

    // Parse
    const tarParser = parse()
      .on('data', entry=>{
        if(entry.path.toLowerCase().endsWith('.json.xz')) {
          promises.push( extraireCatalogueEntry(entry) )
        }
      })
      .on('end', ()=>{
        const catalogues = Promise.all(promises)
        resolve(catalogues)
      })
      .on('error', err=>reject(err))

    // Attendre 1 tick avant de demarrer lecture .tar
    // Bugfix pour erreur random "invalid tar file"
    const stream = fs.createReadStream(pathArchive)
    setTimeout(()=>{
      stream.pipe(tarParser)
    },0)

  }) // Promise

  return catalogues
}

async function extraireCatalogueEntry(entryTar) {
  return new Promise((resolve, reject)=>{
    var decoder = lzma.createStream('autoDecoder')
    var data = ''

    decoder.on("data", buffer=>{
      data += buffer
    })

    decoder.on("end", async ()=>{
      entryTar.pause()

      const catalogue = JSON.parse(data)
      resolve(catalogue)

      entryTar.resume()
    })

    decoder.on("error", err=>{
      reject(err)
      entryTar.resume()
    })

    // Input tar entry vers lzma
    entryTar.pipe(decoder)

    entryTar.resume()  // Continuer a lire
  }) // Promise data

}

module.exports = {
  traiterFichiersBackup, traiterFichiersApplication,
  genererBackupQuotidien, genererBackupAnnuel, genererListeCatalogues,

  sauvegarderFichiersApplication, rotationArchiveApplication,
  sauvegarderCatalogueQuotidien, sauvegarderCatalogueAnnuel,
  traiterBackupQuotidien, sauvegarderLzma, verifierGrosfichiersBackup,
  traiterBackupAnnuel, chargerLzma, trouverArchivesQuotidiennes

  // linkGrosfichiersSousBackup,
}
