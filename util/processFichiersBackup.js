const debug = require('debug')('millegrilles:util:processFichiersBackup')
const fs = require('fs')
const readdirp = require('readdirp')
const path = require('path')
const crypto = require('crypto');
const lzma = require('lzma-native')
const tar = require('tar')
const { calculerHachageFichier, calculerHachageData } = require('./utilitairesHachage')
const { formatterDateString } = require('@dugrema/millegrilles.common/lib/js_formatters')

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

async function linkGrosfichiersSousBackup(pathConsignation, pathRepertoire, fuuidList) {
  // Effectue un hard-link d'un grosfichier sous le repertoire de backup horaire

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

  const fichiers = []

  for(const idx in fuuidList) {
    const fuuid = fuuidList[idx]
    // const paramFichier = fuuidDict[fuuid];
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
        })

        fichiers.push(pathFichierBackup)

      } else {
        // console.warn("Fichier " + fuuid + "  non trouve");
        return({err: "Fichier " + fuuid + "  non trouve"})
      }
    }

  }

  return {fichiers}
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

async function genererBackupQuotidien(mq, routingKey, message, opts) {
  debug("Generer backup quotidien : %O", message);

  try {
    const catalogue = message.catalogue
    const informationArchive = await traiterBackupQuotidien(mq, this.pathConsignation, catalogue)
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
    await supprimerFichiers(fichiersInclure, pathRepertoireBackup)
    await supprimerRepertoiresVides(this.pathConsignation.consignationPathBackupHoraire)

  } catch (err) {
    console.error("genererBackupQuotidien: Erreur creation backup quotidien:\n%O", err)
  }

}

async function genererBackupAnnuel(mq, routingKey, message, opts) {
  debug("Generer backup annuel : %O", message);

  return new Promise( async (resolve, reject) => {

    try {
      const informationArchive = await traiterBackupAnnuel(mq, this.pathConsignation, message.catalogue)

      debug("Journal annuel sauvegarde : %O", informationArchive)

      // Finaliser le backup en retransmettant le journal comme transaction
      // de backup quotidien
      await mq.transmettreEnveloppeTransaction(message.catalogue)

      // Generer transaction pour journal annuel. Inclue SHA512 et nom de l'archive mensuelle
      const {fichiersInclure} = informationArchive

      delete informationArchive.fichiersInclure
      delete informationArchive.pathRepertoireBackup

      debug("Transmettre transaction avec information \n%O", informationArchive)
      await mq.transmettreTransactionFormattee(informationArchive, 'Backup.archiveAnnuelleInfo')

      const domaine = message.catalogue.domaine
      const pathArchivesQuotidiennes = this.pathConsignation.trouverPathDomaineQuotidien(domaine) // path.join(this.pathConsignation.consignationPathBackupArchives, 'quotidiennes', domaine)

      await supprimerFichiers(fichiersInclure, pathArchivesQuotidiennes)
      await supprimerRepertoiresVides(path.join(this.pathConsignation.consignationPathBackupArchives, 'quotidiennes'))

    } catch (err) {
      console.error("Erreur creation backup annuel")
      console.error(err)
    }

    resolve()
  })

}

// Genere un fichier de backup quotidien qui correspond au catalogue
async function traiterBackupQuotidien(mq, pathConsignation, catalogue) {
  debug("genererBackupQuotidien : catalogue \n%O", catalogue)

  const {domaine, securite} = catalogue
  const jourBackup = new Date(catalogue.jour * 1000)
  const repertoireBackup = pathConsignation.trouverPathBackupQuotidien(jourBackup)

  // Faire liste des fichiers de catalogue et transactions a inclure dans le tar quotidien
  const fichiersInclure = []

  // Charger l'information de tous les catalogues horaire correspondants au
  // backup quotidien. Valide le hachage des fichiers de catalogue et de
  // transaction.
  for(let heureStr in catalogue.fichiers_horaire) {
    const heureBackup = new Date(jourBackup.getTime())
    heureBackup.setUTCHours(heureStr)
    if(heureStr.length == 1) heureStr = '0' + heureStr; // Ajouter 0 devant heure < 10

    let infoFichier = catalogue.fichiers_horaire[heureStr]
    debug("Preparer backup heure %s :\n%O", heureStr, infoFichier)

    // Charger backup horaire. Valide le hachage des transactions
    const infoHoraire = await chargerBackupHoraire(pathConsignation, heureBackup, infoFichier.catalogue_nomfichier)

    // Verifier hachage du catalogue horaire (si present dans le catalogue quotidien)
    if(infoFichier.catalogue_hachage && infoFichier.catalogue_hachage !== infoHoraire.hachageCatalogue) {
      throw new Error(`Hachage catalogue ${pathCatalogue} mismatch : calcule ${infoHoraire.hachageCatalogue}`)
    }

    // Conserver information manquante dans le catalogue quotidien
    infoFichier.catalogue_hachage = infoHoraire.hachageCatalogue
    infoFichier.hachage_entete = calculerHachageData(infoHoraire.catalogue['en-tete'].hachage_contenu)
    infoFichier['uuid-transaction'] = infoHoraire.catalogue['en-tete']['uuid-transaction']

    // Conserver path des fichiers relatif au path horaire Utilise pour l'archive tar.
    fichiersInclure.push(path.relative(repertoireBackup, infoHoraire.pathCatalogue))
    fichiersInclure.push(path.relative(repertoireBackup, infoHoraire.pathTransactions));
  }

  // Faire liste des grosfichiers au besoin
  if(catalogue.fuuid_grosfichiers) {
    // Aussi inclure le repertoire des grosfichiers
    // fichiersInclureStr = `${fichiersInclureStr} */grosfichiers/*`

    // await linkGrosfichiersSousBackup(pathConsignation, pathRepertoire, fuuidList)

    preparerGrosfichiresBackupQuotidien()

    // for(let fuuid in catalogue.fuuid_grosfichiers) {
    //   const {err, fichier} = await pathConsignation.trouverPathFuuidExistant(fuuid)
    //   if(err) throw new Error("Erreur grosfichier: " + err)
    //
    //   let infoFichier = catalogue.fuuid_grosfichiers[fuuid]
    //   let heureStr = infoFichier.heure
    //   if(heureStr.length == 1) heureStr = '0' + heureStr
    //
    //   const extension = 'mgs1'
    //   let nomFichier = path.join(heureStr, 'grosfichiers', `${fuuid}.${extension}`)
    //
    //   debug("Ajout grosfichier %s", nomFichier)
    //
    //   // Verifier le hachage des fichiers a inclure
    //   if(infoFichier.hachage) {
    //     const fonctionHash = infoFichier.hachage.split(':')[0]
    //     const hachageCalcule = await calculerHachageFichier(
    //       path.join(repertoireBackup, nomFichier), {fonctionHash});
    //
    //     if(hachageCalcule !== infoFichier.hachage) {
    //       debug("Erreur verification hachage grosfichier\nCatalogue : %s\nCalcule : %s", infoFichier.hachage, hachageCalcule)
    //       throw `Erreur Hachage sur fichier : ${nomFichier}`
    //     } else {
    //       debug("Hachage grosfichier OK : %s => %s ", hachageCalcule, nomFichier)
    //     }
    //   } else {
    //     throw `Erreur Hachage absent sur fichier : ${nomFichier}`;
    //   }
    //
    //   fichiersInclure.push(nomFichier);
    // }
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

  const informationArchive = {
    archive_hachage: infoArchiveQuotidienne.hachageArchive,
    archive_nomfichier: infoArchiveQuotidienne.nomArchive,
    jour: catalogue.jour,
    domaine: catalogue.domaine,
    securite: catalogue.securite,

    fichiersInclure,
    pathRepertoireBackup: repertoireBackup,
  }

  return informationArchive

}

async function preparerGrosfichiresBackupQuotidien(pathConsignation, pathRepertoireHoraire, infoGrosfichiers) {

  // Preparer sous-repertoire grosfichiers/ sous le backup horaire
  const pathBackupGrosFichiers = path.join(pathRepertoireHoraire, 'grosfichiers');
  await new Promise((resolve, reject)=>{
    fs.mkdir(pathBackupGrosFichiers, { recursive: true, mode: 0o770 }, err => {
      if(err) return reject(err)
      resolve()
    })
  })

  // Verifier presence et hachage de chaque grosfichier
  var pathGrosfichiers = []
  for(let fuuid in infoGrosfichiers) {
    const {err, fichier} = await pathConsignation.trouverPathFuuidExistant(fuuid)
    if(err) throw new Error("Erreur grosfichier: " + err)

    let infoFichier = infoGrosfichiers[fuuid]
    // let heureStr = infoFichier.heure
    // if(heureStr.length == 1) heureStr = '0' + heureStr
    //
    // const extension = 'mgs1'
    // let pathGrosfichier = path.join(repertoireBackup, heureStr, 'grosfichiers', `${fuuid}.${extension}`)

    debug("Ajout grosfichier %s", fichier)

    // Verifier le hachage des fichiers a inclure
    if(infoFichier.hachage) {
      const fonctionHash = infoFichier.hachage.split(':')[0]
      const hachageCalcule = await calculerHachageFichier(fichier, {fonctionHash});

      if(hachageCalcule !== infoFichier.hachage) {
        debug("Erreur verification hachage grosfichier\nCatalogue : %s\nCalcule : %s", infoFichier.hachage, hachageCalcule)
        throw `Erreur Hachage sur fichier : ${nomFichier}`
      } else {
        debug("Hachage grosfichier OK : %s => %s ", hachageCalcule, nomFichier)
      }
    } else {
      throw `Erreur Hachage absent sur fichier : ${nomFichier}`;
    }

    pathGrosfichiers.push(nomFichier);
  }
}

async function genererTarArchiveQuotidienne(pathConsignation, domaine, dateJour, fichiersInclure) {
  // Genere un fichier .tar d'une archive quotidienne avec tous les catalogues et transactions
  // - pathRepertoireArchive: str path du repertoire output de l'archive
  // - domaine: str sous-domaine du backup
  // - dateJour: Date() du contenu de fichier de backup
  // - securite: str '3.protege'
  // - fichiersInclure: list Path des fichiers a inclure en ordre

  const pathArchiveQuotidienneRepertoire = path.join(
    pathConsignation.consignationPathBackupArchives, 'quotidiennes', domaine)
  debug("Path repertoire archive quotidienne : %s", pathArchiveQuotidienneRepertoire)
  await new Promise((resolve, reject)=>{
    fs.mkdir(pathArchiveQuotidienneRepertoire, { recursive: true, mode: 0o770 }, err=>{
      if(err) return reject(err)
      resolve()
    })
  })

  const dateFormattee = formatterDateString(dateJour).slice(0, 8)  // Retirer heures
  var nomArchive = [domaine, dateFormattee + '.tar'].join('_')
  const pathArchiveQuotidienne = path.join(pathArchiveQuotidienneRepertoire, nomArchive)
  debug("Path archive quotidienne : %s", pathArchiveQuotidienne)

  const repertoireBackupQuotidien = pathConsignation.trouverPathBackupQuotidien(dateJour)
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

async function chargerBackupHoraire(pathConsignation, dateHeure, nomFichierCatalogue) {
  // Charger et verifier un backup horaire pour un domaine
  // - dateHeure: objet Date
  // - nomFichierCatalogue: str, nom du fichier de catalogue
  // - nomFichierTransactions: str, nom du fichier de transactions (e.g. DOMAINE_transactions_DATE_SECURITE.jsonl.xz)

  var repertoireBackup = pathConsignation.trouverPathBackupQuotidien(dateHeure)

  var heureStr = `${dateHeure.getUTCHours()}`
  // console.debug("HEURESTR init : %O", heureStr)
  if(heureStr.length == 1) heureStr = '0' + heureStr; // Ajouter 0 devant heure < 10
  let fichierCatalogue = path.join(heureStr, nomFichierCatalogue);

  // Verifier SHA512
  const pathCatalogue = path.join(repertoireBackup, fichierCatalogue)
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

  let pathTransactions = path.join(repertoireBackup, heureStr, catalogue.transactions_nomfichier);
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

// Genere un fichier de backup mensuel qui correspond au journal
async function traiterBackupAnnuel2(mq, traitementFichierBackup, pathConsignation, journal) {

  const {domaine, securite} = journal
  const anneeBackup = new Date(journal.annee * 1000)

  const pathRepertoireQuotidien = pathConsignation.trouverPathDomaineQuotidien(domaine)

  const fichiersInclure = []

  // Trier la liste des jours pour ajout dans le fichier .tar
  const listeJoursTriee = Object.keys(journal.fichiers_quotidien)
  listeJoursTriee.sort()
  debug("Liste jours tries : %O", listeJoursTriee)

  for(const idx in listeJoursTriee) {
    const jourStr = listeJoursTriee[idx]
    debug("Traitement jour %s", jourStr)
    let infoFichier = journal.fichiers_quotidien[jourStr]
    debug("Verifier fichier backup quotidien %s :\n%O", jourStr, infoFichier)

    let fichierArchive = infoFichier.archive_nomfichier

    // Verifier SHA512
    const pathFichierArchive = path.join(pathRepertoireQuotidien, fichierArchive)
    const hachageArchive = await calculerHachageFichier(pathFichierArchive)
    if(hachageArchive != infoFichier.archive_hachage) {
      throw new Error(`Fichier archive ${fichierArchive} ne correspond pas au hachage`)
    }

    fichiersInclure.push(fichierArchive)
  }

  // Sauvegarder journal annuel, sauvegarder en format .json.xz
  var resultat = await traitementFichierBackup.sauvegarderJournalAnnuel(journal)
  debug("Resultat preparation catalogue annuel : %O", resultat)
  const pathJournal = resultat.path
  const nomJournal = path.basename(pathJournal)
  fichiersInclure.unshift(nomJournal)  // Ajouter le journal comme premiere entree du .tar

  const pathRepertoireAnnuel = pathConsignation.trouverPathDomaineAnnuel(domaine)

  debug("Path repertoire archives \nannuelles : %s", pathRepertoireAnnuel)
  await new Promise((resolve, reject)=>{
    fs.mkdir(pathRepertoireAnnuel, { recursive: true, mode: 0o770 }, err=>{
      if(err) return reject(err);
      resolve();
    })
  })

  // Creer nom du fichier d'archive - se baser sur le nom du catalogue quotidien
  var nomArchive = [domaine, resultat.dateFormattee + '.tar'].join('_')
  const pathArchiveAnnuelle = path.join(pathRepertoireAnnuel, nomArchive)
  debug("Path archive annuelle : %s", pathArchiveAnnuelle)

  debug('Archive annuelle inclure : %O', fichiersInclure)

  // Creer nouvelle archive quotidienne
  await tar.c(
    {
      file: pathArchiveAnnuelle,
      cwd: pathRepertoireQuotidien,
    },
    fichiersInclure
  )

  // Calculer le hachage du fichier d'archive
  const hachageArchive = await calculerHachageFichier(pathArchiveAnnuelle)

  const informationArchive = {
    archive_hachage: hachageArchive,
    archive_nomfichier: nomArchive,
    annee: journal.annee,
    domaine: journal.domaine,
    securite: journal.securite,

    fichiersInclure,
    pathRepertoireBackup: pathRepertoireQuotidien,
  }

  return informationArchive

}

async function sauvegarderCatalogueQuotidien(pathConsignation, catalogue) {
  const {domaine, securite, jour} = catalogue

  const dateJournal = new Date(jour*1000)
  var repertoireBackup = pathConsignation.trouverPathBackupHoraire(dateJournal)

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
  var repertoireBackup = pathConsignation.trouverPathDomaineQuotidien(domaine)

  let year = dateJournal.getUTCFullYear();
  const dateFormattee = "" + year

  const nomFichier = domaine + "_catalogue_" + dateFormattee + ".json.xz";

  const fullPathFichier = path.join(repertoireBackup, nomFichier)

  // debug("Path fichier journal mensuel " + fullPathFichier);
  await sauvegarderLzma(fullPathFichier, catalogue)

  return {path: fullPathFichier, nomFichier, dateFormattee}
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

module.exports = {
  traiterFichiersBackup, traiterFichiersApplication,
  genererBackupQuotidien, genererBackupAnnuel,

  sauvegarderFichiersApplication, rotationArchiveApplication,
  sauvegarderCatalogueQuotidien, sauvegarderCatalogueAnnuel,
  traiterBackupQuotidien, sauvegarderLzma, linkGrosfichiersSousBackup,
}
