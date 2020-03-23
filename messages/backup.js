const fs = require('fs');
const path = require('path');
const S3 = require('aws-sdk/clients/s3');
const { spawn } = require('child_process');
const { traitementFichier, pathConsignation, utilitaireFichiers } = require('../util/traitementFichier');

class GestionnaireMessagesBackup {

  constructor(mq) {
    this.mq = mq;
    this.genererBackupQuotidien.bind(this);
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message, opts) => {
        // Retourner la promise pour rendre cette operation bloquante (longue duree)
        return this.genererBackupQuotidien(routingKey, message, opts)
      },
      ['commande.backup.genererBackupQuotidien'],
      {operationLongue: true}
    );

    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message, opts) => {
        // Retourner la promise pour rendre cette operation bloquante (longue duree)
        return this.genererBackupMensuel(routingKey, message, opts)
      },
      ['commande.backup.genererBackupMensuel'],
      {operationLongue: true}
    );

  }

  genererBackupQuotidien(routingKey, message, opts) {
    return new Promise( async (resolve, reject) => {
      // console.debug("Generer backup quotidien");
      // console.debug(message);

      try {
        const informationArchive = await genererBackupQuotidien(message.catalogue);
        const { fichiersInclure, pathRepertoireBackup } = informationArchive;
        delete informationArchive.fichiersInclure; // Pas necessaire pour la transaction
        delete informationArchive.pathRepertoireBackup; // Pas necessaire pour la transaction

        // Finaliser le backup en retransmettant le journal comme transaction
        // de backup quotidien
        await this.mq.transmettreEnveloppeTransaction(message.catalogue, 'nouvelle.transaction');

        // Generer transaction pour journal mensuel. Inclue SHA512 et nom de l'archive quotidienne
        // console.debug("Transmettre transaction informationArchive : ");
        // console.debug(informationArchive);

        await this.mq.transmettreTransactionFormattee(informationArchive, 'millegrilles.domaines.Backup.archiveQuotidienneInfo');

        // Effacer les fichiers transferes dans l'archive quotidienne
        await utilitaireFichiers.supprimerFichiers(fichiersInclure, pathRepertoireBackup);
        await utilitaireFichiers.supprimerRepertoiresVides(pathConsignation.consignationPathBackupHoraire);

      } catch (err) {
        console.error("Erreur creation backup quotidien");
        console.error(err);
      }

      // console.debug("Backup quotidien termine");
      resolve();
    });

  }

  genererBackupMensuel(routingKey, message, opts) {
    return new Promise( async (resolve, reject) => {
      // console.debug("Generer backup mensuel");
      // console.debug(message);

      try {
        const informationArchive = await genererBackupMensuel(message.catalogue);

        // console.debug("Information archive mensuelle:");
        // console.debug(informationArchive);

        // Finaliser le backup en retransmettant le journal comme transaction
        // de backup quotidien
        await this.mq.transmettreEnveloppeTransaction(message.catalogue, 'nouvelle.transaction');

        // Generer transaction pour journal annuel. Inclue SHA512 et nom de l'archive mensuelle
        // console.debug("Transmettre transaction informationArchive : ");
        // console.debug(informationArchive);
        const {nomJournal, fichiersInclure} = informationArchive;

        delete informationArchive.fichiersInclure;
        delete informationArchive.nomJournal;

        await this.mq.transmettreTransactionFormattee(informationArchive, 'millegrilles.domaines.Backup.archiveMensuelleInfo');

        // console.debug("Info archive mensuelle transmise, nettoyage des fichiers locaux");
        await utilitaireFichiers.supprimerFichiers(fichiersInclure, pathConsignation.consignationPathBackupArchives);

      } catch (err) {
        console.error("Erreur creation backup mensuel");
        console.error(err);
      }

      // console.debug("Backup mensuel termine");
      resolve();
    });

  }

}

// Genere un fichier de backup quotidien qui correspond au journal
async function genererBackupQuotidien(journal) {

  const {domaine, securite} = journal;
  const jourBackup = new Date(journal.jour * 1000);
  // console.debug("Domaine " + domaine + ", securite " + securite);
  // console.debug(jourBackup);

  // Sauvegarder journal quotidien, sauvegarder en format .json.xz
  var resultat = await traitementFichier.sauvegarderJournalQuotidien(journal);
  const pathJournal = resultat.path;
  const nomJournal = path.basename(pathJournal);
  const pathRepertoireBackup = path.dirname(pathJournal);

  // console.debug(`Path backup : ${pathRepertoireBackup}`);

  const pathArchive = pathConsignation.consignationPathBackupArchives;
  await new Promise((resolve, reject)=>{
    fs.mkdir(pathArchive, { recursive: true, mode: 0o770 }, err=>{
      if(err) reject(err);
      else resolve();
    })
  })

  // Creer nom du fichier d'archive - se baser sur le nom du catalogue quotidien
  var nomArchive = nomJournal.replace('_catalogue', '').replace('.json', '.tar');
  const pathArchiveQuotidienne = path.join(pathArchive, `${nomArchive}`)

  // Faire liste des fichiers de catalogue et transactions a inclure.
  var fichiersInclure = [nomJournal];

  for(let heureStr in journal.fichiers_horaire) {
    let infoFichier = journal.fichiers_horaire[heureStr];
    if(heureStr.length == 1) heureStr = '0' + heureStr; // Ajouter 0 devant heure < 10

    let fichierCatalogue = path.join(heureStr, 'catalogues', infoFichier.catalogue_nomfichier);
    let fichierTransactions = path.join(heureStr, 'transactions', infoFichier.transactions_nomfichier);

    // Verifier SHA512
    const sha512Catalogue = await utilitaireFichiers.calculerSHAFichier(path.join(pathRepertoireBackup, fichierCatalogue));
    if(sha512Catalogue != infoFichier.catalogue_sha512) {
      throw `Fichier catalogue ${fichierCatalogue} ne correspond pas au SHA512`;
    }

    const sha512Transactions = await utilitaireFichiers.calculerSHAFichier(path.join(pathRepertoireBackup, fichierTransactions));
    if(sha512Transactions != infoFichier.transactions_sha512) {
      throw `Fichier catalogue ${fichierCatalogue} ne correspond pas au SHA512`;
    }

    fichiersInclure.push(fichierCatalogue);
    fichiersInclure.push(fichierTransactions);
  }

  // Faire liste des grosfichiers au besoin
  if(journal.fuuid_grosfichiers) {
    // Aussi inclure le repertoire des grosfichiers
    // fichiersInclureStr = `${fichiersInclureStr} */grosfichiers/*`

    for(let fuuid in journal.fuuid_grosfichiers) {
      let infoFichier = journal.fuuid_grosfichiers[fuuid];
      let heureStr = infoFichier.heure;
      if(heureStr.length == 1) heureStr = '0' + heureStr;

      let extension = infoFichier.extension;
      if(infoFichier.securite == '3.protege' || infoFichier.securite == '4.secure') {
        extension = 'mgs1';
      }
      let nomFichier = path.join(heureStr, 'grosfichiers', `${fuuid}.${extension}`);

      // Verifier le SHA si disponible
      if(infoFichier.sha256) {
        const sha256Calcule = await utilitaireFichiers.calculerSHAFichier(
          path.join(pathRepertoireBackup, nomFichier), {fonctionHash: 'sha256'});

        if(sha256Calcule != infoFichier.sha256) {
          throw `Erreur SHA256 sur fichier : ${nomFichier}`
        }
      } else if(infoFichier.sha512) {
        const sha512Calcule = await utilitaireFichiers.calculerSHAFichier(
          path.join(pathRepertoireBackup, nomFichier));

        if(sha512Calcule != infoFichier.sha512) {
          throw `Erreur SHA512 sur fichier : ${nomFichier}`;
        }
      }

      fichiersInclure.push(nomFichier);
    }
  }

  var fichiersInclureStr = fichiersInclure.join(' ');


  // console.debug(`Fichiers inclure : ${fichiersInclure}`);

  const commandeBackup = spawn('/bin/sh', ['-c', `cd ${pathRepertoireBackup}; tar -jcf ${pathArchiveQuotidienne} ${fichiersInclureStr}`]);
  commandeBackup.stderr.on('data', data=>{
    console.error(`tar backup quotidien: ${data}`);
  })

  const resultatTar = await new Promise(async (resolve, reject) => {
    commandeBackup.on('close', async code =>{
      if(code != 0) {
        return reject(code);
      }

      // Calculer le SHA512 du fichier d'archive
      const sha512Archive = await utilitaireFichiers.calculerSHAFichier(pathArchiveQuotidienne);

      const informationArchive = {
        archive_sha512: sha512Archive,
        archive_nomfichier: nomArchive,
        jour: journal.jour,
        domaine: journal.domaine,
        securite: journal.securite,

        fichiersInclure,
        pathRepertoireBackup,
      }

      return resolve(informationArchive);

    })
  })
  .catch(err=>{
    return({err});
  });

  if(resultatTar.err) {
    throw err;
  }

  return resultatTar;

}

// Genere un fichier de backup mensuel qui correspond au journal
async function genererBackupMensuel(journal) {

  const {domaine, securite} = journal;
  const moisBackup = new Date(journal.mois * 1000);
  // console.debug("Domaine " + domaine + ", securite " + securite);
  // console.debug(moisBackup);

  // Sauvegarder journal quotidien, sauvegarder en format .json.xz
  var resultat = await traitementFichier.sauvegarderJournalMensuel(journal);
  const pathJournal = resultat.pathJournal;
  const nomJournal = path.basename(pathJournal);

  // console.debug(`Path journal mensuel : ${pathJournal}`);

  const pathArchives = pathConsignation.consignationPathBackupArchives;

  // Creer nom du fichier d'archive - se baser sur le nom du catalogue quotidien
  const nomArchive = nomJournal.replace('_catalogue', '').replace('.json', '.tar');
  const pathArchiveMensuelle = path.join(pathArchives, nomArchive);

  // Faire liste des fichiers a inclure.
  let year = moisBackup.getUTCFullYear();
  let month = moisBackup.getUTCMonth() + 1; if(month < 10) month = '0'+month;
  const moisStr = '' + year + month;

  // const fichiersInclure = `${nomJournal} ${domaine}_${moisStr}??_${securite}.tar.xz`
  var fichiersInclure = [nomJournal];
  for(let jour in journal.fichiers_quotidien) {
    let infoArchive = journal.fichiers_quotidien[jour];
    let sha512_archive = infoArchive.archive_sha512;
    let nomFichier = infoArchive.archive_nomfichier;

    let pathFichierArchive = path.join(pathArchives, nomFichier);
    const sha512Archive = await utilitaireFichiers.calculerSHAFichier(pathFichierArchive);
    if(sha512Archive != sha512_archive) {
      throw `SHA512 archive ${nomFichier} est incorrect, backup annule`;
    }
    // console.debug(`Reducing fichier ${nomFichier}, SHA512: ${sha512Archive}`);
    // console.debug(fichiersInclure);
    fichiersInclure.push(nomFichier);
  }

  // console.debug("Array fichiersInclure");
  // console.debug(fichiersInclure);

  let fichiersInclureStr = fichiersInclure.join(' ');

  // console.debug(`Archive mensuelle inclure : ${fichiersInclure}`);

  const commandeBackup = spawn('/bin/sh', ['-c', `cd ${pathArchives}; tar -jcf ${pathArchiveMensuelle} ${fichiersInclureStr}`]);
  commandeBackup.stderr.on('data', data=>{
    console.error(`tar backup mensuel: ${data}`);
  })

  const resultatTar = await new Promise(async (resolve, reject) => {
    commandeBackup.on('close', async code =>{
      if(code != 0) {
        return reject(code);
      }

      // Calculer le SHA512 du fichier d'archive
      try {
        const sha512Archive = await utilitaireFichiers.calculerSHAFichier(pathArchiveMensuelle);
        const informationArchive = {
          archive_sha512: sha512Archive,
          archive_nomfichier: nomArchive,
          catalogue_sha512: resultat.sha512,
          mois: journal.mois,
          domaine: journal.domaine,
          securite: journal.securite,

          nomJournal,
          fichiersInclure,
        }

        // Supprimer les archives quotidiennes correspondants au mois, journal

        // console.debug("SHA512 archive mensuelle : " + sha512Archive);

        return resolve(informationArchive);
      } catch(err) {
        reject(err);
      }

    })
  })
  .catch(err=>{
    return({err});
  });

  if(resultatTar.err) {
    throw err;
  }

  return resultatTar;

}

module.exports = {GestionnaireMessagesBackup};
