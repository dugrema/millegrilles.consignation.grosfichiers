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
      console.debug("Generer backup quotidien");
      console.debug(message);

      try {
        const informationArchive = await genererBackupQuotidien(message.catalogue);

        // Finaliser le backup en retransmettant le journal comme transaction
        // de backup quotidien
        await this.mq.transmettreEnveloppeTransaction(message.catalogue, 'nouvelle.transaction');

        // Generer transaction pour journal mensuel. Inclue SHA512 et nom de l'archive quotidienne
        console.debug("Transmettre transaction informationArchive : ");
        console.debug(informationArchive);

        await this.mq.transmettreTransactionFormattee(informationArchive, 'millegrilles.domaines.Backup.archiveQuotidienneInfo');

      } catch (err) {
        console.error("Erreur creation backup quotidien");
        console.error(err);
      }

      console.debug("Backup quotidien termine");
      resolve();
    });

  }

  genererBackupMensuel(routingKey, message, opts) {
    return new Promise( async (resolve, reject) => {
      console.debug("Generer backup mensuel");
      console.debug(message);

      try {
        const informationArchive = await genererBackupMensuel(message.catalogue);

        console.debug("Information archive mensuelle:");
        console.debug(informationArchive);

        // Finaliser le backup en retransmettant le journal comme transaction
        // de backup quotidien
        await this.mq.transmettreEnveloppeTransaction(message.catalogue, 'nouvelle.transaction');

        // Generer transaction pour journal annuel. Inclue SHA512 et nom de l'archive mensuelle
        console.debug("Transmettre transaction informationArchive : ");
        console.debug(informationArchive);

        await this.mq.transmettreTransactionFormattee(informationArchive, 'millegrilles.domaines.Backup.archiveMensuelleInfo');

        console.debug("Info archive mensuelle transmise");

      } catch (err) {
        console.error("Erreur creation backup mensuel");
        console.error(err);
      }

      console.debug("Backup mensuel termine");
      resolve();
    });

  }

}

// Genere un fichier de backup quotidien qui correspond au journal
async function genererBackupQuotidien(journal) {

  const {domaine, securite} = journal;
  const jourBackup = new Date(journal.jour * 1000);
  console.debug("Domaine " + domaine + ", securite " + securite);
  console.debug(jourBackup);

  // Sauvegarder journal quotidien, sauvegarder en format .json.xz
  var resultat = await traitementFichier.sauvegarderJournalQuotidien(journal);
  const pathJournal = resultat.path;
  const nomJournal = path.basename(pathJournal);
  const pathRepertoireBackup = path.dirname(pathJournal);

  console.debug(`Path backup : ${pathRepertoireBackup}`);

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

  // Faire liste des fichiers a inclure.
  var fichiersInclure = `${nomJournal} */catalogues/${domaine}*.json.xz */transactions/${domaine}*.json.xz`

  if(journal.fuuid_grosfichiers) {
    // Aussi inclure le repertoire des grosfichiers
    fichiersInclure = `${fichiersInclure} */grosfichiers/*`
  }

  console.debug(`Fichiers inclure : ${fichiersInclure}`);

  const commandeBackup = spawn('/bin/sh', ['-c', `cd ${pathRepertoireBackup}; tar -jcf ${pathArchiveQuotidienne} ${fichiersInclure}`]);
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

      // Supprimer repertoire horaire
      fs.rmdir(pathRepertoireBackup, { recursive: true }, err=>{
        if(err) return reject(err);

        const informationArchive = {
          archive_sha512: sha512Archive,
          archive_nomfichier: nomArchive,
          jour: journal.jour,
          domaine: journal.domaine,
          securite: journal.securite,
        }

        return resolve(informationArchive);
      });

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
  console.debug("Domaine " + domaine + ", securite " + securite);
  console.debug(moisBackup);

  // Sauvegarder journal quotidien, sauvegarder en format .json.xz
  var resultat = await traitementFichier.sauvegarderJournalMensuel(journal);
  const pathJournal = resultat.pathJournal;
  const nomJournal = path.basename(pathJournal);

  console.debug(`Path journal mensuel : ${pathJournal}`);

  const pathArchives = pathConsignation.consignationPathBackupArchives;

  // Creer nom du fichier d'archive - se baser sur le nom du catalogue quotidien
  const nomArchive = nomJournal.replace('_catalogue', '').replace('.json', '.tar');
  const pathArchiveMensuelle = path.join(pathArchives, nomArchive);

  // Faire liste des fichiers a inclure.
  let year = moisBackup.getUTCFullYear();
  let month = moisBackup.getUTCMonth() + 1; if(month < 10) month = '0'+month;
  const moisStr = '' + year + month;

  const fichiersInclure = `${nomJournal} ${domaine}_${moisStr}??_${securite}.tar.xz`

  console.debug(`Archive mensuelle inclure : ${fichiersInclure}`);

  const commandeBackup = spawn('/bin/sh', ['-c', `cd ${pathArchives}; tar -jcf ${pathArchiveMensuelle} ${fichiersInclure}`]);
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
        }

        // Supprimer les archives quotidiennes correspondants au mois, journal

        console.debug("SHA512 archive mensuelle : " + sha512Archive);

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
