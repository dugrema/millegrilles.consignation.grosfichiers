const fs = require('fs');
const path = require('path');
const S3 = require('aws-sdk/clients/s3');
const { spawn } = require('child_process');
const { traitementFichier, pathConsignation } = require('../util/traitementFichier');

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
  }

  genererBackupQuotidien(routingKey, message, opts) {
    return new Promise( async (resolve, reject) => {
      console.debug("Generer backup quotidien");
      console.debug(message);

      try {
        await genererBackupQuotidien(message.catalogue);

        // Finaliser le backup en retransmettant le journal comme transaction
        // de backup quotidien
        await this.mq.transmettreEnveloppeTransaction(message.catalogue, 'nouvelle.transaction');

      } catch (err) {
        console.error("Erreur creation backup quotidien");
        console.error(err);
      }

      console.debug("Backup quotidien termine");
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

  const promiseTar = new Promise((resolve, reject) => {
    commandeBackup.on('close', code =>{
      if(code != 0) {
        return reject(code);
      }

      // Supprimer repertoire horaire
      fs.rmdir(pathRepertoireBackup, { recursive: true }, err=>{
        if(err) return reject(err);

        return resolve();
      });

    })
  });

  await promiseTar;

}

module.exports = {GestionnaireMessagesBackup};
