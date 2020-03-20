const fs = require('fs');
const path = require('path');
const S3 = require('aws-sdk/clients/s3');
const { pathConsignation } = require('../util/traitementFichier');

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
      // console.debug(message);
      await genererBackupQuotidien(message);

      console.debug("Backup quotidien termine");
      resolve();
    });

  }

}

async function genererBackupQuotidien(journal) {

  const {domaine, securite} = journal;
  const jourBackup = new Date(journal.jour * 1000);
  console.debug("Domaine " + domaine + ", securite " + securite);
  console.debug(jourBackup);

}

module.exports = {GestionnaireMessagesBackup};
