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
      }, ['commande.backup.genererBackupQuotidien']
    );
  }

  genererBackupQuotidien(routingKey, message, opts) {
    return new Promise( (resolve, reject) => {
      console.debug("Generer backup quotidien");
      resolve();
    });

  }

}

module.exports = {GestionnaireMessagesBackup};
