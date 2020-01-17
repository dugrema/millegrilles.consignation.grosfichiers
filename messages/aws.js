const fs = require('fs');
const path = require('path');
const S3 = require('aws-sdk/clients/s3');
const { DecrypterFichier, decrypterCleSecrete, getDecipherPipe4fuuid } = require('./crypto.js')
const { pathConsignation } = require('../util/traitementFichier');

const AWS_API_VERSION = '2006-03-01';

class PublicateurAWS {

  constructor(mq) {
    this.mq = mq;
    this.publierCollection.bind(this);
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
      this.publierCollection(routingKey, message, opts)}, ['commande.grosfichiers.publierCollection']);
  }

  publierCollection(routingKey, message, opts) {
    console.debug("AWS publicCollection Properties");
    console.debug(opts.properties);

    var messageConfiguration = Object.assign({}, message);  // Copie message
    if(message.contenuChiffre) {
      // On doit commencer par dechiffrer le contenu protege (mots de passe, etc.)
      let cleSecrete = decrypterCleSecrete(message.cleSecreteChiffree);
    }

    let configurationAws = {
      apiVersion: AWS_API_VERSION,
      region: messageConfiguration.region,
      credentials: {
        accessKeyId: messageConfiguration.credentials.accessKeyId,
        secretAccessKey: messageConfiguration.credentials.secretAccessKey,
        region: messageConfiguration.credentials.region,
      }
    }

    // Connecter a Amazon S3
    const s3 = new S3(configurationAws);
    const fichiers = messageConfiguration.fuuidFichiers.slice();  // Copier liste fichiers

    // Commencer le telechargement
    uploaderFichier(
      s3, fichiers,
      {
          mq: this.mq,
          message: messageConfiguration,
          properties: opts.properties,
      }
    );

  }

}

function uploaderFichier(s3, fichiers, msg) {
  if(fichiers.length === 0) {
    console.debug("Batch upload AWS termine");

    // Transmettre reponse a la commande d'upload
    if(msg.properties && msg.properties.replyTo && msg.properties.correlationId) {
      console.debug("Transmettre message de reponse pour transfert AWS");
      let reponseUpload = {
        uuid_source_figee: msg.message.uuid_source_figee,
        uuid_collection_figee: msg.message.uuid_collection_figee,
      }
      msg.mq.transmettreReponse(
        reponseUpload,
        msg.properties.replyTo,
        msg.properties.correlationId
      );

    }

  } else {
    let fichier = fichiers.pop();
    let fuuidFichier = fichier.fuuid;
    let extension = fichier.extension;
    let file = pathConsignation.trouverPathLocal(fuuidFichier, false, {extension});

    var fileStream = fs.createReadStream(file);
    fileStream.on('error', function(err) {
      console.log('File Error', err);
    });

    let dirFichier = msg.message.dirfichier || '';

    var pathSurServeur = path.format({
      dir: dirFichier,
      name: fuuidFichier,
      ext: '.'+extension
    })

    var uploadParams = {
      Bucket: msg.message.bucket,
      Key: pathSurServeur,
      Body: fileStream,
      ACL: 'public-read',
    };

    // call S3 to retrieve upload file to specified bucket
    s3.upload (uploadParams, function (err, data) {
      if (err) {
        console.log("Error", err);
        return;
      }

      if (data) {
        console.log("Upload Success", data.Location);
        console.debug(data);
      }

      uploaderFichier(s3, fichiers, msg);  // Continuer
    });

  }
}

module.exports = {AWS_API_VERSION, PublicateurAWS}
