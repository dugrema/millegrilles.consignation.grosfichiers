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
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, properties)=>{
      this.publierCollection(routingKey, message, properties)}, ['commande.grosfichiers.publierCollection']);
  }

  publierCollection(routingKey, message, properties) {

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
          properties,
      }
    );

  }

}

function uploaderFichier(s3, fichiers, msg) {
  if(fichiers.length === 0) {
    console.debug("Batch upload AWS termine");

    // Transmettre reponse a la commande d'upload


  } else {
    let fichier = fichiers.pop();
    let fuuidFichier = fichier.fuuid;
    let extension = fichier.extension;
    let file = pathConsignation.trouverPathLocal(fuuidFichier, false, {extension});

    var fileStream = fs.createReadStream(file);
    fileStream.on('error', function(err) {
      console.log('File Error', err);
    });

    var pathSurServeur = path.format({name: fuuidFichier, ext: '.'+extension})

    var uploadParams = {
      Bucket: msg.message.bucket,
      Key: pathSurServeur,
      Body: fileStream
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
