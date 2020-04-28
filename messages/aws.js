const fs = require('fs');
const path = require('path');
const S3 = require('aws-sdk/clients/s3');
const { DecrypterFichier, decrypterCleSecrete, getDecipherPipe4fuuid } = require('./crypto.js')
const { decrypterSymmetrique } = require('../util/cryptoUtils')
const { PathConsignation } = require('../util/traitementFichier');

const AWS_API_VERSION = '2006-03-01';

class PublicateurAWS {

  constructor(mq) {
    this.mq = mq;
    this.publierCollection.bind(this);

    const idmg = mq.pki.idmg;
    this.pathConsignation = new PathConsignation({idmg});
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
      this.publierCollection(routingKey, message, opts)}, ['commande.grosfichiers.publierCollection'], {operationLongue: true});
  }

  publierCollection(routingKey, message, opts) {
    // console.debug("AWS publicCollection Properties");
    // console.debug(opts.properties);

    var messageConfiguration = Object.assign({}, message);  // Copie message

    let promise = Promise.resolve();

    // console.debug("AWS message");
    // console.debug(message);
    if(messageConfiguration.credentials.secretAccessKeyChiffre) {
      // On doit commencer par dechiffrer le contenu protege (mots de passe, etc.)
      let secretAccessKeyChiffre = messageConfiguration.credentials.secretAccessKeyChiffre;
      let iv = messageConfiguration.credentials.iv;
      let cleSecrete = decrypterCleSecrete(messageConfiguration.credentials.cle);
      // console.debug("Secret access key: ");
      // console.debug(cleSecrete);

      promise = decrypterSymmetrique(secretAccessKeyChiffre, cleSecrete, iv);
    }

    promise.then(secretAccessKeyDecrypte=>{
      var secretAccessKey = messageConfiguration.credentials.secretAccessKey;
      if(secretAccessKeyDecrypte) {
        // Extraire la cle du contenu decrypte
        let jsonDict = JSON.parse(secretAccessKeyDecrypte);
        secretAccessKey = jsonDict.awsSecretAccessKey;
      }

      // console.debug("Secret access key AWS: " + secretAccessKey);

      let configurationAws = {
        apiVersion: AWS_API_VERSION,
        region: messageConfiguration.region,
        credentials: {
          accessKeyId: messageConfiguration.credentials.accessKeyId,
          secretAccessKey,
          region: messageConfiguration.credentials.region,
        }
      }

      // Connecter a Amazon S3
      const s3 = new S3(configurationAws);
      const fichiers = {};

      // Creer un dictionnaire de fichiers par fuuid pour retirer
      // les fichiers dejas presents sur le drive S3
      for(let idx in messageConfiguration.fuuidFichiers) {
        let fichier = messageConfiguration.fuuidFichiers[idx];
        fichiers[fichier.fuuid] = fichier;
      }

      // Demander la liste des fichiers du bucket; on ne veut pas re-uploader
      // les memes fichiers (consignation est immuable)
      var paramsListing = {
        Bucket: messageConfiguration.bucket,
        MaxKeys: 1000,
      }
      if(messageConfiguration.dirfichier) {
        paramsListing.Prefix = messageConfiguration.dirfichier;
      }

      return new Promise((resolve, reject)=>{
        listerFichiers(s3, paramsListing, fichiers, {resolve, reject});
      })
      .then(()=>{
        console.debug("Commencer upload AWS");
        // Commencer le telechargement
        const listeFichiers = Object.values(fichiers);

        uploaderFichier(
          s3, listeFichiers,
          {
              mq: this.mq,
              message: messageConfiguration,
              properties: opts.properties,
          }
        );
      });
    });

  }

}

function listerFichiers(s3, paramsListing, fichiers, promiseRR) {
  s3.listObjectsV2(paramsListing, (err, data)=>{
    if(err) {
      console.error("Erreur demande liste fichiers");
      promiseRR.reject(err);
    } else {
      // console.log("Listing fichiers bucket " + paramsListing.Bucket);
      for(let idx in data.Contents) {
        let contents = data.Contents[idx];

        let keyInfo = contents.Key.split('/');
        let nomFichier = keyInfo[keyInfo.length-1];
        let fuuid = nomFichier.split('.')[0];

        if(fuuid && fuuid !== '') {
          // console.log(contents);
          // console.log('fuuid: ' + fuuid);

          if(fichiers[fuuid]) {
            // console.log("Fichier " + fuuid + " existe deja");
            delete fichiers[fuuid];
          }
        }
      }

      if(data.IsTruncated) {
        // console.debug("Continuer listing");
        paramsListing.ContinuationToken = data.NextContinuationToken;
        listerFichiers(s3, paramsListing, fichiers, promiseRR);
      } else {
        promiseRR.resolve();  // Listing termine
      }
    }
  });
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
    let mimetype = fichier.mimetype;
    let file = this.pathConsignation.trouverPathLocal(fuuidFichier, false, {extension});

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
      ContentType: mimetype,
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
