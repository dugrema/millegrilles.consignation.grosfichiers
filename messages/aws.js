const debug = require('debug')('millegrilles:fichiers:aws')
const fs = require('fs')
const path = require('path')
const S3 = require('aws-sdk/clients/s3')
const { DecrypterFichier, decrypterCleSecrete, getDecipherPipe4fuuid } = require('./crypto.js')
const { decrypterSymmetrique } = require('../util/cryptoUtils')
const { PathConsignation } = require('../util/traitementFichier')
const { dechiffrerTemporaire } = require('../util/traitementMedia')

const AWS_API_VERSION = '2006-03-01'

class PublicateurAWS {

  constructor(mq) {
    this.mq = mq

    const idmg = mq.pki.idmg
    this.pathConsignation = new PathConsignation({idmg})
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel()
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
      return publierAwsS3(this.mq, this.pathConsignation, routingKey, message, opts)}, ['commande.fichiers.publierAwsS3'], {operationLongue: true})
  }

  // publierCollection(routingKey, message, opts) {
  //   // console.debug("AWS publicCollection Properties");
  //   // console.debug(opts.properties);
  //
  //   var messageConfiguration = Object.assign({}, message)  // Copie message
  //
  //   let promise = Promise.resolve()
  //
  //   // console.debug("AWS message");
  //   // console.debug(message);
  //   if(messageConfiguration.credentials.secretAccessKeyChiffre) {
  //     // On doit commencer par dechiffrer le contenu protege (mots de passe, etc.)
  //     let secretAccessKeyChiffre = messageConfiguration.credentials.secretAccessKeyChiffre;
  //     let iv = messageConfiguration.credentials.iv;
  //     let cleSecrete = decrypterCleSecrete(messageConfiguration.credentials.cle);
  //     // console.debug("Secret access key: ");
  //     // console.debug(cleSecrete);
  //
  //     promise = decrypterSymmetrique(secretAccessKeyChiffre, cleSecrete, iv);
  //   }
  //
  //   promise.then(secretAccessKeyDecrypte=>{
  //     var secretAccessKey = messageConfiguration.credentials.secretAccessKey;
  //     if(secretAccessKeyDecrypte) {
  //       // Extraire la cle du contenu decrypte
  //       let jsonDict = JSON.parse(secretAccessKeyDecrypte);
  //       secretAccessKey = jsonDict.awsSecretAccessKey;
  //     }
  //
  //     // console.debug("Secret access key AWS: " + secretAccessKey);
  //
  //     let configurationAws = {
  //       apiVersion: AWS_API_VERSION,
  //       region: messageConfiguration.region,
  //       credentials: {
  //         accessKeyId: messageConfiguration.credentials.accessKeyId,
  //         secretAccessKey,
  //         region: messageConfiguration.credentials.region,
  //       }
  //     }
  //
  //     // Connecter a Amazon S3
  //     const s3 = new S3(configurationAws);
  //     const fichiers = {};
  //
  //     // Creer un dictionnaire de fichiers par fuuid pour retirer
  //     // les fichiers dejas presents sur le drive S3
  //     for(let idx in messageConfiguration.fuuidFichiers) {
  //       let fichier = messageConfiguration.fuuidFichiers[idx];
  //       fichiers[fichier.fuuid] = fichier;
  //     }
  //
  //     // Demander la liste des fichiers du bucket; on ne veut pas re-uploader
  //     // les memes fichiers (consignation est immuable)
  //     var paramsListing = {
  //       Bucket: messageConfiguration.bucket,
  //       MaxKeys: 1000,
  //     }
  //     if(messageConfiguration.dirfichier) {
  //       paramsListing.Prefix = messageConfiguration.dirfichier;
  //     }
  //
  //     return new Promise((resolve, reject)=>{
  //       listerFichiers(s3, paramsListing, fichiers, {resolve, reject});
  //     })
  //     .then(()=>{
  //       console.debug("Commencer upload AWS");
  //       // Commencer le telechargement
  //       const listeFichiers = Object.values(fichiers);
  //
  //       uploaderFichier(
  //         s3, listeFichiers,
  //         {
  //             mq: this.mq,
  //             message: messageConfiguration,
  //             properties: opts.properties,
  //         }
  //       );
  //     });
  //   });
  //
  // }

}

// function listerFichiers(s3, paramsListing, fichiers) {
//   return new Promise( (resolve, reject)=>{
//     s3.listObjectsV2(paramsListing, async (err, data)=>{
//       if(err) {
//         console.error("aws.listerFichiers: Erreur demande liste fichiers");
//         return reject(err)
//       } else {
//         // console.log("Listing fichiers bucket " + paramsListing.Bucket);
//         for(let idx in data.Contents) {
//           let contents = data.Contents[idx];
//
//           let keyInfo = contents.Key.split('/');
//           let nomFichier = keyInfo[keyInfo.length-1];
//           let fuuid = nomFichier.split('.')[0];
//
//           if(fuuid && fuuid !== '') {
//             // console.log(contents);
//             // console.log('fuuid: ' + fuuid);
//
//             if(fichiers[fuuid]) {
//               // console.log("Fichier " + fuuid + " existe deja");
//               delete fichiers[fuuid];
//             }
//           }
//         }
//
//         if(data.IsTruncated) {
//           // console.debug("Continuer listing");
//           paramsListing.ContinuationToken = data.NextContinuationToken;
//           await listerFichiers(s3, paramsListing, fichiers);
//         } else {
//           promiseRR.resolve();  // Listing termine
//         }
//       }
//     })
//   })
// }

function uploaderFichier(s3, mq, noeudConfig, message, pathFichier) {
  let fuuidFichier = message.fuuid
  let extension = message.extension
  let mimetype = message.mimetype

  var fileStream = fs.createReadStream(pathFichier)
  fileStream.on('error', function(err) {
    console.log('File Error', err);
  });

  let dirFichier = noeudConfig.bucketDirfichier || ''

  var pathSurServeur = path.format({
    dir: dirFichier,
    name: fuuidFichier,
    // ext: '.'+extension
  })

  var uploadParams = {
    Bucket: noeudConfig.bucketName,
    Key: pathSurServeur,
    Body: fileStream,
    ACL: 'public-read',
    ContentType: mimetype,
    CacheControl: 'public, max-age=604800, immutable',
    ContentDisposition: 'attachment; filename="' + message.nom_fichier + '"',
    Metadata: {
      'uuid': message.uuid,
      'fuuid': message.fuuid,
      'nom_fichier': message.nom_fichier,
    }
  }

  // call S3 to upload file to specified bucket
  var options = {partSize: 5 * 1024 * 1024, queueSize: 1}

  debug("AWS S3 upload params : %O", uploadParams)

  return new Promise((resolve, reject)=>{
    // DUMMY UPLOAD
    // setTimeout(_=>{
    //   debug("DUMMY upload termine")
    //   resolve()
    // }, 4000)  // Simuler upload de 4 secondes

    const managedUpload = s3.upload(uploadParams, options, function (err, data) {
      if (err) {
        console.error("aws.uploaderFichier: Error %O", err);
        return reject(err)
      }
      debug("Upload Success : %O", data);
      resolve(data)
    })

    // Attacher listener d'evenements
    managedUpload.on('httpUploadProgress', progress => {
      //  progress : {
      //   loaded: 8174,
      //   total: 8174,
      //   part: 1,
      //   key: 'QME8SjhaCFySD9qBt1AikQ1U7WxieJY2xDg2JCMczJST/public/89122e80-4227-11eb-a00c-0bb29e75acbf'
      // }
      debug("Upload progress : %O", progress)
      const pctProgres = Math.round(progress.loaded / progress.total * 94.0) + 5 // (94% alloue au transfert)
      mq.emettreEvenement({fuuid: message.fuuid, etat: 'upload', progres: pctProgres}, 'evenement.fichiers.publicAwsS3')
    })
  })

}

async function publierAwsS3(mq, pathConsignation, routingKey, message, opts) {
  if(!opts) opts = {}
  debug("Commande publicAwsS3 : %O", message)
  mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'debut', progres: 1}, 'evenement.fichiers.publicAwsS3')

  // Recuperer information noeud, info dechiffrage mot de passe AWS S3
  const noeudId = message.noeud_id
  var infoConsignationWebNoeud = null
  try {
    const domaineActionInfoNoeud = 'Topologie.infoNoeud'
    const reponseNoeud = await mq.transmettreRequete(domaineActionInfoNoeud, {noeud_id: noeudId})
    infoConsignationWebNoeud = reponseNoeud.consignation_web
    debug("publierAwsS3: Information noeud : %O", reponseNoeud)
  } catch(err) {
    console.error("publierAwsS3 ERROR: Information noeud (topologie) non disponible %O", err)
    mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'echec', progres: -1, err: ''+err}, 'evenement.fichiers.publicAwsS3')
    return
  }

  var motDePasseAWSS3 = null
  try {
    // Dechiffrer mot de passe AWS S3
    const domaineActionMotdepasse = 'Topologie.permissionDechiffrage'
    const credentialsSecretAccessKey = infoConsignationWebNoeud.credentialsSecretAccessKey
    const identificateurs_document = credentialsSecretAccessKey.identificateurs_document
    const reponseCleSecrete = await mq.transmettreRequete(
      domaineActionMotdepasse, {identificateurs_document}, {attacherCertificat: true})
    // debug("publierAwsS3: Cle secrete chiffree pour secret access key : %O", reponseCleSecrete)

    const secretChiffre = credentialsSecretAccessKey.secret_chiffre
    motDePasseAWSS3 = await mq.pki.dechiffrerContenuAsymetric(reponseCleSecrete.cle, reponseCleSecrete.iv, secretChiffre)
    motDePasseAWSS3 = JSON.parse(motDePasseAWSS3)  // Enlever wrapping (")

    // debug("publierAwsS3: Mot de passe (secret access key) : %s", motDePasseAWSS3)
  } catch(err) {
    console.error("publierAwsS3 ERROR: Information dechiffrage mot de passe AWSS3 non disponible %O", err)
    mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'echec', progres: -1, err: ''+err}, 'evenement.fichiers.publicAwsS3')
    return
  }

  // Dechiffrer fichier (tmp) pour upload
  var reponseDechiffrageFichier = null, cleFichier = null
  try {
    const domaineActionPermission = message.permission['en-tete'].domaine
    reponseDechiffrageFichier = await mq.transmettreRequete(
      domaineActionPermission, message.permission, {noformat: true, attacherCertificat: true})
    debug("Reponse cle dechiffrage fichier : %O", reponseDechiffrageFichier)

    const cleChiffree = reponseDechiffrageFichier.cle
    cleFichier = await mq.pki.decrypterAsymetrique(cleChiffree)

  } catch(err) {
    console.error("publierAwsS3 ERROR: Cle dechiffrage fichier refusee %O", err)
    mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'echec', progres: -1, err: ''+err}, 'evenement.fichiers.publicAwsS3')
    return
  }
  mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'dechiffrage', progres: 2}, 'evenement.fichiers.publicAwsS3')

  var fichierTemporaire = null
  try {
    const fuuid = message.fuuid
    fichierTemporaire = await dechiffrerTemporaire(pathConsignation, fuuid, 'dat', cleFichier, reponseDechiffrageFichier.iv)
    debug("Fichier dechiffre sous : %O", fichierTemporaire)
    mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'upload', progres: 5}, 'evenement.fichiers.publicAwsS3')

    const s3 = preparerConnexionS3(infoConsignationWebNoeud, motDePasseAWSS3)
    await uploaderFichier(s3, mq, infoConsignationWebNoeud, message, fichierTemporaire.path)
  } catch(err) {
    console.error("aws.publierAwsS3 Erreur upload fichier : %O", err)
    mq.emettreEvenement(
      {
        noeud_id: message.noeud_id,
        fuuid: message.fuuid, etat: 'echec', progres: -1,
        err: 'aws.publierAwsS3 Erreur upload fichier : '+err
      },
      'evenement.fichiers.publicAwsS3'
    )
    return
  } finally {
    // Cleanup fichiers temporaires
    if(fichierTemporaire) fichierTemporaire.cleanup()
  }

  mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'succes', progres: 100}, 'evenement.fichiers.publicAwsS3')
}

function preparerConnexionS3(noeudConfiguration, secretAccessKey) {
  /** Prepare connexion avec Amazon Web Services S3 **/
  let configurationAws = {
    apiVersion: AWS_API_VERSION,
    region: noeudConfiguration.bucketRegion,
    credentials: {
      accessKeyId: noeudConfiguration.credentialsAccessKeyId,
      secretAccessKey: secretAccessKey,
    },
  }
  // debug("Configuration credentials AWS S3 : %O", configurationAws)

  const s3 = new S3(configurationAws)
  return s3
}

// async function uploadFichierAWSS3(params, secretAccessKey, pathFichier) {
//   const credentials = new Credentials({
//     accessKeyId: messageConfiguration.credentials.accessKeyId,
//     secretAccessKey,
//   })
//   let configurationAws = {
//     apiVersion: AWS_API_VERSION,
//     region: messageConfiguration.region,
//     credentials,
//   }
//
//   // Connecter a Amazon S3
//   const s3 = new S3(configurationAws);
//
//   uploaderFichier(
//     s3, listeFichiers,
//     {
//         mq: this.mq,
//         message: messageConfiguration,
//         properties: opts.properties,
//     }
//   )
//
// }


module.exports = {AWS_API_VERSION, PublicateurAWS}
