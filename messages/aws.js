const debug = require('debug')('millegrilles:fichiers:aws')
const fs = require('fs')
const path = require('path')
// const {PassThrough} = require('stream')
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

async function executerUploadFichier(
  mq, s3, pathConsignation, infoConsignationWebNoeud, reponseDechiffrageFichier, message,
  fuuid, mimetype, opts
) {
  // Dechiffrer fichier (tmp) pour upload
  if(!opts) opts = {}

  var cleFichier = null, iv = null
  try {
    var infoClePreview = reponseDechiffrageFichier.cles_par_fuuid[fuuid]
    iv = reponseDechiffrageFichier.iv
    var cleChiffree = infoClePreview.cle
    cleFichier = await mq.pki.decrypterAsymetrique(cleChiffree)

  } catch(err) {
    console.error("publierAwsS3 ERROR: Cle dechiffrage fichier refusee %O", err)
    mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'echec', progres: -1, err: ''+err}, 'evenement.fichiers.publicAwsS3')
    throw err
  }
  mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'dechiffrage', progres: 2}, 'evenement.fichiers.publicAwsS3')

  var fichierTemporaire = null
  try {
    fichierTemporaire = await dechiffrerTemporaire(pathConsignation, fuuid, 'dat', cleFichier, iv)
    debug("Fichier dechiffre sous : %O", fichierTemporaire)
    mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'upload', progres: 5}, 'evenement.fichiers.publicAwsS3')

    const metadata = {
      uuid: message.uuid,
      fuuid,
      mimetype,
    }
    if(opts.nom_fichier) {
      metadata.nom_fichier = opts.nom_fichier
    }
    if(opts.key_fichier) {
      metadata.key_fichier = opts.key_fichier
    }

    await uploaderFichier(s3, mq, message, infoConsignationWebNoeud, metadata, fichierTemporaire.path)
  } catch(err) {
    console.error("aws.publierAwsS3 Erreur upload fichier : %O", err)
    mq.emettreEvenement(
      {
        noeud_id: message.noeud_id,
        fuuid,
        etat: 'echec',
        progres: -1,
        err: 'aws.publierAwsS3 Erreur upload fichier : '+err
      },
      'evenement.fichiers.publicAwsS3'
    )
    throw err
  } finally {
    // Cleanup fichiers temporaires
    if(fichierTemporaire) fichierTemporaire.cleanup()
  }
}

function uploaderFichier(s3, mq, message, noeudConfig, metadata, pathFichier) {
  var fileStream = fs.createReadStream(pathFichier)

  // Creer un stream intermedaire pour calculer le progres
  // var calculProgresStream = new PassThrough()
  // var bytesLoaded = 0, bytesTotal = -1  // Total pour tracking
  var timerUpdate = null

  // Recuprer taille fichier
  fs.stat(pathFichier, (err, stat)=>{
    if(err) return console.error('aws.uploaderFichier: Erreur sur stat fichier %s : %O', pathFichier, err);
    debug("Taille fichier a uploader : %s", stat.size)
    bytesTotal = stat.size
  })

  // calculProgresStream.on('data', chunk=>{
  //   debug("Chunk size : %s", chunk.length)
  //   bytesLoaded += chunk.length
  //   if(!timerUpdate && bytesTotal > 0) {
  //     timerUpdate = setTimeout(_=>{
  //       timerUpdate = null
  //       const pctProgres = Math.round(bytesLoaded / bytesTotal * 94.0) + 5 // (94% alloue au transfert)
  //       debug("Progres %s", pctProgres)
  //       mq.emettreEvenement(
  //         {noeud_id: message.noeud_id, fuuid: metadata.fuuid, etat: 'upload', progres: pctProgres},
  //         'evenement.fichiers.publicAwsS3'
  //       )
  //     }, 5000)
  //   }
  // })
  // fileStream.pipe(calculProgresStream)

  fileStream.on('error', function(err) {
    console.log('File Error', err);
  });

  let dirFichier = noeudConfig.bucketDirfichier || ''

  var pathSurServeur = path.format({
    dir: dirFichier,
    name: metadata.key_fichier || metadata.fuuid,
  })

  var uploadParams = {
    Bucket: noeudConfig.bucketName,
    Key: pathSurServeur,
    Body: fileStream,
    ACL: 'public-read',
    ContentType: metadata.mimetype,
    CacheControl: 'public, max-age=604800, immutable',
    Metadata: metadata,
    // Metadata: {
    //   'uuid': message.uuid,
    //   'fuuid': message.fuuid,
    //   'nom_fichier': message.nom_fichier,
    // }
  }

  if(metadata.nom_fichier) {
    uploadParams.ContentDisposition = 'attachment; filename="' + metadata.nom_fichier + '"'
  }

  // call S3 to upload file to specified bucket
  var options = {partSize: 5 * 1024 * 1024, queueSize: 1}

  debug("AWS S3 upload params : %O", uploadParams)

  return new Promise((resolve, reject)=>{
    // // DUMMY UPLOAD
    // setTimeout(_=>{
    //   mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: metadata.fuuid, etat: 'upload', progres: 33}, 'evenement.fichiers.publicAwsS3')
    // }, 1000)  // Simuler upload de 4 secondes
    // setTimeout(_=>{
    //   mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: metadata.fuuid, etat: 'upload', progres: 87}, 'evenement.fichiers.publicAwsS3')
    // }, 2500)  // Simuler upload de 4 secondes
    // setTimeout(_=>{
    //   debug("DUMMY upload termine : %s", metadata.fuuid)
    //   resolve()
    // }, 4000)  // Simuler upload de 4 secondes
    // return
    // // DUMMY UPLOAD

    const managedUpload = s3.upload(uploadParams, options, function (err, data) {
      if (err) {
        console.error("aws.uploaderFichier: Error %O", err);
        return reject(err)
      }
      debug("Upload Success : %O", data);
      resolve(data)
      if(timerUpdate) {
        clearTimeout(timerUpdate)
      }
    })

    // Attacher listener d'evenements
    managedUpload.on('httpUploadProgress', progress => {
      //  progress : {
      //   loaded: 8174,
      //   total: 8174,
      //   part: 1,
      //   key: 'QME8SjhaCFySD9qBt1AikQ1U7WxieJY2xDg2JCMczJST/public/89122e80-4227-11eb-a00c-0bb29e75acbf'
      // }
      var total = progress.total || bytesTotal
      if(total) {
        const pctProgres = Math.round(progress.loaded / total * 94.0) + 5 // (94% alloue au transfert)
        debug("Upload progress (%s) : %O", pctProgres, progress)
        mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: metadata.fuuid, etat: 'upload', progres: pctProgres}, 'evenement.fichiers.publicAwsS3')
      }
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

  var reponseDechiffrageFichier = null
  try {
    const domaineActionPermission = message.permission['en-tete'].domaine
    reponseDechiffrageFichier = await mq.transmettreRequete(
      domaineActionPermission, message.permission, {noformat: true, attacherCertificat: true})
    debug("Reponse cle dechiffrage fichier : %O", reponseDechiffrageFichier)
  } catch(err) {
    console.error("publierAwsS3 ERROR: Information dechiffrage fichiers non disponible %O", err)
    mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'echec', progres: -1, err: ''+err}, 'evenement.fichiers.publicAwsS3')
    return
  }

  const s3 = preparerConnexionS3(infoConsignationWebNoeud, motDePasseAWSS3)

  // Determiner les fichiers a uploader
  try {
    await executerUploadFichier(
      mq, s3, pathConsignation, infoConsignationWebNoeud, reponseDechiffrageFichier, message,
      message.fuuid, message.mimetype, {nom_fichier: message.nom_fichier}
    )

    // Uploader preview si present
    if(message.fuuid_preview && message.mimetype_preview) {
      debug("Uploader preview du fichier")
      const key_fichier = message.fuuid + '_preview_1'
      await executerUploadFichier(
        mq, s3, pathConsignation, infoConsignationWebNoeud, reponseDechiffrageFichier, message,
        message.fuuid_preview, message.mimetype_preview, {key_fichier}
      )
    }

    // Uploader videos re-encodes si presents
    if(message.video) {
      for(const resolution in message.video) {
        const key_fichier = message.fuuid + '_video_' + resolution
        const {mimetype, fuuid} = message.video[resolution]
        debug("Uploader video re-encode %s = %s", resolution, fuuid)
        await executerUploadFichier(
          mq, s3, pathConsignation, infoConsignationWebNoeud, reponseDechiffrageFichier, message,
          fuuid, mimetype, {key_fichier}
        )
      }
    }

    mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'succes', progres: 100}, 'evenement.fichiers.publicAwsS3')
  } catch(err) {
    const msg = "publierAwsS3 ERROR: Erreur upload fichier uuid: " + message.uuid + "\n"
    console.error(msg + ": %O", err)
    mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'echec', progres: -1, err: msg+err}, 'evenement.fichiers.publicAwsS3')
  }
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


module.exports = {AWS_API_VERSION, PublicateurAWS}
