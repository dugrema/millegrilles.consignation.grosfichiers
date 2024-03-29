const debug = require('debug')('millegrilles:fichiers:awss3')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const S3 = require('aws-sdk/clients/s3')

const { trouverExtension, trouverMimetype } = require('../util/traitementFichier')
const { preparerPublicationRepertoire } = require('./publierUtils')
const { dechiffrerDocumentAvecMq } = require('@dugrema/millegrilles.nodejs/src/chiffrage')

const AWS_API_VERSION = '2006-03-01',
      L2PRIVE = '2.prive'

async function preparerConnexionS3(mq, bucketRegion, credentialsAccessKeyId, secretKeyInfo) {
  /** Prepare connexion avec Amazon Web Services S3 **/
  debug("Preparer connexion : %s, credsSecret chiffre : %O", bucketRegion, secretKeyInfo)

  // // Dechiffrer la cle secrete AWS S3 (secretAccessKey)
  // const credsBytes = multibase.decode(secretKeyInfo.secretAccessKey)
  // const hachage_bytes = await hacher(credsBytes, {encoding: 'base58btc'})
  // const requeteCle = {
  //   liste_hachage_bytes: [hachage_bytes],
  //   permission: secretKeyInfo.permission,
  // }
  // const domaineActionCle = 'MaitreDesCles.dechiffrage'
  // const reponseDemandeCle = await mq.transmettreRequete(
  //   domaineActionCle, requeteCle, {attacherCertificat: true})
  // const infoCleRechiffree = reponseDemandeCle.cles[hachage_bytes]
  // const secretAccessKey = await dechiffrerDocument(
  //   credsBytes, infoCleRechiffree, mq.pki.cleForge, {nojson: true})
  const permission = secretKeyInfo.permission
  const secretAccessKey = await dechiffrerDocumentAvecMq(
    mq, secretKeyInfo.secretAccessKey, {permission, nojson: true})

  let configurationAws = {
    apiVersion: AWS_API_VERSION,
    region: bucketRegion,
    credentials: {
      accessKeyId: credentialsAccessKeyId,
      secretAccessKey: secretAccessKey,
    },
  }

  return new S3(configurationAws)
}

// class PublicateurAWS {
//
//   constructor(mq) {
//     this.mq = mq
//
//     const idmg = mq.pki.idmg
//     this.pathConsignation = new PathConsignation({idmg})
//   }
//
//   // Appele lors d'une reconnexion MQ
//   on_connecter() {
//     this.enregistrerChannel()
//   }
//
//   enregistrerChannel() {
//     this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message, opts)=>{
//       return publierAwsS3(this.mq, this.pathConsignation, routingKey, message, opts)}, ['commande.fichiers.publierAwsS3'], {operationLongue: true})
//   }
//
//   // publierCollection(routingKey, message, opts) {
//   //   // console.debug("AWS publicCollection Properties");
//   //   // console.debug(opts.properties);
//   //
//   //   var messageConfiguration = Object.assign({}, message)  // Copie message
//   //
//   //   let promise = Promise.resolve()
//   //
//   //   // console.debug("AWS message");
//   //   // console.debug(message);
//   //   if(messageConfiguration.credentials.secretAccessKeyChiffre) {
//   //     // On doit commencer par dechiffrer le contenu protege (mots de passe, etc.)
//   //     let secretAccessKeyChiffre = messageConfiguration.credentials.secretAccessKeyChiffre;
//   //     let iv = messageConfiguration.credentials.iv;
//   //     let cleSecrete = decrypterCleSecrete(messageConfiguration.credentials.cle);
//   //     // console.debug("Secret access key: ");
//   //     // console.debug(cleSecrete);
//   //
//   //     promise = decrypterSymmetrique(secretAccessKeyChiffre, cleSecrete, iv);
//   //   }
//   //
//   //   promise.then(secretAccessKeyDecrypte=>{
//   //     var secretAccessKey = messageConfiguration.credentials.secretAccessKey;
//   //     if(secretAccessKeyDecrypte) {
//   //       // Extraire la cle du contenu decrypte
//   //       let jsonDict = JSON.parse(secretAccessKeyDecrypte);
//   //       secretAccessKey = jsonDict.awsSecretAccessKey;
//   //     }
//   //
//   //     // console.debug("Secret access key AWS: " + secretAccessKey);
//   //
//   //     let configurationAws = {
//   //       apiVersion: AWS_API_VERSION,
//   //       region: messageConfiguration.region,
//   //       credentials: {
//   //         accessKeyId: messageConfiguration.credentials.accessKeyId,
//   //         secretAccessKey,
//   //         region: messageConfiguration.credentials.region,
//   //       }
//   //     }
//   //
//   //     // Connecter a Amazon S3
//   //     const s3 = new S3(configurationAws);
//   //     const fichiers = {};
//   //
//   //     // Creer un dictionnaire de fichiers par fuuid pour retirer
//   //     // les fichiers dejas presents sur le drive S3
//   //     for(let idx in messageConfiguration.fuuidFichiers) {
//   //       let fichier = messageConfiguration.fuuidFichiers[idx];
//   //       fichiers[fichier.fuuid] = fichier;
//   //     }
//   //
//   //     // Demander la liste des fichiers du bucket; on ne veut pas re-uploader
//   //     // les memes fichiers (consignation est immuable)
//   //     var paramsListing = {
//   //       Bucket: messageConfiguration.bucket,
//   //       MaxKeys: 1000,
//   //     }
//   //     if(messageConfiguration.dirfichier) {
//   //       paramsListing.Prefix = messageConfiguration.dirfichier;
//   //     }
//   //
//   //     return new Promise((resolve, reject)=>{
//   //       listerFichiers(s3, paramsListing, fichiers, {resolve, reject});
//   //     })
//   //     .then(()=>{
//   //       console.debug("Commencer upload AWS");
//   //       // Commencer le telechargement
//   //       const listeFichiers = Object.values(fichiers);
//   //
//   //       uploaderFichier(
//   //         s3, listeFichiers,
//   //         {
//   //             mq: this.mq,
//   //             message: messageConfiguration,
//   //             properties: opts.properties,
//   //         }
//   //       );
//   //     });
//   //   });
//   //
//   // }
//
// }

// listerFichiers(s3, paramsListing, fichiers) {
async function listerConsignation(s3, bucketName, repertoire, opts) {
  opts = opts || {}
  const res = opts.res  // OutputStream (optionnel)

  const paramsListing = {
    Bucket: bucketName,
    MaxKeys: 1000,
    Prefix: repertoire,
  }
  if(opts.ContinuationToken) paramsListing.ContinuationToken = opts.ContinuationToken

  debug("awss3.listerConsignation: Faire lecture AWS S3 sous %s / %s", bucketName, repertoire)
  const data = await new Promise ((resolve, reject) => {
    s3.listObjectsV2(paramsListing, async (err, data)=>{
      if(err) {
        console.error("awss3.listerConsignation: Erreur demande liste fichiers")
        return reject(err)
      }
      resolve(data)
    })
  })

  // console.log("Listing fichiers bucket " + paramsListing.Bucket);
  for await (const contents of data.Contents) {
    // debug("Contents : %O", contents)
    const pathFichier = path.parse(contents.Key)
    // debug("Path fichier : %s", pathFichier)
    const fuuid = pathFichier.name
    // debug("Fuuid : %s", fuuid)
    const contentFichier = {
      ...contents,
      fuuid,
    }
    if(res) {
      res.write(JSON.stringify(contentFichier) + '\n')
    }
  }

  if(data.IsTruncated) {
    // debug("Continuer listing");
    opts.ContinuationToken = data.NextContinuationToken
    await listerConsignation(s3, bucketName, repertoire, opts)
  }

}

// async function executerUploadFichier(
//   mq, s3, pathConsignation, infoConsignationWebNoeud, reponseDechiffrageFichier, message,
//   fuuid, mimetype, opts
// ) {
//   debug("aws Upload fichier : %s", fuuid)
//   // Dechiffrer fichier (tmp) pour upload
//   if(!opts) opts = {}
//
//   var cleFichier = null, iv = null
//   try {
//     var infoClePreview = reponseDechiffrageFichier.cles_par_fuuid[fuuid]
//     iv = reponseDechiffrageFichier.iv
//     var cleChiffree = infoClePreview.cle
//     cleFichier = await mq.pki.decrypterAsymetrique(cleChiffree)
//
//   } catch(err) {
//     console.error("publierAwsS3 ERROR: Cle dechiffrage fichier refusee %O", err)
//     mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'echec', progres: -1, err: ''+err}, 'evenement.fichiers.publicAwsS3')
//     throw err
//   }
//   mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'dechiffrage', progres: 2}, 'evenement.fichiers.publicAwsS3')
//
//   var fichierTemporaire = null
//   try {
//     fichierTemporaire = await dechiffrerTemporaire(pathConsignation, fuuid, 'dat', cleFichier, iv)
//     debug("Fichier dechiffre sous : %O", fichierTemporaire)
//     mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'upload', progres: 5}, 'evenement.fichiers.publicAwsS3')
//
//     const metadata = {
//       uuid: message.uuid,
//       fuuid,
//       mimetype,
//     }
//     if(opts.nom_fichier) {
//       metadata.nom_fichier = opts.nom_fichier
//     }
//     if(opts.key_fichier) {
//       metadata.key_fichier = opts.key_fichier
//     }
//
//     await uploaderFichier(s3, mq, message, infoConsignationWebNoeud, metadata, fichierTemporaire.path)
//   } catch(err) {
//     console.error("aws.publierAwsS3 Erreur upload fichier : %O", err)
//     mq.emettreEvenement(
//       {
//         noeud_id: message.noeud_id,
//         fuuid,
//         etat: 'echec',
//         progres: -1,
//         err: 'aws.publierAwsS3 Erreur upload fichier : '+err
//       },
//       'evenement.fichiers.publicAwsS3'
//     )
//     throw err
//   } finally {
//     // Cleanup fichiers temporaires
//     if(fichierTemporaire) fichierTemporaire.cleanup()
//   }
// }

async function uploaderFichier(s3, message, pathFichier, bucketName, bucketDirfichier, opts) {
  opts = opts || {}
  bucketDirfichier = bucketDirfichier || ''

  const fileStream = fs.createReadStream(pathFichier)
  fileStream.on('error', function(err) {
    console.error('ERROR awss3.uploaderFichier: fileStream Error', err);
  })

  // Recuperer taille fichier
  const stat = await fsPromises.stat(pathFichier)
  debug("Taille fichier a uploader : %s", stat.size)
  const bytesTotal = stat.size

  var nomFichier = message.nomFichier
  if(!nomFichier) {
     nomFichier = message.fuuid
     if(message.securite !== '1.public') {
       nomFichier += '.mgs2'
     } else if(message.mimetype) {
       // TODO, mettre extension selon mimetype
       nomFichier += trouverExtension(message.mimetype)
     }
  }

  var pathSurServeur = path.format({
    dir: bucketDirfichier,
    name: nomFichier,
  })

  // - metadata: {
  //     'uuid': message.uuid,
  //     'fuuid': message.fuuid,
  //     'nom_fichier': message.nom_fichier,
  //   }
  const metadata = {
    nom_fichier: nomFichier,
    securite: message.securite || L2Prive,
  }
  if(message.uuid) metadata.uuid = message.uuid
  if(message.fuuid) metadata.fuuid = message.fuuid
  var cacheControl = 'public, max-age=604800, immutable'
  if(message.maxAge !== undefined) {
    const maxAge = message.maxAge
    cacheControl = 'public, max-age=' + maxAge
  }

  const contentType = message.mimetype

  const uploadParams = {
    Bucket: bucketName,
    Key: pathSurServeur,
    Body: fileStream,
    ACL: 'public-read',
    // ContentType: message.mimetype || 'application/octet-stream',
    CacheControl: cacheControl,
    Metadata: metadata,
  }

  if(contentType) {
    uploadParams.ContentType = contentType
  }

  if(message.contentEncoding) {
    uploadParams.ContentEncoding = message.contentEncoding
  }

  if(nomFichier && (opts.download || message.download) ) {
    uploadParams.ContentDisposition = 'attachment; filename="' + nomFichier + '"'
  }

  // call S3 to upload file to specified bucket
  var options = {partSize: 5 * 1024 * 1024, queueSize: 1}

  debug("AWS S3 upload params : %O", uploadParams)

  return new Promise((resolve, reject)=>{
    const managedUpload = s3.upload(uploadParams, options, function (err, data) {
      if (err) {
        console.error("aws.uploaderFichier: Error %O", err);
        return reject(err)
      }
      debug("S3 Upload Success : %O", data);
      return resolve(data)
    })

    // Attacher listener d'evenements
    if(opts.progressCb) {
      managedUpload.on('httpUploadProgress', progress => {
        //  progress : {
        //   loaded: 8174,
        //   total: 8174,
        //   part: 1,
        //   key: 'QME8SjhaCFySD9qBt1AikQ1U7WxieJY2xDg2JCMczJST/public/89122e80-4227-11eb-a00c-0bb29e75acbf'
        // }
        var total = progress.total || bytesTotal
        if(total) {
          progressCb({...progress, total})
        }
      })
    }
  })

}

async function addRepertoire(s3, repertoire, bucketName, opts) {
  opts = opts || {}
  const repertoireRemote = opts.bucketDirfichier || ''

  const listeFichiers = []
  const cb = entry => cbPreparerAwsS3(entry, listeFichiers, repertoire, repertoireRemote)
  const info = await preparerPublicationRepertoire(repertoire, cb)
  debug("Info publication repertoire avec AWS S3 : %O, liste fichiers: %O", info, listeFichiers)

  const message = opts.message || {}
  const pathMimetypes = message.pathMimetypes || {}
  debug("Path mimetypes : %O", pathMimetypes)

  const chunkImmutable = message.chunkImmutable

  for await (const fichier of listeFichiers) {
    debug("Traiter fichier : %O", fichier)
    const originalname = fichier.localPath.replace(repertoire + '/', '')
    var mimetype = pathMimetypes[originalname]
    debug("Mimetype pour nom original %s = %s", originalname, mimetype)
    if(!mimetype) {
      // Tenter de fournir le mimetype. Defaut = application/octet-stream
      mimetype = trouverMimetype(originalname)
      debug("Mimetype identifie pour extension de %s = %s", originalname, mimetype)
    }
    const params = {
      nomFichier: path.basename(fichier.localPath),
      securite: '1.public',
      mimetype,
    }
    if(chunkImmutable && originalname.indexOf('.chunk.') > -1) {
      // Aucuns params de maxAge, va faire public immutable maxAge 1 an
    } else if(message.maxAge !== undefined) {
      params.maxAge = message.maxAge
    }
    if(message.contentEncoding) params.contentEncoding = message.contentEncoding
    const remotePath = path.dirname(fichier.remotePath)
    // await putFichier(sftp, fichier.localPath, fichier.remotePath)
    // s3, message, pathFichier, bucketName, bucketDirfichier, opts
    await uploaderFichier(s3, params, fichier.localPath, bucketName, remotePath, opts)
  }
}

// async function publierAwsS3(mq, pathConsignation, routingKey, message, opts) {
//   if(!opts) opts = {}
//   debug("Commande publicAwsS3 : %O", message)
//   mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'debut', progres: 1}, 'evenement.fichiers.publicAwsS3')
//
//   // Recuperer information noeud, info dechiffrage mot de passe AWS S3
//   const noeudId = message.noeud_id
//   var infoConsignationWebNoeud = null
//   try {
//     const domaineActionInfoNoeud = 'Topologie.infoNoeud'
//     const reponseNoeud = await mq.transmettreRequete(domaineActionInfoNoeud, {noeud_id: noeudId})
//     infoConsignationWebNoeud = reponseNoeud.consignation_web
//     debug("publierAwsS3: Information noeud : %O", reponseNoeud)
//   } catch(err) {
//     console.error("publierAwsS3 ERROR: Information noeud (topologie) non disponible %O", err)
//     mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'echec', progres: -1, err: ''+err}, 'evenement.fichiers.publicAwsS3')
//     return
//   }
//
//   var motDePasseAWSS3 = null
//   try {
//     // Dechiffrer mot de passe AWS S3
//     const domaineActionMotdepasse = 'Topologie.permissionDechiffrage'
//     const credentialsSecretAccessKey = infoConsignationWebNoeud.credentialsSecretAccessKey
//     const identificateurs_document = credentialsSecretAccessKey.identificateurs_document
//     const reponseCleSecrete = await mq.transmettreRequete(
//       domaineActionMotdepasse, {identificateurs_document}, {attacherCertificat: true})
//     // debug("publierAwsS3: Cle secrete chiffree pour secret access key : %O", reponseCleSecrete)
//
//     const secretChiffre = credentialsSecretAccessKey.secret_chiffre
//     motDePasseAWSS3 = await mq.pki.dechiffrerContenuAsymetric(reponseCleSecrete.cle, reponseCleSecrete.iv, secretChiffre)
//     motDePasseAWSS3 = JSON.parse(motDePasseAWSS3)  // Enlever wrapping (")
//
//     // debug("publierAwsS3: Mot de passe (secret access key) : %s", motDePasseAWSS3)
//   } catch(err) {
//     console.error("publierAwsS3 ERROR: Information dechiffrage mot de passe AWSS3 non disponible %O", err)
//     mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'echec', progres: -1, err: ''+err}, 'evenement.fichiers.publicAwsS3')
//     return
//   }
//
//   var reponseDechiffrageFichier = null
//   try {
//     const domaineActionPermission = message.permission['en-tete'].domaine
//     reponseDechiffrageFichier = await mq.transmettreRequete(
//       domaineActionPermission, message.permission, {noformat: true, attacherCertificat: true})
//     debug("Reponse cle dechiffrage fichier : %O", reponseDechiffrageFichier)
//   } catch(err) {
//     console.error("publierAwsS3 ERROR: Information dechiffrage fichiers non disponible %O", err)
//     mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'echec', progres: -1, err: ''+err}, 'evenement.fichiers.publicAwsS3')
//     return
//   }
//
//   const s3 = preparerConnexionS3(infoConsignationWebNoeud, motDePasseAWSS3)
//
//   // Determiner les fichiers a uploader
//   try {
//     await executerUploadFichier(
//       mq, s3, pathConsignation, infoConsignationWebNoeud, reponseDechiffrageFichier, message,
//       message.fuuid, message.mimetype, {nom_fichier: message.nom_fichier}
//     )
//
//     // Uploader preview si present
//     if(message.fuuid_preview && message.mimetype_preview) {
//       debug("Uploader preview du fichier")
//       const key_fichier = message.fuuid + '_preview_1'
//       await executerUploadFichier(
//         mq, s3, pathConsignation, infoConsignationWebNoeud, reponseDechiffrageFichier, message,
//         message.fuuid_preview, message.mimetype_preview, {key_fichier}
//       )
//     }
//
//     // Uploader videos re-encodes si presents
//     if(message.video) {
//       for(const resolution in message.video) {
//         const key_fichier = message.fuuid + '_video_' + resolution
//         const {mimetype, fuuid} = message.video[resolution]
//         debug("Uploader video re-encode %s = %s", resolution, fuuid)
//         await executerUploadFichier(
//           mq, s3, pathConsignation, infoConsignationWebNoeud, reponseDechiffrageFichier, message,
//           fuuid, mimetype, {key_fichier}
//         )
//       }
//     }
//
//     mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'succes', progres: 100}, 'evenement.fichiers.publicAwsS3')
//   } catch(err) {
//     const msg = "publierAwsS3 ERROR: Erreur upload fichier uuid: " + message.uuid + "\n"
//     console.error(msg + ": %O", err)
//     mq.emettreEvenement({noeud_id: message.noeud_id, fuuid: message.fuuid, etat: 'echec', progres: -1, err: msg+err}, 'evenement.fichiers.publicAwsS3')
//   }
// }

function cbPreparerAwsS3(entry, listeFichiers, pathStaging, repertoireRemote) {
  /* Sert a preparer l'upload d'un repertoire vers IPFS. Append a FormData. */
  const pathRelatif = entry.fullPath.replace(pathStaging + '/', '')

  debug("Ajout path relatif : %s", pathRelatif)
  if(entry.stats.isFile()) {
    debug("Creer readStream fichier %s", entry.fullPath)
    listeFichiers.push({
      localPath: entry.fullPath,
      remotePath: path.join(repertoireRemote, pathRelatif),
    })
  }
}

module.exports = {
  AWS_API_VERSION, preparerConnexionS3, uploaderFichier, addRepertoire,
  listerConsignation,
}
