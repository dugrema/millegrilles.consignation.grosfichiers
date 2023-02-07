const debug = require('debug')('messages:publication')
const path = require('path')
const fsPromises = require('fs/promises')
const {
  getPublicKey, 
  //connecterSSH, preparerSftp,
  //putFichier: putFichierSsh,
  //addRepertoire: putRepertoireSsh
} = require('../util/ssh')
// const { init: initIpfs,
//         addFichier: addFichierIpfs,
//         addRepertoire: putRepertoireIpfs,
//         publishName: publishIpns,
//         creerCleIpns: _creerCleIpns,
//         importerCleIpns,
//       } = require('../util/ipfs')
// const { preparerConnexionS3, uploaderFichier: putFichierAwsS3, addRepertoire: putRepertoireAwsS3 } = require('../util/awss3')

const L2PRIVE = '2.prive'
const CONST_CHAMPS_CONFIG = ['typeStore', 'urlDownload', 'consignationUrl']

// fixme
// const { creerStreamDechiffrage, stagingFichier: stagingPublic } = require('../util/publicStaging')

var _mq = null,
    _pathConsignation = null,
    _repertoireCodeWebapps = process.env.WEBAPPS_SRC_FOLDER,
    _storeConsignation = null,
    _instanceId = null

function init(mq, storeConsignation) {
  _mq = mq
  _storeConsignation = storeConsignation

  const idmg = mq.pki.idmg
  _instanceId = mq.pki.cert.subject.getField('CN').value
  // _pathConsignation = new PathConsignation({idmg});

  // const ipfsHost = process.env.IPFS_HOST || 'http://ipfs:5001'
  // initIpfs(ipfsHost)
}

function on_connecter() {
  // _ajouterCb('requete.fichiers.getConfiguration', getConfiguration, {direct: true})
  _ajouterCb(`commande.fichiers.${_instanceId}.modifierConfiguration`, modifierConfiguration, {direct: true})
  _ajouterCb(`evenement.CoreTopologie.changementConsignationPrimaire`, changementConsignationPrimaire, {direct: true})

  // Commandes SSH/SFTP
  _ajouterCb('requete.fichiers.getPublicKeySsh', getPublicKeySsh, {direct: true})

  // _ajouterCb('commande.fichiers.publierFichierSftp', publierFichierSftp)
  // _ajouterCb('commande.fichiers.publierRepertoireSftp', publierRepertoireSftp)
  // _ajouterCb('commande.fichiers.publierVitrineSftp', publierVitrineSftp)

  // Commandes AWS S3
  // _ajouterCb('commande.fichiers.publierFichierAwsS3', publierFichierAwsS3)
  // _ajouterCb('commande.fichiers.publierRepertoireAwsS3', publierRepertoireAwsS3)
  // _ajouterCb('commande.fichiers.publierVitrineAwsS3', publierVitrineAwsS3)

  // Commandes IPFS
  // _ajouterCb('commande.fichiers.publierFichierIpfs', publierFichierIpfs)
  // _ajouterCb('commande.fichiers.publierFichierIpns', publierFichierIpns)
  // _ajouterCb('commande.fichiers.publierRepertoireIpfs', publierRepertoireIpfs)
  // _ajouterCb('commande.fichiers.publierVitrineIpfs', publierVitrineIpfs)
  // _ajouterCb('commande.fichiers.publierIpns', publierIpns)
  // _ajouterCb('commande.fichiers.creerCleIpns', creerCleIpns)
}

function _ajouterCb(rk, cb, opts) {
  opts = opts || {}

  let paramsSup = {}
  if(!opts.direct) paramsSup.qCustom = 'publication'

  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message, opts)=>{return cb(message, routingKey, opts)},
    [rk],
    paramsSup
  )
}

async function getConfiguration(message, rk, opts) {
  opts = opts || {}
  const properties = opts.properties || {}
  debug("publication.getConfiguration (replyTo: %s)", properties.replyTo)
  const configuration = await _storeConsignation.chargerConfiguration()

  _mq.transmettreReponse(configuration, properties.replyTo, properties.correlationId)

}

async function emettrePresence() {
  debug("emettrePresence Configuration fichiers")
  const configuration = await _storeConsignation.chargerConfiguration()
    
  const info = {}
  for(const champ of Object.keys(configuration)) {
      if(CONST_CHAMPS_CONFIG.includes(champ)) info[champ] = configuration[champ]
  }
  
  await _mq.emettreEvenement(info, 'fichiers', {action: 'presence', attacherCertificat: true})
}

async function modifierConfiguration(message, rk, opts) {
  opts = opts || {}
  const properties = opts.properties || {}
  debug("publication.modifierConfiguration (replyTo: %s)", properties.replyTo)

  let reponse = {ok: false}
  try {
    await _storeConsignation.modifierConfiguration(message, {override: true})

    reponse = {ok: true}
  } catch(err) {
    console.error("%O storeConsignation.modifierConfiguration, Erreur store modifierConfiguration : %O", new Date(), err)
    reponse = {ok: false, err: ''+err}
  }
  _mq.transmettreReponse(reponse, properties.replyTo, properties.correlationId)

  emettrePresence()
    .catch(err=>console.error("publication.getConfiguration Erreur emission presence ", err))
}

function getPublicKeySsh(message, rk, opts) {
  opts = opts || {}
  const properties = opts.properties || {}
  debug("publication.getPublicKey (replyTo: %s)", properties.replyTo)
  const clePubliqueEd25519 = getPublicKey()
  const clePubliqueRsa = getPublicKey({keyType: 'rsa'})

  const reponse = {clePubliqueEd25519, clePubliqueRsa}
  _mq.transmettreReponse(reponse, properties.replyTo, properties.correlationId)
}

async function changementConsignationPrimaire(message, rk, opts) {
  const instanceIdPrimaire = message.instance_id
  await _storeConsignation.setEstConsignationPrimaire(instanceIdPrimaire===_instanceId)
}

async function publierFichierSftp(message, rk, opts) {
  throw new Error("TODO - fix me")
  // opts = opts || {}
  // debug("publication.publierFichierSftp : %O (opts: %O)", message, opts)

  // const {host, port, username, fuuid, cdn_id: cdnId} = message
  // const properties = opts.properties || {}
  // const securite = message.securite || L2PRIVE
  // const identificateur_document = {fuuid, '_mg-libelle': 'fichier'}
  // const keyType = message.keyType || 'ed25519'
  // try {
  //   const basedir = message.basedir || './'

  //   var localPath = _pathConsignation.trouverPathLocal(fuuid)
  //   debug("Fichier local a publier sur SSH : %s", localPath)

  //   var mimetype = message.mimetype
  //   if(securite === '1.public') {
  //     // Dechiffrer le fichier public dans staging
  //     const infoFichierPublic = await preparerStagingPublic(fuuid)
  //     debug("Information fichier public : %O", infoFichierPublic)
  //     localPath = infoFichierPublic.filePath
  //   } else if(securite === '2.prive' && mimetype.startsWith('video/')) {
  //     const infoFichierStream = await preparerStagingStream(fuuid)
  //     debug("Information fichier stream : %O", infoFichierStream)
  //     localPath = infoFichierStream.filePath
  //   }
  //   // else {
  //   //   throw new Error("FICHIER PAS PUBLIC : message: %O, opts: %O", message, opts)
  //   // }

  //   const conn = await connecterSSH(host, port, username, {keyType})
  //   const sftp = await preparerSftp(conn)
  //   debug("Connexion SSH et SFTP OK")

  //   var remotePath = null
  //   if(securite === '1.public') {
  //     remotePath = path.join(basedir, 'fichiers', 'public', _pathConsignation.trouverPathRelatif(fuuid, {mimetype}))
  //     if(remotePath.endsWith('.mgs2')) {
  //       // Pas suppose arriver, diag
  //       debug("ERREUR publication.publierFichierSftp fichier public (mimetype: %s) avec ext mgs2 %O (props: %O)", mimetype, message, opts)
  //       throw new Error("Erreur fichier public avec .mgs2")
  //     }
  //   } else if(securite === '2.prive' && mimetype.startsWith('video/')) {
  //     remotePath = path.join(basedir, 'fichiers', 'stream', _pathConsignation.trouverPathRelatif(fuuid, {mimetype}))
  //     if(remotePath.endsWith('.mgs2')) {
  //       // Pas suppose arriver, diag
  //       debug("ERREUR publication.publierFichierSftp fichier public (mimetype: %s) avec ext mgs2 %O (props: %O)", mimetype, message, opts)
  //       throw new Error("Erreur fichier public avec .mgs2")
  //     }
  //   } else {
  //     remotePath = path.join(basedir, 'fichiers', _pathConsignation.trouverPathRelatif(fuuid))
  //   }
  //   debug("Path remote pour le fichier : %s", remotePath)

  //   var dernierEvent = 0
  //   const intervalPublish = 2000
  //   const optsPut = {
  //     progressCb: (current, _, total) => {
  //       debug("SFTP progres cb : %s/%s", current, total)
  //       // Throttle evenements, toutes les 2 secondes
  //       const epochCourant = new Date().getTime()
  //       if(epochCourant-intervalPublish > dernierEvent) {
  //         dernierEvent = epochCourant  // Update date pour throttle
  //         const confirmation = {
  //           identificateur_document,
  //           cdn_id: cdnId,
  //           current_bytes: current,
  //           total_bytes: total,
  //           complete: false,
  //         }
  //         const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
  //         _mq.emettreEvenement(confirmation, domaineActionConfirmation)
  //       }
  //     }
  //   }

  //   await putFichierSsh(sftp, localPath, remotePath, optsPut)
  //   debug("Put fichier ssh OK")

  //   if(properties && properties.replyTo) {
  //     _mq.transmettreReponse({ok: true}, properties.replyTo, properties.correlationId)
  //   }

  //   // Emettre evenement de publication
  //   const confirmation = {
  //     identificateur_document,
  //     cdn_id: cdnId,
  //     complete: true,
  //     securite,
  //   }
  //   const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
  //   _mq.emettreEvenement(confirmation, domaineActionConfirmation)

  // } catch(err) {
  //   console.error("ERROR publication.publierFichierSftp: Erreur publication fichier sur sftp : %O", err)
  //   if(properties && properties.replyTo) {
  //     _mq.transmettreReponse({ok: false, err: ''+err}, properties.replyTo, properties.correlationId)
  //   }

  //   // Emettre evenement de publication
  //   const confirmation = {
  //     identificateur_document,
  //     cdn_id: cdnId,
  //     complete: false,
  //     err: ''+err,
  //     stack: JSON.stringify(err.stack),
  //   }
  //   const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
  //   _mq.emettreEvenement(confirmation, domaineActionConfirmation)
  // }
}

// async function publierFichierIpfs(message, rk, opts) {
//   opts = opts || {}
//   const {fuuid, cdn_id: cdnId} = message
//   const securite = message.securite || L2PRIVE
//   const properties = opts.properties || {}
//   const identificateur_document = {fuuid, '_mg-libelle': 'fichier'}
//
//   try {
//
//     var localPath = _pathConsignation.trouverPathLocal(fuuid)
//     debug("Fichier local a publier sur SSH : %s", localPath)
//
//     if(securite === '1.public') {
//       // Dechiffrer le fichier public dans staging
//       const infoFichierPublic = await preparerStagingPublic(fuuid)
//       debug("Information fichier public : %O", infoFichierPublic)
//       localPath = infoFichierPublic.filePath
//     }
//
//     const reponse = await addFichierIpfs(localPath)
//     debug("Put fichier ipfs OK : %O", reponse)
//     const reponseMq = {
//       ok: true,
//       hash: reponse.Hash,
//       size: reponse.Size
//     }
//
//     if(properties && properties.replyTo) {
//       _mq.transmettreReponse(reponseMq, properties.replyTo, properties.correlationId)
//     }
//
//     // Emettre evenement de publication
//     const confirmation = {
//       identificateur_document,
//       cdn_id: cdnId,
//       complete: true,
//       securite,
//       cid: reponse.Hash,
//     }
//     const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//     _mq.emettreEvenement(confirmation, domaineActionConfirmation)
//
//   } catch(err) {
//     console.error("ERROR publication.publierFichierIpfs: Erreur publication fichier sur ipfs : %O", err)
//     if(properties && properties.replyTo) {
//       _mq.transmettreReponse({ok: false, err: ''+err}, properties.replyTo, properties.correlationId)
//     }
//
//     // Emettre evenement de publication
//     const confirmation = {
//       identificateur_document,
//       cdn_id: cdnId,
//       complete: false,
//       err: ''+err,
//       stack: JSON.stringify(err.stack),
//     }
//     const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//     _mq.emettreEvenement(confirmation, domaineActionConfirmation)
//   }
// }
//
// async function publierFichierIpns(message, rk, opts) {
//   const cdns = JSON.parse(message.cdns)
//   const cdnIds = cdns.map(item=>item.cdn_id)
//   const identificateur_document = JSON.parse(message.identificateur_document)
//   const securite = message.securite
//   try {
//     debug("publication.publierFichierIpns %O", message)
//
//     const localPath = message.fichier.path
//
//     const reponseCid = await addFichierIpfs(localPath)
//     debug("Put fichier ipfs OK : %O", reponseCid)
//     const cid = reponseCid.Hash,
//           keyName = message.ipns_key_name
//     var reponseIpns = null
//     try {
//       reponseIpns = await publishIpns(cid, keyName)
//       debug("Put fichier ipns OK")
//     } catch(err) {
//       debug("erreur publication ipns, cle manquante ou autre erreur %O", err)
//       if(err.code === 404) {
//         // On doit importer la cle
//         const permission = JSON.parse(message.permission)
//         // const ipnsKeyBytes = multibase.decode(message.ipns_key)
//         const cleJson = await dechiffrerDocumentAvecMq(
//           _mq, message.ipns_key, {permission})
//         await importerCleIpns(keyName, cleJson)
//
//         // Tenter a nouveau de publier la ressource
//         reponseIpns = await publishIpns(cid, keyName)
//         debug("Put fichier ipns OK")
//
//       } else {
//         throw err
//       }
//     }
//
//     // Emettre evenement de publication
//     const ipns_key_hash = reponseIpns.data.Name
//     const confirmation = {
//       cdn_ids: cdnIds,
//       identificateur_document,
//       complete: true,
//       securite,
//       cid,
//       ipns_id: ipns_key_hash,
//     }
//     const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//     _mq.emettreEvenement(confirmation, domaineActionConfirmation)
//   } catch(err) {
//     console.error("ERROR publication.publierFichierIpns %O", err)
//     const messageErreur = {
//       cdn_ids: cdnIds,
//       identificateur_document,
//       complete: false,
//       err: ''+err,
//       stack: JSON.stringify(err.stack),
//     }
//     const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//     _mq.emettreEvenement(messageErreur, domaineActionConfirmation)
//   }
//
// }

// async function publierFichierAwsS3(message, rk, opts) {
//   opts = opts || {}
//   const {
//     fuuid, bucketRegion, credentialsAccessKeyId, secretAccessKey_chiffre,
//     permission, bucketName, bucketDirfichier, cdn_id: cdnId} = message
//   const securite = message.securite || L2PRIVE
//   const properties = opts.properties || {}
//   const identificateur_document = {fuuid, '_mg-libelle': 'fichier'}

//   try {
//     // Connecter AWS S3
//     const secretKeyInfo = {secretAccessKey: secretAccessKey_chiffre, permission}
//     const s3 = await preparerConnexionS3(_mq, bucketRegion, credentialsAccessKeyId, secretKeyInfo)
//     // const s3 = await preparerConnexionS3(_mq, bucketRegion, credentialsAccessKeyId, secretAccessKey_chiffre)

//     var localPath = _pathConsignation.trouverPathLocal(fuuid)
//     debug("Fichier local a publier sur AWS S3 : %s", localPath)

//     if(securite === '1.public') {
//       // Dechiffrer le fichier public dans staging
//       const infoFichierPublic = await preparerStagingPublic(fuuid)
//       debug("Information fichier public : %O", infoFichierPublic)
//       localPath = infoFichierPublic.filePath
//     }

//     var dernierEvent = 0
//     const intervalPublish = 1000
//     const optsPut = {
//       progressCb: update => {
//         //   loaded: 8174,
//         //   total: 8174,
//         //   part: 1,
//         //   key: 'QME8SjhaCFySD9qBt1AikQ1U7WxieJY2xDg2JCMczJST/public/89122e80-4227-11eb-a00c-0bb29e75acbf'

//         debug("AWS S3 progres cb : %O", update)
//         // Throttle evenements, toutes les 2 secondes
//         const epochCourant = new Date().getTime()
//         if(epochCourant-intervalPublish > dernierEvent) {
//           dernierEvent = epochCourant  // Update date pour throttle
//           const confirmation = {
//             identificateur_document,
//             cdn_id: cdnId,
//             // current_bytes: current,
//             // total_bytes: total,
//             complete: false,
//           }
//           const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//           _mq.emettreEvenement(confirmation, domaineActionConfirmation)
//         }
//       }
//     }

//     debug("Debut upload AWS S3 %s vers %s", fuuid, bucketName)
//     // let bucketDirfichierEtendu = bucketDirfichier
//     // if(securite === '1.public') {
//     //   bucketDirfichierEtendu = bucketDirfichier + '/public'
//     // }
//     var remotePath = null
//     if(securite === '1.public') {
//       remotePath = path.join(bucketDirfichier, 'fichiers', 'public')
//     } else {
//       remotePath = path.join(bucketDirfichier, 'fichiers')
//     }
//     const resultat = await putFichierAwsS3(s3, message, localPath, bucketName, remotePath, optsPut)
//     debug("Resultat upload S3 : %O", resultat)
//     // Resultat:
//     // {
// 	  //    ETag: '"874e56c9ae15779368e082b3b95b0832"',
// 	  //    Location: 'https://millegrilles.s3.amazonaws.com/mg-dev4/z8VwJR6hCq6z7TJY2MjsJsfAGTkjEimw9yduR6dDnHnUf4uF7cJFJxCWKmy2tw5kpRJtgvaZCatQKu5dDbCC63fVk6t.mgs2',
// 	  //    key: 'mg-dev4/z8VwJR6hCq6z7TJY2MjsJsfAGTkjEimw9yduR6dDnHnUf4uF7cJFJxCWKmy2tw5kpRJtgvaZCatQKu5dDbCC63fVk6t.mgs2',
// 	  //    Key: 'mg-dev4/z8VwJR6hCq6z7TJY2MjsJsfAGTkjEimw9yduR6dDnHnUf4uF7cJFJxCWKmy2tw5kpRJtgvaZCatQKu5dDbCC63fVk6t.mgs2',
// 	  //    Bucket: 'millegrilles'
//     // }

//     const reponseMq = {
//       ok: true,
//       ...resultat,
//     }

//     if(properties && properties.replyTo) {
//       _mq.transmettreReponse(reponseMq, properties.replyTo, properties.correlationId)
//     }

//     // Emettre evenement de publication
//     const confirmation = {
//       identificateur_document,
//       cdn_id: cdnId,
//       complete: true,
//       securite,
//     }
//     const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//     _mq.emettreEvenement(confirmation, domaineActionConfirmation)

//   } catch(err) {
//     console.error("ERROR publication.publierFichierAwsS3: Erreur publication fichier sur AWS S3 : %O", err)
//     if(properties && properties.replyTo) {
//       _mq.transmettreReponse({ok: false, err: ''+err}, properties.replyTo, properties.correlationId)
//     }

//     // Emettre evenement de publication
//     const confirmation = {
//       identificateur_document,
//       cdn_id: cdnId,
//       complete: false,
//       err: ''+err,
//       stack: JSON.stringify(err.stack),
//     }
//     const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//     _mq.emettreEvenement(confirmation, domaineActionConfirmation)
//   }
// }

// async function preparerStagingPublic(fuuid) {
//   // Dechiffrer un fichier public dans zone de downloadStaging (auto-cleanup)

//   const infoStream = await creerStreamDechiffrage(_mq, fuuid)
//   if(infoStream.acces === '0.refuse') {
//     debug("Permission d'acces refuse en mode %s pour %s", infoStream.acces, fuuid)
//     throw new Error("Acces public refuse a " + fuuid)
//   }

//   // // Ajouter information de dechiffrage pour la reponse
//   // res.decipherStream = infoStream.decipherStream
//   // res.permission = infoStream.permission
//   // res.fuuid = infoStream.fuuidEffectif

//   // const fuuidEffectif = infoStream.fuuidEffectif

//   // Preparer le fichier dechiffre dans repertoire de staging
//   const infoFichierEffectif = await stagingPublic(_pathConsignation, fuuid, infoStream)
//   //res.stat = infoFichierEffectif.stat
//   //res.filePath = infoFichierEffectif.filePath
//   return infoFichierEffectif
// }

// async function preparerStagingStream(fuuid) {
//   const infoStream = await creerStreamDechiffrage(_mq, fuuid, {prive: true})
//   if(infoStream.acces === '0.refuse') {
//     debug("Permission d'acces refuse en mode %s pour %s", infoStream.acces, fuuid)
//     throw new Error("Acces public refuse a " + fuuid)
//   }

//   // Preparer le fichier dechiffre dans repertoire de staging
//   const infoFichierEffectif = await stagingPublic(_pathConsignation, fuuid, infoStream)
//   return infoFichierEffectif
// }

// async function publierRepertoireSftp(message, rk, opts) {
//   const {host, port, username, repertoireStaging, repertoireRemote, identificateur_document, cdn_id, securite, keyType} = message
//   try {
//     debug("Publier repertoire sftp\n%O", message)
//     const conn = await connecterSSH(host, port, username, {keyType})
//     const sftp = await preparerSftp(conn)
//     const reponseSsh = await putRepertoireSsh(sftp, repertoireStaging, {repertoireRemote})

//     // Emettre evenement de publication
//     const confirmation = {
//       identificateur_document,
//       cdn_id,
//       complete: true,
//       securite,
//     }
//     const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//     _mq.emettreEvenement(confirmation, domaineActionConfirmation)

//   } catch(err) {
//     console.error('ERROR publication.publierRepertoireSftp %O', err)
//     // Emettre evenement d'echec de publication
//     const confirmation = {
//       identificateur_document,
//       cdn_id,
//       complete: false,
//       err: ''+err,
//       stack: JSON.stringify(err.stack),
//     }
//     const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//     _mq.emettreEvenement(confirmation, domaineActionConfirmation)
//   } finally {
//     if(message.uploadUnique) {
//       // Supprimer le repertoire de staging
//       await fsPromises.rm(repertoireStaging, {recursive: true})
//     }
//   }
// }

// function publierVitrineSftp(message, rk, opts) {
//   // Va publier le code de vitrine via SFTP
//   const pathVitrine = path.join(_repertoireCodeWebapps, 'vitrine')
//   const configurationPublication = {
//     ...message,

//     // Ajouter repertoire source
//     repertoireStaging: pathVitrine,
//   }
//   debug("Publier vitrine : %O", configurationPublication)
//   return publierRepertoireSftp(configurationPublication, rk, opts)
// }

// async function publierRepertoireIpfs(message, rk, opts) {
//   debug("Publier repertoire ipfs : %O", message)
//   const {repertoireStaging, identificateur_document, cdn_id, securite} = message
//
//   const ipnsKeyName = message.ipns_key_name
//   const optsCommande = {
//     ...opts,
//     ipnsKeyName
//   }
//
//   try {
//     if(message.fichierUnique) {
//       debug("Publier fichier %s", message.fichierUnique)
//
//       const reponse = await addFichierIpfs(message.fichierUnique, optsCommande)
//       debug("Put fichier ipfs OK : %O", reponse)
//       const reponseMq = {
//         ok: true,
//         hash: reponse.Hash,
//         size: reponse.Size
//       }
//
//       // Emettre evenement de publication
//       const confirmation = {
//         identificateur_document,
//         cdn_id,
//         complete: true,
//         // securite,
//         cid: reponse.Hash,
//         ...reponse,
//       }
//
//       const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//       _mq.emettreEvenement(confirmation, domaineActionConfirmation)
//
//     } else {
//       const reponseIpfs = await putRepertoireIpfs(repertoireStaging, optsCommande)
//       debug("Publication IPFS : %O", reponseIpfs)
//
//       // Emettre evenement de publication
//       const confirmation = {
//         identificateur_document,
//         cdn_id,
//         complete: true,
//         securite,
//         ...reponseIpfs,
//       }
//
//       const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//       _mq.emettreEvenement(confirmation, domaineActionConfirmation)
//     }
//
//   } catch(err) {
//     console.error('ERROR publication.publierRepertoireSftp %O', err)
//
//     // Emettre evenement de publication
//     const confirmation = {
//       identificateur_document,
//       cdn_id,
//       complete: false,
//       err: ''+err,
//       stack: JSON.stringify(err.stack),
//     }
//     const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//     _mq.emettreEvenement(confirmation, domaineActionConfirmation)
//
//   } finally {
//     if(message.uploadUnique) {
//       // Supprimer le repertoire de staging
//       await fsPromises.rm(repertoireStaging, {recursive: true})
//     }
//   }
// }
//
// function publierVitrineIpfs(message, rk, opts) {
//   // Va publier le code de vitrine via SFTP
//   const pathVitrine = path.join(_repertoireCodeWebapps, 'vitrine')
//   const configurationPublication = {
//     ...message,
//
//     // Ajouter repertoire source
//     repertoireStaging: pathVitrine,
//     ipnsKeyName: message.ipns_key_name,
//   }
//   debug("Publier vitrine IPFS : %O", configurationPublication)
//   return publierRepertoireIpfs(configurationPublication, rk, opts)
// }

// async function publierRepertoireAwsS3(message, rk, opts) {
//   debug("publication.publierVitrineIpfs Publier vitrine vers AWS S3 : %O", message)
//   const {
//     repertoireStaging, bucketRegion, credentialsAccessKeyId, secretAccessKey_chiffre,
//     permission, bucketName, bucketDirfichier,
//     identificateur_document, cdn_id, securite } = message

//   try {
//     // Connecter AWS S3
//     const secretKeyInfo = {secretAccessKey: secretAccessKey_chiffre, permission}
//     const s3 = await preparerConnexionS3(_mq, bucketRegion, credentialsAccessKeyId, secretKeyInfo)
//     // const s3 = await preparerConnexionS3(_mq, bucketRegion, credentialsAccessKeyId, secretAccessKey_chiffre)
//     const reponse = await putRepertoireAwsS3(s3, repertoireStaging, bucketName, {bucketDirfichier, message})
//     debug("publication.publierVitrineIpfs Fin upload AWS S3 : %O", reponse)

//     // Emettre evenement de publication
//     const confirmation = {
//       identificateur_document,
//       cdn_id,
//       complete: true,
//       securite,
//     }
//     const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//     _mq.emettreEvenement(confirmation, domaineActionConfirmation)

//   } catch(err) {
//     console.error('ERROR publication.publierRepertoireSftp %O', err)

//     // Emettre evenement d'echec de publication
//     const confirmation = {
//       identificateur_document,
//       cdn_id,
//       complete: false,
//       err: ''+err,
//       stack: JSON.stringify(err.stack),
//     }
//     const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
//     _mq.emettreEvenement(confirmation, domaineActionConfirmation)

//   } finally {
//     if(message.uploadUnique) {
//       // Supprimer le repertoire de staging
//       await fsPromises.rm(repertoireStaging, {recursive: true})
//     }
//   }
// }

// function publierVitrineAwsS3(message, rk, opts) {
//   // Va publier le code de vitrine via SFTP
//   const pathVitrine = path.join(_repertoireCodeWebapps, 'vitrine')
//   const configurationPublication = {
//     ...message,

//     // Ajouter repertoire source
//     repertoireStaging: pathVitrine,
//     maxAge: 0,
//     chunkImmutable: true,  // fichiers avec .chunk. vont etre mis en cache public immutable maxAge 1 an
//   }
//   debug("Publier vitrine sur AWS S3 : %O", configurationPublication)
//   return publierRepertoireAwsS3(configurationPublication, rk, opts)
// }

// async function publierIpns(message, rk, opts) {
//   debug("Publier cle ipns")
//   const {cid, keyName} = message
//   await publishIpns(cid, keyName)
// }
//
// async function creerCleIpns(commande, rk, opts) {
//   try {
//     // Creer une cle privee, chiffrer et sauvegarder (e.g. maitre des cles)
//     const {nom: nomCle} = commande
//     const reponse = await _creerCleIpns(_mq, nomCle)
//     repondre(reponse, opts.properties)
//   } catch(err) {
//     // Emettre evenement d'echec
//     emettreErreur(err, opts.properties)
//   }
// }

function repondre(reponse, properties) {
  _mq.transmettreReponse(reponse, properties.replyTo, properties.correlationId)
}

function emettreErreur(erreur, properties) {
  debug("TODO Emettre echec : %O", erreur)
  const message = {err: ''+erreur}
  if(erreur.stack) {
    message.stack = JSON.stringify(erreur.stack)
  }
  _mq.transmettreReponse(message, properties.replyTo, properties.correlationId)
}

module.exports = {
  init, on_connecter, 
  // getPublicKey  // fixme ipfs
}
