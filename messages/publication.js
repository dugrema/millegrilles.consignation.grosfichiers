const debug = require('debug')('millegrilles:fichiers:publication')
const path = require('path')
const fsPromises = require('fs/promises')
const multibase = require('multibase')
const { PathConsignation } = require('../util/traitementFichier')
const { getPublicKey, connecterSSH, preparerSftp, putFichier: putFichierSsh, addRepertoire: putRepertoireSsh } = require('../util/ssh')
const { init: initIpfs,
        addFichier: addFichierIpfs,
        addRepertoire: putRepertoireIpfs,
        publishName: publishIpns,
        creerCleIpns: _creerCleIpns,
        importerCleIpns,
      } = require('../util/ipfs')
const { preparerConnexionS3, uploaderFichier: putFichierAwsS3, addRepertoire: putRepertoireAwsS3 } = require('../util/awss3')
const { creerStreamDechiffrage, stagingFichier: stagingPublic } = require('../util/publicStaging')
const { dechiffrerDocumentAvecMq } = require('@dugrema/millegrilles.common/lib/chiffrage')

var _mq = null,
    _pathConsignation = null

function init(mq) {
  _mq = mq

  const idmg = mq.pki.idmg
  _pathConsignation = new PathConsignation({idmg});

  const ipfsHost = process.env.IPFS_HOST || 'http://ipfs:5001'
  initIpfs(ipfsHost)
}

function on_connecter() {
  // Commandes SSH/SFTP
  _ajouterCb('requete.fichiers.getPublicKeySsh', getPublicKeySsh, {direct: true})
  _ajouterCb('commande.fichiers.publierFichierSftp', publierFichierSftp)
  _ajouterCb('commande.fichiers.publierRepertoireSftp', publierRepertoireSftp)

  // Commandes AWS S3
  _ajouterCb('commande.fichiers.publierFichierAwsS3', publierFichierAwsS3)
  _ajouterCb('commande.fichiers.publierRepertoireAwsS3', publierRepertoireAwsS3)

  // Commandes IPFS
  _ajouterCb('commande.fichiers.publierFichierIpfs', publierFichierIpfs)
  _ajouterCb('commande.fichiers.publierFichierIpns', publierFichierIpns)
  _ajouterCb('commande.fichiers.publierRepertoireIpfs', publierRepertoireIpfs)
  _ajouterCb('commande.fichiers.publierIpns', publierIpns)
  _ajouterCb('commande.fichiers.creerCleIpns', creerCleIpns)
}

function _ajouterCb(rk, cb, opts) {
  opts = opts || {}
  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message, opts)=>{return cb(message, routingKey, opts)},
    [rk],
    {operationLongue: !opts.direct}
  )
}

function getPublicKeySsh(message, rk, opts) {
  opts = opts || {}
  const properties = opts.properties || {}
  debug("publication.getPublicKey (replyTo: %s)", properties.replyTo)
  const clePublique = getPublicKey()

  const reponse = {clePublique}
  _mq.transmettreReponse(reponse, properties.replyTo, properties.correlationId)
}

async function publierFichierSftp(message, rk, opts) {
  opts = opts || {}
  const {host, port, username, fuuid, cdn_id: cdnId} = message
  const properties = opts.properties || {}
  const securite = message.securite || '3.protege'
  try {
    const basedir = message.basedir || './'

    var localPath = _pathConsignation.trouverPathLocal(fuuid)
    debug("Fichier local a publier sur SSH : %s", localPath)

    var mimetype = null
    if(securite === '1.public') {
      // Dechiffrer le fichier public dans staging
      const infoFichierPublic = await preparerStagingPublic(fuuid)
      debug("Information fichier public : %O", infoFichierPublic)
      localPath = infoFichierPublic.filePath
      mimetype = message.mimetype
    }

    const conn = await connecterSSH(host, port, username)
    const sftp = await preparerSftp(conn)
    debug("Connexion SSH et SFTP OK")

    var remotePath = null
    if(securite === '1.public') {
      remotePath = path.join(basedir, 'fichiers', 'public', _pathConsignation.trouverPathRelatif(fuuid, {mimetype}))
    } else {
      remotePath = path.join(basedir, 'fichiers', _pathConsignation.trouverPathRelatif(fuuid, {mimetype}))
    }
    debug("Path remote pour le fichier : %s", remotePath)

    var dernierEvent = 0
    const intervalPublish = 2000
    const optsPut = {
      progressCb: (current, _, total) => {
        debug("SFTP progres cb : %s/%s", current, total)
        // Throttle evenements, toutes les 2 secondes
        const epochCourant = new Date().getTime()
        if(epochCourant-intervalPublish > dernierEvent) {
          dernierEvent = epochCourant  // Update date pour throttle
          const confirmation = {
            fuuid,
            cdn_id: cdnId,
            current_bytes: current,
            total_bytes: total,
            complete: false,
          }
          const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
          _mq.emettreEvenement(confirmation, domaineActionConfirmation)
        }
      }
    }

    await putFichierSsh(sftp, localPath, remotePath, optsPut)
    debug("Put fichier ssh OK")

    if(properties && properties.replyTo) {
      _mq.transmettreReponse({ok: true}, properties.replyTo, properties.correlationId)
    }

    // Emettre evenement de publication
    const confirmation = {
      fuuid,
      cdn_id: cdnId,
      complete: true,
      securite,
    }
    const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
    _mq.emettreEvenement(confirmation, domaineActionConfirmation)

  } catch(err) {
    console.error("ERROR publication.publierFichierSftp: Erreur publication fichier sur sftp : %O", err)
    if(properties && properties.replyTo) {
      _mq.transmettreReponse({ok: false, err: ''+err}, properties.replyTo, properties.correlationId)
    }

    // Emettre evenement de publication
    const confirmation = {
      fuuid,
      cdn_id: cdnId,
      complete: false,
      err: ''+err,
      stack: JSON.stringify(err.stack),
    }
    const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
    _mq.emettreEvenement(confirmation, domaineActionConfirmation)
  }
}

async function publierFichierIpfs(message, rk, opts) {
  opts = opts || {}
  const {fuuid, cdn_id: cdnId} = message
  const securite = message.securite || '3.protege'
  const properties = opts.properties || {}

  try {

    var localPath = _pathConsignation.trouverPathLocal(fuuid)
    debug("Fichier local a publier sur SSH : %s", localPath)

    if(securite === '1.public') {
      // Dechiffrer le fichier public dans staging
      const infoFichierPublic = await preparerStagingPublic(fuuid)
      debug("Information fichier public : %O", infoFichierPublic)
      localPath = infoFichierPublic.filePath
    }

    const reponse = await addFichierIpfs(localPath)
    debug("Put fichier ipfs OK : %O", reponse)
    const reponseMq = {
      ok: true,
      hash: reponse.Hash,
      size: reponse.Size
    }

    if(properties && properties.replyTo) {
      _mq.transmettreReponse(reponseMq, properties.replyTo, properties.correlationId)
    }

    // Emettre evenement de publication
    const confirmation = {
      fuuid,
      cdn_id: cdnId,
      complete: true,
      securite,
      cid: reponse.Hash,
    }
    const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
    _mq.emettreEvenement(confirmation, domaineActionConfirmation)

  } catch(err) {
    console.error("ERROR publication.publierFichierIpfs: Erreur publication fichier sur ipfs : %O", err)
    if(properties && properties.replyTo) {
      _mq.transmettreReponse({ok: false, err: ''+err}, properties.replyTo, properties.correlationId)
    }

    // Emettre evenement de publication
    const confirmation = {
      fuuid,
      cdn_id: cdnId,
      complete: false,
      err: ''+err,
      stack: JSON.stringify(err.stack),
    }
    const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
    _mq.emettreEvenement(confirmation, domaineActionConfirmation)
  }
}

async function publierFichierIpns(message, rk, opts) {
  try {
    debug("publication.publierFichierIpns %O", message)

    const localPath = message.fichier.path

    const reponseCid = await addFichierIpfs(localPath)
    debug("Put fichier ipfs OK : %O", reponseCid)
    const cid = reponseCid.Hash,
          keyName = message.ipns_key_name
    try {
      const reponseIpns = await publishIpns(cid, keyName)
      debug("Put fichier ipns OK")
    } catch(err) {
      if(err.code === 404) {
        // On doit importer la cle
        const permission = JSON.parse(message.permission)
        // const ipnsKeyBytes = multibase.decode(message.ipns_key)
        const cleJson = await dechiffrerDocumentAvecMq(
          _mq, message.ipns_key, {permission})
        await importerCleIpns(keyName, cleJson)

        // Tenter a nouveau de publier la ressource
        const reponseIpns = await publishIpns(cid, keyName)
        debug("Put fichier ipns OK")

      } else {
        throw err
      }
    }
    // const reponseMq = {
    //   ok: true,
    //   hash: reponse.Hash,
    //   size: reponse.Size
    // }

    // Emettre evenement de publication
    const ipns_key_hash = reponseIpns.data.Name
    const confirmation = {
      //cdn_id: cdnId,

      // identificateurs_document: {section_id, type_section: page, etc.}

      complete: true,
      // securite,
      hash: cid,
      ipns_id: ipns_key_hash,
    }
    const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
    _mq.emettreEvenement(confirmation, domaineActionConfirmation)
  } catch(err) {
    console.error("ERROR publication.publierFichierIpns %O", err)
  }

}

async function publierFichierAwsS3(message, rk, opts) {
  opts = opts || {}
  const {
    fuuid, bucketRegion, credentialsAccessKeyId, secretAccessKey_chiffre,
    permission, bucketName, bucketDirfichier, cdn_id: cdnId} = message
  const securite = message.securite || '3.protege'
  const properties = opts.properties || {}

  try {
    // Connecter AWS S3
    const secretKeyInfo = {secretAccessKey: secretAccessKey_chiffre, permission}
    const s3 = await preparerConnexionS3(_mq, bucketRegion, credentialsAccessKeyId, secretKeyInfo)
    // const s3 = await preparerConnexionS3(_mq, bucketRegion, credentialsAccessKeyId, secretAccessKey_chiffre)

    var localPath = _pathConsignation.trouverPathLocal(fuuid)
    debug("Fichier local a publier sur AWS S3 : %s", localPath)

    if(securite === '1.public') {
      // Dechiffrer le fichier public dans staging
      const infoFichierPublic = await preparerStagingPublic(fuuid)
      debug("Information fichier public : %O", infoFichierPublic)
      localPath = infoFichierPublic.filePath
    }

    var dernierEvent = 0
    const intervalPublish = 1000
    const optsPut = {
      progressCb: update => {
        //   loaded: 8174,
        //   total: 8174,
        //   part: 1,
        //   key: 'QME8SjhaCFySD9qBt1AikQ1U7WxieJY2xDg2JCMczJST/public/89122e80-4227-11eb-a00c-0bb29e75acbf'

        debug("AWS S3 progres cb : %O", update)
        // Throttle evenements, toutes les 2 secondes
        const epochCourant = new Date().getTime()
        if(epochCourant-intervalPublish > dernierEvent) {
          dernierEvent = epochCourant  // Update date pour throttle
          const confirmation = {
            fuuid,
            cdn_id: cdnId,
            // current_bytes: current,
            // total_bytes: total,
            complete: false,
          }
          const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
          _mq.emettreEvenement(confirmation, domaineActionConfirmation)
        }
      }
    }

    debug("Debut upload AWS S3 %s vers %s", fuuid, bucketName)
    let bucketDirfichierEtendu = bucketDirfichier
    if(securite === '1.public') {
      bucketDirfichierEtendu = bucketDirfichier + '/public'
    }
    const resultat = await putFichierAwsS3(s3, message, localPath, bucketName, bucketDirfichierEtendu, optsPut)
    debug("Resultat upload S3 : %O", resultat)
    // Resultat:
    // {
	  //    ETag: '"874e56c9ae15779368e082b3b95b0832"',
	  //    Location: 'https://millegrilles.s3.amazonaws.com/mg-dev4/z8VwJR6hCq6z7TJY2MjsJsfAGTkjEimw9yduR6dDnHnUf4uF7cJFJxCWKmy2tw5kpRJtgvaZCatQKu5dDbCC63fVk6t.mgs2',
	  //    key: 'mg-dev4/z8VwJR6hCq6z7TJY2MjsJsfAGTkjEimw9yduR6dDnHnUf4uF7cJFJxCWKmy2tw5kpRJtgvaZCatQKu5dDbCC63fVk6t.mgs2',
	  //    Key: 'mg-dev4/z8VwJR6hCq6z7TJY2MjsJsfAGTkjEimw9yduR6dDnHnUf4uF7cJFJxCWKmy2tw5kpRJtgvaZCatQKu5dDbCC63fVk6t.mgs2',
	  //    Bucket: 'millegrilles'
    // }

    const reponseMq = {
      ok: true,
      ...resultat,
    }

    if(properties && properties.replyTo) {
      _mq.transmettreReponse(reponseMq, properties.replyTo, properties.correlationId)
    }

    // Emettre evenement de publication
    const confirmation = {
      fuuid,
      cdn_id: cdnId,
      complete: true,
      securite,
    }
    const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
    _mq.emettreEvenement(confirmation, domaineActionConfirmation)

  } catch(err) {
    console.error("ERROR publication.publierFichierAwsS3: Erreur publication fichier sur AWS S3 : %O", err)
    if(properties && properties.replyTo) {
      _mq.transmettreReponse({ok: false, err: ''+err}, properties.replyTo, properties.correlationId)
    }

    // Emettre evenement de publication
    const confirmation = {
      fuuid,
      cdn_id: cdnId,
      complete: false,
      err: ''+err,
      stack: JSON.stringify(err.stack),
    }
    const domaineActionConfirmation = 'evenement.fichiers.publierFichier'
    _mq.emettreEvenement(confirmation, domaineActionConfirmation)
  }
}

async function preparerStagingPublic(fuuid) {
  // Dechiffrer un fichier public dans zone de downloadStaging (auto-cleanup)

  const infoStream = await creerStreamDechiffrage(_mq, fuuid)
  if(infoStream.acces === '0.refuse') {
    debug("Permission d'acces refuse en mode %s pour %s", infoStream.acces, fuuid)
    throw new Error("Acces public refuse a " + fuuid)
  }

  // // Ajouter information de dechiffrage pour la reponse
  // res.decipherStream = infoStream.decipherStream
  // res.permission = infoStream.permission
  // res.fuuid = infoStream.fuuidEffectif

  // const fuuidEffectif = infoStream.fuuidEffectif

  // Preparer le fichier dechiffre dans repertoire de staging
  const infoFichierEffectif = await stagingPublic(_pathConsignation, fuuid, infoStream)
  //res.stat = infoFichierEffectif.stat
  //res.filePath = infoFichierEffectif.filePath
  return infoFichierEffectif
}

async function publierRepertoireSftp(message, rk, opts) {
  const {host, port, username, repertoireStaging, repertoireRemote} = message
  try {
    debug("Publier repertoire sftp")
    const conn = await connecterSSH(host, port, username)
    const sftp = await preparerSftp(conn)
    const reponseSsh = await putRepertoireSsh(sftp, repertoireStaging, {repertoireRemote})

    // Emettre evenement de publication

  } catch(err) {
    console.error('ERROR publication.publierRepertoireSftp %O', err)
    // Emettre evenement d'echec de publication

  } finally {
    if(message.uploadUnique) {
      // Supprimer le repertoire de staging
      await fsPromises.rm(repertoireStaging, {recursive: true})
    }
  }
}

async function publierRepertoireIpfs(message, rk, opts) {
  debug("Publier repertoire ipfs")
  const {repertoireStaging} = message
  try {
    const reponseIpfs = await putRepertoireIpfs(repertoireStaging)
    debug("Publication IPFS : %O", reponseIpfs)
    // Emettre evenement de publication

  } catch(err) {
    console.error('ERROR publication.publierRepertoireSftp %O', err)
    // Emettre evenement d'echec de publication

  } finally {
    if(message.uploadUnique) {
      // Supprimer le repertoire de staging
      await fsPromises.rm(repertoireStaging, {recursive: true})
    }
  }
}

async function publierRepertoireAwsS3(message, rk, opts) {
  debug("Publier repertoire aws s3 : %O", message)
  const {
    repertoireStaging, bucketRegion, credentialsAccessKeyId, secretAccessKey_chiffre,
    permission, bucketName, bucketDirfichier } = message

  try {
    // Connecter AWS S3
    const secretKeyInfo = {secretAccessKey: secretAccessKey_chiffre, permission}
    const s3 = await preparerConnexionS3(_mq, bucketRegion, credentialsAccessKeyId, secretKeyInfo)
    // const s3 = await preparerConnexionS3(_mq, bucketRegion, credentialsAccessKeyId, secretAccessKey_chiffre)
    const reponse = await putRepertoireAwsS3(s3, repertoireStaging, bucketName, {bucketDirfichier, message})
    debug("Fin upload AWS S3 : %O", reponse)
    // Emettre evenement de publication

  } catch(err) {
    console.error('ERROR publication.publierRepertoireSftp %O', err)
    // Emettre evenement d'echec de publication
  } finally {
    if(message.uploadUnique) {
      // Supprimer le repertoire de staging
      await fsPromises.rm(repertoireStaging, {recursive: true})
    }
  }
}

async function publierIpns(message, rk, opts) {
  debug("Publier cle ipns")
  const {cid, keyName} = message
  await publishIpns(cid, keyName)
}

async function creerCleIpns(commande, rk, opts) {
  try {
    // Creer une cle privee, chiffrer et sauvegarder (e.g. maitre des cles)
    const {nom: nomCle} = commande
    const reponse = await _creerCleIpns(_mq, nomCle)
    repondre(reponse, opts.properties)
  } catch(err) {
    // Emettre evenement d'echec
    emettreErreur(err, opts.properties)
  }
}

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

module.exports = {init, on_connecter, getPublicKey}
