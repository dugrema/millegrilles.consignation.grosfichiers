const debug = require('debug')('millegrilles:fichiers:publication')
const path = require('path')
const { PathConsignation } = require('../util/traitementFichier')
const { getPublicKey, connecterSSH, preparerSftp, putFichier: putFichierSsh } = require('../util/ssh')
const { init: initIpfs, addFichier: addFichierIpfs } = require('../util/ipfs')
const { preparerConnexionS3, uploaderFichier: putFichierAwsS3 } = require('../util/awss3')
const { creerStreamDechiffrage, stagingFichier: stagingPublic } = require('../util/publicStaging')

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
  _ajouterCb('commande.fichiers.publierFichierSftp', publierFichierSftp)
  _ajouterCb('commande.fichiers.publierFichierIpfs', publierFichierIpfs)
  _ajouterCb('commande.fichiers.publierFichierAwsS3', publierFichierAwsS3)
  _ajouterCb('requete.fichiers.getPublicKeySsh', getPublicKeySsh, {direct: true})
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
  try {
    const {host, port, username, fuuid} = message
    const basedir = message.basedir || './'
    const properties = opts.properties || {}
    const securite = message.securite || '3.protege'

    var localPath = _pathConsignation.trouverPathLocal(fuuid)
    debug("Fichier local a publier sur SSH : %s", localPath)

    if(securite === '1.public') {
      // Dechiffrer le fichier public dans staging
      const infoFichierPublic = await preparerStagingPublic(fuuid)
      debug("Information fichier public : %O", infoFichierPublic)
      localPath = infoFichierPublic.filePath
    }

    const conn = await connecterSSH(host, port, username)
    const sftp = await preparerSftp(conn)
    debug("Connexion SSH et SFTP OK")

    const remotePath = path.join(basedir, _pathConsignation.trouverPathRelatif(fuuid, {mimetype: message.mimetype}))
    debug("Path remote pour le fichier : %s", remotePath)

    await putFichierSsh(sftp, localPath, remotePath)
    debug("Put fichier ssh OK")

    if(properties && properties.replyTo) {
      _mq.transmettreReponse({ok: true}, properties.replyTo, properties.correlationId)
    }

  } catch(err) {
    console.error("ERROR publication.publierFichierSftp: Erreur publication fichier sur sftp : %O", err)
    if(properties && properties.replyTo) {
      _mq.transmettreReponse({ok: false, err: ''+err}, properties.replyTo, properties.correlationId)
    }
  }
}

async function publierFichierIpfs(message, rk, opts) {
  opts = opts || {}
  try {
    const {fuuid} = message
    const properties = opts.properties || {}
    const securite = message.securite || '3.protege'

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

  } catch(err) {
    console.error("ERROR publication.publierFichierIpfs: Erreur publication fichier sur ipfs : %O", err)
    if(properties && properties.replyTo) {
      _mq.transmettreReponse({ok: false, err: ''+err}, properties.replyTo, properties.correlationId)
    }
  }
}

async function publierFichierAwsS3(message, rk, opts) {
  opts = opts || {}
  const properties = opts.properties || {}
  try {
    const {fuuid, bucketRegion, credentialsAccessKeyId, secretAccessKey, bucketName, bucketDirfichier} = message
    const securite = message.securite || '3.protege'

    // Connecter AWS S3
    const s3 = await preparerConnexionS3(bucketRegion, credentialsAccessKeyId, secretAccessKey)

    var localPath = _pathConsignation.trouverPathLocal(fuuid)
    debug("Fichier local a publier sur IPFS : %s", localPath)

    if(securite === '1.public') {
      // Dechiffrer le fichier public dans staging
      const infoFichierPublic = await preparerStagingPublic(fuuid)
      debug("Information fichier public : %O", infoFichierPublic)
      localPath = infoFichierPublic.filePath
    }

    const progressCb = update => {
      debug("Progress S3 : %O", update)
    }

    debug("Debut upload AWS S3 %s vers %s", fuuid, bucketName)
    const resultat = await putFichierAwsS3(s3, message, localPath, bucketName, bucketDirfichier, {progressCb})
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

  } catch(err) {
    console.error("ERROR publication.publierFichierAwsS3: Erreur publication fichier sur AWS S3 : %O", err)
    if(properties && properties.replyTo) {
      _mq.transmettreReponse({ok: false, err: ''+err}, properties.replyTo, properties.correlationId)
    }
  }
}

async function preparerStagingPublic(fuuid) {
  // Dechiffrer un fichier public dans zone de downloadStaging (auto-cleanup)

  const infoStream = await creerStreamDechiffrage(_mq, fuuid)
  if(infoStream.acces === '0.refuse') {
    debug("Permission d'acces refuse en mode %s pour %s", niveauAcces, fuuid)
    return res.sendStatus(403)  // Acces refuse
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

module.exports = {init, on_connecter, getPublicKey}