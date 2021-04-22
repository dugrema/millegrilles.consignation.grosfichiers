const debug = require('debug')('millegrilles:fichiers:publication')
const path = require('path')
const { PathConsignation } = require('../util/traitementFichier')
const { getPublicKey, connecterSSH, preparerSftp, putFichier: putFichierSsh } = require('../util/ssh')
const { init: initIpfs, addFichier: addFichierIpfs } = require('../util/ipfs')

var _mq = null,
    _pathConsignation = null

function init(mq) {
  _mq = mq

  const idmg = mq.pki.idmg
  _pathConsignation = new PathConsignation({idmg});

  initIpfs('http://192.168.2.131:5001')
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

    const conn = await connecterSSH(host, port, username)
    const sftp = await preparerSftp(conn)
    debug("Connexion SSH et SFTP OK")

    const localPath = _pathConsignation.trouverPathLocal(fuuid)
    debug("Fichier local a publier sur SSH : %s", localPath)

    const remotePath = path.join(basedir, _pathConsignation.trouverPathRelatif(fuuid))
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

    const localPath = _pathConsignation.trouverPathLocal(fuuid)
    debug("Fichier local a publier sur IPFS : %s", localPath)

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

async function publierFichierAwsS3(message) {

}

module.exports = {init, on_connecter, getPublicKey}
