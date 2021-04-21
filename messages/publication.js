const debug = require('debug')('millegrilles:fichiers:publication')
const path = require('path')
const { PathConsignation } = require('../util/traitementFichier')
const { getPublicKey, connecterSSH, preparerSftp, putFichier } = require('../util/ssh')

var _mq = null,
    _pathConsignation = null

function init(mq) {
  _mq = mq

  const idmg = mq.pki.idmg
  _pathConsignation = new PathConsignation({idmg});
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
    // const localPath = '/var/opt/millegrilles/consignation/grosfichiers/z8VwJ/R6/z8VwJR6hCq6z7TJY2MjsJsfAGTkjEimw9yduR6dDnHnUf4uF7cJFJxCWKmy2tw5kpRJtgvaZCatQKu5dDbCC63fVk6t.mgs2'
    // const remotePath = './test1/soustest/z8VwJR6hCq6z7TJY2MjsJsfAGTkjEimw9yduR6dDnHnUf4uF7cJFJxCWKmy2tw5kpRJtgvaZCatQKu5dDbCC63fVk6t.mgs2'
    const remotePath = path.join(basedir, _pathConsignation.trouverPathRelatif(fuuid))
    debug("Path remote pour le fichier : %s", remotePath)
    await putFichier(sftp, localPath, remotePath)
    debug("Put fichier termine OK")

    if(properties && properties.replyTo) {
      const reponse = {ok: true}
      _mq.transmettreReponse(reponse, properties.replyTo, properties.correlationId)
    }

  } catch(err) {
    console.error("ERROR publication.publierFichierSftp: Erreur publication fichier sur sftp : %O", err)
  }
}

async function publierFichierIpfs(message) {

}

async function publierFichierAwsS3(message) {

}

module.exports = {init, on_connecter, getPublicKey}
