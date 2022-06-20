const debug = require('debug')('messages:backup')
const { conserverBackup } = require('../util/traitementBackup')

var _mq = null,
    _storeConsignation

function init(mq, storeConsignation) {
    debug("messages backup init()")
    _mq = mq
    _storeConsignation = storeConsignation
}

// Appele lors d'une reconnexion MQ
function on_connecter() {
  enregistrerChannel()
}

function enregistrerChannel() {

  const exchange = '2.prive'
  _mq.routingKeyManager.addRoutingKeyCallback(
    (_routingKey, message, opts)=>{return recevoirConserverBackup(message, opts)},
    ['commande.fichiers.backupTransactions'],
    { qCustom: 'backup', exchange }
  )

}

async function recevoirConserverBackup(message, opts) {
    const {replyTo, correlationId} = opts.properties
    debug("recevoirConserverBackup, message : %O\nopts %O", message, opts)
    let reponse = {ok: false}
    try {
        reponse = await conserverBackup(_mq, _storeConsignation, message)
    } catch(err) {
        console.error("ERROR recevoirConserverBackup: %O", err)
        reponse = {ok: false, err: ''+err}
    }

    // await _mq.transmettreReponse(reponse, replyTo, correlationId, {ajouterCertificat: true})
    return reponse
}

module.exports = { init, on_connecter }
