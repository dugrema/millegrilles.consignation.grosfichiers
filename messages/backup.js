const debug = require('debug')('messages:backup')
const forgecommon = require('@dugrema/millegrilles.utiljs/src/forgecommon')
const { conserverBackup, rotationBackupTransactions, 
  getClesBackupTransactions: getClesBackupTransactionsRun,
  getBackupTransaction: getBackupTransactionRun,
} = require('../util/traitementBackup')

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

  _mq.routingKeyManager.addRoutingKeyCallback(
    (_routingKey, message, opts)=>{return recevoirRotationBackupTransactions(message, opts)},
    ['commande.fichiers.rotationBackupTransactions'],
    { qCustom: 'backup', exchange }
  )

  _mq.routingKeyManager.addRoutingKeyCallback(
    (_routingKey, message, opts)=>{return getClesBackupTransactions(message, opts)},
    ['commande.fichiers.getClesBackupTransactions'],
    { qCustom: 'backup', exchange }
  )

  _mq.routingKeyManager.addRoutingKeyCallback(
    (_routingKey, message, opts)=>{return demarrerBackupTransactions(message, opts)},
    ['commande.fichiers.demarrerBackupTransactions'],
    { qCustom: 'backup', exchange }
  )

  _mq.routingKeyManager.addRoutingKeyCallback(
    (_routingKey, message, opts)=>{return getBackupTransaction(message, opts)},
    ['requete.fichiers.getBackupTransaction'],
    { qCustom: 'backup', exchange }
  )

}

async function recevoirConserverBackup(message, opts) {
    debug("recevoirConserverBackup, message : %O\nopts %O", message, opts)

    let reponse = {ok: false}
    try {
        reponse = await conserverBackup(_mq, _storeConsignation, message)
    } catch(err) {
        console.error("ERROR recevoirConserverBackup: %O", err)
        reponse = {ok: false, err: ''+err}
    }

    return reponse
}

async function recevoirRotationBackupTransactions(message, opts) {
  debug("recevoirRotationBackupTransactions, message : %O\nopts %O", message, opts)
  let reponse = {ok: false}
  try {
      reponse = await rotationBackupTransactions(_mq, _storeConsignation, message)
  } catch(err) {
      console.error("ERROR recevoirRotationBackupTransactions: %O", err)
      reponse = {ok: false, err: ''+err}
  }

  debug("recevoirRotationBackupTransactions reponse %O", reponse)

  return reponse
}

async function getClesBackupTransactions(message, opts) {
  debug("getClesBackupTransactions, message : %O\nopts %O", message, opts)
  let reponse = {ok: false}
  try {
      reponse = await getClesBackupTransactionsRun(_mq, _storeConsignation, message, opts)
  } catch(err) {
      console.error("ERROR getClesBackupTransactions: %O", err)
      reponse = {ok: false, err: ''+err}
  }

  debug("getClesBackupTransactions reponse %O", reponse)

  return reponse
}

async function getBackupTransaction(message, opts) {
  debug("getBackupTransaction, message : %O\nopts %O", message, opts)
  let reponse = {ok: false}
  try {
      reponse = await getBackupTransactionRun(_mq, _storeConsignation, message, opts)
  } catch(err) {
      console.error("ERROR getBackupTransaction: %O", err)
      reponse = {ok: false, err: ''+err}
  }

  debug("getBackupTransaction reponse %O", reponse)

  return reponse
}

async function demarrerBackupTransactions(message, opts) {
  debug("demarrerBackupTransactions, message : %O\nopts %O", message, opts)

  // Verifier autorisation
  const { certificat } = opts
  const { roles, niveauxSecurite, delegationGlobale } = forgecommon.extraireExtensionsMillegrille(certificat)

  if(delegationGlobale === 'proprietaire') {
    // Ok
  } else if(roles && roles.includes('instance') && niveauxSecurite.includes('3.protege')) {
    // Ok
  } else {
    debug("demarrerBackupTransactions Acces refuse (roles=%O, niveauxSecurite=%O, delegationGlobale=%O)", roles, niveauxSecurite, delegationGlobale)
    return {ok: false, err: 'Acces refuse'}
  }

  const { complet } = message

  if(complet === true) {
      debug("emettreMessagesBackup Declencher un backup complet avec rotation des archives")

      // Entretien fichiers supprimes
      _storeConsignation.entretienFichiersSupprimes()
        .catch(err=>console.error("entretien ERROR entretienFichiersSupprimes a echoue : %O", err))

      const evenement = { complet: true }
      await _mq.emettreEvenement(evenement, 'fichiers', {action: 'declencherBackup', attacherCertificat: true})
  } else {
      debug("emettreMessagesBackup Emettre trigger backup incremental")
      const evenement = { complet: false }
      await _mq.emettreEvenement(evenement, 'fichiers', {action: 'declencherBackup', attacherCertificat: true})
  }
}

module.exports = { init, on_connecter }
