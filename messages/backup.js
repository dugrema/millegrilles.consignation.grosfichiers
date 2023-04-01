const debug = require('debug')('messages:backup')
const forgecommon = require('@dugrema/millegrilles.utiljs/src/forgecommon')
const { conserverBackup, rotationBackupTransactions, 
  getClesBackupTransactions: getClesBackupTransactionsRun,
  getBackupTransaction: getBackupTransactionRun,
} = require('../util/traitementBackup')

var _mq = null,
    _consignationManager

function init(mq, consignationManager) {
    debug("messages backup init()")
    _mq = mq
    _consignationManager = consignationManager
}

// Appele lors d'une reconnexion MQ
function on_connecter() {
  enregistrerChannel()
}

const exchange = '2.prive',
      backupQueue = 'fichiers/backup'

async function startConsuming() {
  await _mq.startConsumingCustomQ('backup')
}

async function stopConsuming() {
  await _mq.stopConsumingCustomQ('backup')
}

function enregistrerChannel() {

  debug("backup Enregistrer channel")

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
        reponse = await conserverBackup(_mq, _consignationManager, message)
        if(_consignationManager.estPrimaire() !== true) reponse = null  // Secondaire, ne pas repondre
    } catch(err) {
        console.error("ERROR recevoirConserverBackup: %O", err)
        reponse = {ok: false, err: ''+err}
    }

    return reponse
}

async function recevoirRotationBackupTransactions(message, opts) {
  debug("recevoirRotationBackupTransactions, message : %O\nopts %O", message, opts)
  let reponse = {ok: true}
  // try {
  //     reponse = await rotationBackupTransactions(_mq, _consignationManager, message)
  //     if(_consignationManager.estPrimaire() !== true) reponse = null  // Secondaire, ne pas repondre
  // } catch(err) {
  //     console.error("ERROR recevoirRotationBackupTransactions: %O", err)
  //     reponse = {ok: false, err: ''+err}
  // }

  debug("recevoirRotationBackupTransactions reponse %O", reponse)

  return reponse
}

async function getClesBackupTransactions(message, opts) {
  debug("getClesBackupTransactions, message : %O\nopts %O", message, opts)
  let reponse = {ok: false}
  try {
      reponse = await getClesBackupTransactionsRun(_mq, _consignationManager, message, opts)
      if(_consignationManager.estPrimaire() !== true) reponse = null  // Secondaire, ne pas repondre
  } catch(err) {
      console.error("ERROR getClesBackupTransactions: %O", err)
      reponse = {ok: false, err: ''+err}
  }

  debug("getClesBackupTransactions reponse %O", reponse)

  return reponse
}

async function getBackupTransaction(message, opts) {
  if(_consignationManager.estPrimaire() !== true) return  // Skip, secondaire
  debug("getBackupTransaction, message : %O\nopts %O", message, opts)
  let reponse = {ok: false}
  try {
      reponse = await getBackupTransactionRun(_mq, _consignationManager, message, opts)
  } catch(err) {
      console.error("ERROR getBackupTransaction: %O", err)
      reponse = {ok: false, err: ''+err}
  }

  debug("getBackupTransaction reponse %O", reponse)

  return reponse
}

async function demarrerBackupTransactions(message, opts) {
  if(_consignationManager.estPrimaire() !== true) return  // Skip, secondaire
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
      const evenement = { complet: true }

      // Rotation repertoire transactions
      await _consignationManager.rotationBackupTransactions()

      await _mq.emettreEvenement(evenement, 'fichiers', {action: 'declencherBackup', attacherCertificat: true})
  } else {
      debug("emettreMessagesBackup Emettre trigger backup incremental")
      const evenement = { complet: false }
      await _mq.emettreEvenement(evenement, 'fichiers', {action: 'declencherBackup', attacherCertificat: true})
  }

  return {ok: true}
}

module.exports = { init, on_connecter, startConsuming, stopConsuming }
