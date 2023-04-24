const debug = require('debug')('backup')
const fs = require('fs')
const readdirp = require('readdirp')
const path = require('path')

async function conserverBackup(mq, consignationManager, message) {
  debug("conserverBackup commande %O", message)
  
  const { uuid_backup } = message

  // Validations de base
  const champsManquants = validerMessageBackup(message)
  if(champsManquants.length > 0) {
    return {ok: false, err: `Champs manquants dans le fichier de backup ${uuid_backup} : ${champsManquants}`}
  }

  // Sauvegarder le backup
  await consignationManager.sauvegarderBackupTransactions(message['__original'])

  return {ok: true}
}

function validerMessageBackup(message) {
  const champsObligatoires = [
    'uuid_backup', 'certificats', 'data_hachage_bytes', 'data_transactions', 'domaine', 
    'date_backup', 'date_transactions_debut', 'date_transactions_fin',
    'cle', 'format', 'iv', 'tag',  // Chiffrage
  ]
  const champsMessage = Object.keys(message)
  const champsManquants = champsObligatoires.filter(champ=>!champsMessage.includes(champ))
  
 
  return champsManquants
}

async function rotationBackupTransactions(mq, consignationManager, message) {
  debug("rotationBackupTransactions commande %O", message)
  await consignationManager.rotationBackupTransactions(message)
  return {ok: true}
}

async function getClesBackupTransactions(mq, consignationManager, message, messageProperties) {
  debug("getClesBackupTransactions (params: %O, messageProps: %O)", message, messageProperties)

  const replyTo = messageProperties.properties.replyTo

  // Parcourir liste des fichiers de backup de transaction courants, retenir les cles
  await consignationManager.getFichiersBackupTransactionsCourant(mq, replyTo)
}

async function getBackupTransaction(mq, consignationManager, message, messageProperties) {
  debug("getBackupTransaction (params: %O, messageProps: %O)", message, messageProperties)

  const pathBackupTransaction = message.fichierBackup
  const backupTransaction = await consignationManager.getBackupTransaction(pathBackupTransaction)

  const reponse = { ok: true, backup: backupTransaction }
  return reponse
}

module.exports = { conserverBackup, rotationBackupTransactions, getClesBackupTransactions, getBackupTransaction }
