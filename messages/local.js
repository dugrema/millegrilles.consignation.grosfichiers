const debug = require('debug')('messages:local')
const { getPublicKey } = require('../util/ssh')

const CONST_CHAMPS_CONFIG = ['typeStore', 'urlDownload', 'consignationUrl']

var _mq = null,
    _consignationManager = null,
    _instanceId = null

function init(mq, consignationManager) {
    debug("messages local init()")
    if(!mq || !consignationManager) throw new Error("params mq ou consignationManager null")
    _mq = mq
    _consignationManager = consignationManager
    _instanceId = mq.pki.cert.subject.getField('CN').value
}

function on_connecter() {
    ajouterCb('evenement.grosfichiers.fuuidSupprimerDocument', traiterFichiersSupprimes)
    // ajouterCb('evenement.grosfichiers.fuuidRecuperer', traiterFichiersRecuperes)
    ajouterCb('evenement.fichiers.consignationPrimaire', consignationPrimaire)
    ajouterCb('commande.fichiers.reactiverFuuids', reactiverFuuids)
    ajouterCb(`commande.fichiers.${_instanceId}.modifierConfiguration`, modifierConfiguration)
    // ajouterCb('commande.fichiers.declencherSync', declencherSyncPrimaire)
    ajouterCb('commande.fichiers.entretienBackup', entretienBackup)
    ajouterCb('evenement.CoreTopologie.changementConsignationPrimaire', changementConsignationPrimaire)
    
    // Synchronisation
    // ajouterCb(`evenement.fichiers.syncPret`, SYNCINFO)
    ajouterCb('evenement.fichiers.declencherSyncSecondaire', declencherSyncSecondaire)
  
    // Commandes SSH/SFTP
    ajouterCb('requete.fichiers.getPublicKeySsh', getPublicKeySsh)
}

function parseMessage(message) {
    try {
      const parsed = JSON.parse(message.contenu)
      parsed['__original'] = message
      return parsed
    } catch(err) {
      console.error(new Date() + ' media.parseMessage Erreur traitement %O\n%O', err, message)
    }
}
  
function ajouterCb(rk, cb, opts) {
    opts = opts || {}
  
    debug("Enregistrer callback pour rk ", rk)

    _mq.routingKeyManager.addRoutingKeyCallback(
        (routingKey, message, opts)=>{
            debug("Message recu sur rk %s : %O", rk, message)
            return cb(parseMessage(message), routingKey, opts)
        },
        [rk],
        {}
    )
}

async function traiterFichiersSupprimes(message, rk, opts) {
    const fuuids = message.fuuids
    debug("traiterFichiersSupprimes, fuuids : %O", fuuids)
    for(let fuuid of fuuids) {
        try {
            await _consignationManager.supprimerFichier(fuuid)
        } catch(err) {
            debug("Erreur suppression fichier : %O", err)
        }
    }
}

// async function traiterFichiersRecuperes(message, rk, opts) {
//     const fuuids = message.fuuids
//     debug("traiterFichiersRecuperes, fuuids : %O", fuuids)
//     for(let fuuid of fuuids) {
//         try {
//             await _consignationManager.recupererFichier(fuuid)
//         } catch(err) {
//             debug("Erreur recuperation fichier : %O", err)
//         }
//     }
// }

async function consignationPrimaire(message, rk, opts) {
    debug("Message consignation primaire (estPrimaire? %s) : %O", _consignationManager.estPrimaire(), message)
    if(_consignationManager.estPrimaire())  return  // Rien a faire si primaire
    const { fuuid } = message
    debug("Message consignation primaire ajouterDownload ", fuuid)
    _consignationManager.ajouterDownloadPrimaire(fuuid)
}

async function emettrePresence() {
    debug("emettrePresence Configuration fichiers")
    const configuration = await _consignationManager.chargerConfiguration()
      
    const info = {}
    for(const champ of Object.keys(configuration)) {
        if(CONST_CHAMPS_CONFIG.includes(champ)) info[champ] = configuration[champ]
    }
    
    await _mq.emettreEvenement(info, {domaine: 'fichiers', action: 'presence', attacherCertificat: true})
}
  
async function modifierConfiguration(message, rk, opts) {
    opts = opts || {}
    const properties = opts.properties || {}
    debug("local.modifierConfiguration (config: %s)", message)
  
    _consignationManager.modifierConfiguration(message, {override: true})
        .then(async ()=>{
            try {
              await _consignationManager.modifierConfiguration(message, {override: true})
            } catch(err) {
              console.error(new Date() + " %O storeConsignation.modifierConfiguration, Erreur store modifierConfiguration : %O", new Date(), err)
            }
            emettrePresence()
              .catch(err=>console.error("publication.getConfiguration Erreur emission presence ", err))
        })
        .catch(err=>console.error(new Date() + ' local.modifierConfiguration Erreur ', err))

    _mq.transmettreReponse({ok: true}, properties.replyTo, properties.correlationId)

}
  
function getPublicKeySsh(message, rk, opts) {
    opts = opts || {}
    const properties = opts.properties || {}
    debug("publication.getPublicKey (replyTo: %s)", properties.replyTo)
    const clePubliqueEd25519 = getPublicKey()
    const clePubliqueRsa = getPublicKey({keyType: 'rsa'})
  
    const reponse = {clePubliqueEd25519, clePubliqueRsa}
    _mq.transmettreReponse(reponse, properties.replyTo, properties.correlationId)
        .catch(err=>console.error(new Date() + " ERROR getPublicKeySSh Erreur reponse ", err))
}
  
async function changementConsignationPrimaire(message, rk, opts) {
    const instanceIdPrimaire = message.instance_id
    await _consignationManager.setEstConsignationPrimaire(instanceIdPrimaire===_instanceId, instanceIdPrimaire)
}
  
async function declencherSyncSecondaire(message, rk, opts) {
    if(_consignationManager.estPrimaire() !== true) {
      _consignationManager.demarrerSynchronization()
        .catch(err=>console.error("publication.declencherSyncSecondaire Erreur traitement sync : %O", err))
    } else {
      debug("syncPret recu - mais on est le primaire (ignore)")
    }
}

async function reactiverFuuids(message, rk, opts) {
    const properties = opts.properties || {}
    debug("reactiverFuuids Message ", message)
    const { fuuids, surEchec } = message
    const resultat = await _consignationManager.reactiverFuuids(fuuids)
    debug("reactiverFuuids Resultat ", resultat)
    if(!surEchec && resultat.recuperes.length === 0) {
        return  // Aucuns resultats a rapporter
    }
    debug("Repondre reactiverFuuids a %s / %s", properties.replyTo, properties.correlationId)
    await _mq.transmettreReponse(resultat, properties.replyTo, properties.correlationId)
}

async function entretienBackup(message, rk, opts) {
    debug("entretienBackup, message : %O\nopts %O", message, opts)
  
    let reponse = {ok: true}
  
    try {
      await _consignationManager.rotationBackupTransactions(message.uuid_backups)
    } catch(err) {
      console.error("ERROR entretienBackup: %O", err)
      reponse = {ok: false, err: ''+err}
    }
  
    debug("entretienBackup reponse %O", reponse)
    return reponse
}

  
module.exports = { init, on_connecter }
