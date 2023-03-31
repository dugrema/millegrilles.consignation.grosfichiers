const debug = require('debug')('messages:local')
const { getPublicKey } = require('../util/ssh')

const CONST_CHAMPS_CONFIG = ['typeStore', 'urlDownload', 'consignationUrl']

var _mq = null,
    _consignationManager = null,
    _instanceId = null

function init(mq, consignationManager) {
    debug("messages local init()")
    _mq = mq
    _consignationManager = consignationManager
    _instanceId = mq.pki.cert.subject.getField('CN').value
}

function on_connecter() {
    ajouterCb('evenement.grosfichiers.fuuidSupprimerDocument', traiterFichiersSupprimes)
    ajouterCb('evenement.grosfichiers.fuuidRecuperer', traiterFichiersRecuperes)
    ajouterCb('evenement.fichiers.consignationPrimaire', consignationPrimaire)
    ajouterCb(`commande.fichiers.${_instanceId}.modifierConfiguration`, modifierConfiguration)
    ajouterCb(`commande.fichiers.declencherSync`, declencherSyncPrimaire)
    ajouterCb(`evenement.CoreTopologie.changementConsignationPrimaire`, changementConsignationPrimaire)

    // Synchronisation
    // ajouterCb(`evenement.fichiers.syncPret`, SYNCINFO)
    ajouterCb(`evenement.fichiers.declencherSyncSecondaire`, declencherSyncSecondaire)
  
    // Commandes SSH/SFTP
    ajouterCb('requete.fichiers.getPublicKeySsh', getPublicKeySsh)
}

function ajouterCb(rk, cb, opts) {
    opts = opts || {}
  
    _mq.routingKeyManager.addRoutingKeyCallback(
        (routingKey, message, opts)=>{return cb(message, routingKey, opts)},
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

async function traiterFichiersRecuperes(message, rk, opts) {
    const fuuids = message.fuuids
    debug("traiterFichiersRecuperes, fuuids : %O", fuuids)
    for(let fuuid of fuuids) {
        try {
            await _consignationManager.recupererFichier(fuuid)
        } catch(err) {
            debug("Erreur recuperation fichier : %O", err)
        }
    }
}

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
    
    await _mq.emettreEvenement(info, 'fichiers', {action: 'presence', attacherCertificat: true})
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
}
  
async function changementConsignationPrimaire(message, rk, opts) {
    const instanceIdPrimaire = message.instance_id
    await _consignationManager.setEstConsignationPrimaire(instanceIdPrimaire===_instanceId, instanceIdPrimaire)
}
  
async function declencherSyncPrimaire(message, rk, opts) {
    const properties = opts.properties || {}
    if(_consignationManager.estPrimaire() === true) {
      debug('declencherSyncPrimaire')
      _consignationManager.demarrerSynchronization()
        .catch(err=>console.error(new Date() + ' publication.declencherSyncPrimaire Erreur traitement ', err))
      const reponse = {ok: true}
      _mq.transmettreReponse(reponse, properties.replyTo, properties.correlationId)
    }
}
  
async function declencherSyncSecondaire(message, rk, opts) {
    if(_consignationManager.estPrimaire() !== true) {
      _consignationManager.demarrerSynchronization()
        .catch(err=>console.error("publication.declencherSyncSecondaire Erreur traitement sync : %O", err))
    } else {
      debug("syncPret recu - mais on est le primaire (ignore)")
    }
}

// async function confirmerActiviteFuuids(message, rk, opts) {
//     if(_storeConsignation.estPrimaire()) {
//         const fuuids = message.fuuids || []
//         const archive = message.archive || false
//         debug("confirmerActiviteFuuids recu - ajouter a la liste %d fuuids (archive : %s)", fuuids.length, archive)
//         await _storeConsignation.recevoirFuuidsDomaines(fuuids, {archive})
//     }
// }

module.exports = { init, on_connecter }
