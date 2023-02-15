const debug = require('debug')('messages:local')

var _mq = null,
    _storeConsignation = null,
    _instanceId = null

function init(mq, storeConsignation) {
    debug("messages local init()")
    _mq = mq
    _storeConsignation = storeConsignation
    _instanceId = mq.pki.cert.subject.getField('CN').value
}

function on_connecter() {
    ajouterCb('evenement.grosfichiers.fuuidSupprimerDocument', traiterFichiersSupprimes)
    ajouterCb('evenement.grosfichiers.fuuidRecuperer', traiterFichiersRecuperes)
    ajouterCb('evenement.fichiers.consignationPrimaire', consignationPrimaire)
    ajouterCb(`commande.fichiers.${_instanceId}.modifierConfiguration`, modifierConfiguration)
    ajouterCb(`commande.fichiers.declencherSync`, declencherSyncPrimaire)
    ajouterCb(`evenement.CoreTopologie.changementConsignationPrimaire`, changementConsignationPrimaire)
    ajouterCb(`evenement.fichiers.syncPret`, declencherSyncSecondaire)
  
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
            await _storeConsignation.supprimerFichier(fuuid)
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
            await _storeConsignation.recupererFichier(fuuid)
        } catch(err) {
            debug("Erreur recuperation fichier : %O", err)
        }
    }
}

async function consignationPrimaire(message, rk, opts) {
    if(_storeConsignation.estPrimaire())  return  // Rien a faire si primaire
    const { fuuid } = message
    _storeConsignation.ajouterDownloadPrimaire(fuuid)
}

async function emettrePresence() {
    debug("emettrePresence Configuration fichiers")
    const configuration = await _storeConsignation.chargerConfiguration()
      
    const info = {}
    for(const champ of Object.keys(configuration)) {
        if(CONST_CHAMPS_CONFIG.includes(champ)) info[champ] = configuration[champ]
    }
    
    await _mq.emettreEvenement(info, 'fichiers', {action: 'presence', attacherCertificat: true})
}
  
async function modifierConfiguration(message, rk, opts) {
    opts = opts || {}
    const properties = opts.properties || {}
    debug("publication.modifierConfiguration (replyTo: %s)", properties.replyTo)
  
    let reponse = {ok: false}
    try {
      await _storeConsignation.modifierConfiguration(message, {override: true})
  
      reponse = {ok: true}
    } catch(err) {
      console.error("%O storeConsignation.modifierConfiguration, Erreur store modifierConfiguration : %O", new Date(), err)
      reponse = {ok: false, err: ''+err}
    }
    _mq.transmettreReponse(reponse, properties.replyTo, properties.correlationId)
  
    emettrePresence()
      .catch(err=>console.error("publication.getConfiguration Erreur emission presence ", err))
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
    await _storeConsignation.setEstConsignationPrimaire(instanceIdPrimaire===_instanceId)
}
  
async function declencherSyncPrimaire(message, rk, opts) {
    const properties = opts.properties || {}
    if(_storeConsignation.estPrimaire() === true) {
      debug('declencherSyncPrimaire')
      _storeConsignation.demarrerSynchronization()
        .catch(err=>console.error(new Date() + ' publication.declencherSyncPrimaire Erreur traitement ', err))
      const reponse = {ok: true}
      _mq.transmettreReponse(reponse, properties.replyTo, properties.correlationId)
    }
}
  
async function declencherSyncSecondaire(message, rk, opts) {
    if(_storeConsignation.estPrimaire() !== true) {
      _storeConsignation.demarrerSynchronization()
        .catch(err=>console.error("publication.declencherSyncSecondaire Erreur traitement sync : %O", err))
    } else {
      debug("syncPret recu - mais on est le primaire (ignore)")
    }
}

module.exports = { init, on_connecter }
