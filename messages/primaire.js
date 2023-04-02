// Messages pour la consignation **primaire**
const debug = require('debug')('messages:primaire')

var _mq = null,
    _consignationManager

function init(mq, consignationManager) {
    debug("messages primaire init()")
    _mq = mq
    _consignationManager = consignationManager
}

async function startConsuming() {
    await _mq.startConsumingCustomQ('primaire')
}
  
async function stopConsuming() {
    await _mq.stopConsumingCustomQ('primaire')
}
  
function on_connecter() {
    debug("on_connecter Enregistrer 'evenement.global.cedule'")
    // ajouterCb('evenement.global.cedule', traiterCedule, {direct: true})
    // ajouterCb('evenement.grosfichiers.fuuidRecuperer', traiterFichiersRecuperes)
    ajouterCb('requete.fichiers.fuuidVerifierExistance', verifierExistanceFichiers)
    ajouterCb(`commande.fichiers.confirmerActiviteFuuids`, confirmerActiviteFuuids)
    ajouterCb('commande.fichiers.declencherSync', declencherSyncPrimaire)
}

function ajouterCb(rk, cb, opts) {
    opts = opts || {}
  
    let paramsSup = {exchange: '2.prive'}
    if(!opts.direct) paramsSup.qCustom = 'primaire'
  
    _mq.routingKeyManager.addRoutingKeyCallback(
        (routingKey, message, opts)=>{return cb(message, routingKey, opts)},
        [rk],
        paramsSup
    )
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

async function verifierExistanceFichiers(message, rk, opts) {
    opts = opts || {}
    let replyTo = null, correlationId = null
    if(opts.properties) {
        replyTo = opts.properties.replyTo;
        correlationId = opts.properties.correlationId;
    }
    if(!replyTo || !correlationId) return  // Rien a faire
    
    const fuuids = message.fuuids
    const reponse = {fuuids: {}}
    debug("verifierExistanceFichiers, fuuids : %O", fuuids)
    for(let fuuid of fuuids) {
        try {
            const infoFichier = await _consignationManager.getInfoFichier(fuuid)
            if(!infoFichier) {
                reponse.fuuids[fuuid] = false
            } else if(infoFichier.redirect) {
                throw new Error("TODO")
            } else if (infoFichier.stat) {
                const statFichier = infoFichier.stat
                reponse.fuuids[fuuid] = true  // {size: statFichier.size, ctime: Math.floor(statFichier.ctimeMs/1000)}
            }
        } catch(err) {
            debug("Erreur recuperation fichier : %O", err)
            reponse.fuuids[fuuid] = false
        }
    }
    debug('verifierExistanceFichiers, fuuids reponse : %O', reponse)

    // console.debug("Reponse a " + replyTo + ", correlation " + correlationId);
    _mq.transmettreReponse(reponse, replyTo, correlationId);
    
}

async function confirmerActiviteFuuids(message, rk, opts) {
    if(_consignationManager.estPrimaire()) {
        const fuuids = message.fuuids || []
        const archive = message.archive || false
        debug("confirmerActiviteFuuids recu - ajouter a la liste %d fuuids (archive : %s)", fuuids.length, archive)
        await _consignationManager.recevoirFuuidsDomaines(fuuids, {archive})
    }
}

async function declencherSyncPrimaire(message, rk, opts) {
    const properties = opts.properties || {}
    if(_consignationManager.estPrimaire() === true) {
      debug('declencherSyncPrimaire')
      _consignationManager.demarrerSynchronization()
        .catch(err=>console.error(new Date() + ' publication.declencherSyncPrimaire Erreur traitement ', err))
      const reponse = {ok: true}
      await _mq.transmettreReponse(reponse, properties.replyTo, properties.correlationId)
    }
}

module.exports = { init, on_connecter, startConsuming, stopConsuming }
