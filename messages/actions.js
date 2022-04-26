const debug = require('debug')('messages:actions')

var _mq = null,
    _storeConsignation

function init(mq, storeConsignation) {
    debug("messages actions init()")
    _mq = mq
    _storeConsignation = storeConsignation
}

function on_connecter() {
    debug("on_connecter Enregistrer 'evenement.global.cedule'")
    // ajouterCb('evenement.global.cedule', traiterCedule, {direct: true})
    ajouterCb('evenement.grosfichiers.fuuidSupprimerDocument', traiterFichiersSupprimes)
    ajouterCb('evenement.grosfichiers.fuuidRecuperer', traiterFichiersRecuperes)
}

function ajouterCb(rk, cb, opts) {
    opts = opts || {}
  
    let paramsSup = {exchange: '2.prive'}
    if(!opts.direct) paramsSup.qCustom = 'actions'
  
    _mq.routingKeyManager.addRoutingKeyCallback(
        (routingKey, message, opts)=>{return cb(message, routingKey, opts)},
        [rk],
        paramsSup
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

module.exports = { init, on_connecter }
