const debug = require('debug')('messages:entretien')

var _mq = null,
    _storeConsignation

function init(mq, storeConsignation) {
    debug("entretien init()")
    _mq = mq
    _storeConsignation = storeConsignation
}

function on_connecter() {
    debug("entretien Enregistrer 'evenement.global.cedule'")
    ajouterCb('evenement.global.cedule', traiterCedule, {direct: true})
}

function ajouterCb(rk, cb, opts) {
    opts = opts || {}
  
    let paramsSup = {exchange: '2.prive'}
    if(!opts.direct) paramsSup.qCustom = 'publication'
  
    _mq.routingKeyManager.addRoutingKeyCallback(
        (routingKey, message, opts)=>{return cb(message, routingKey, opts)},
        [rk],
        paramsSup
    )
}

function traiterCedule(message, rk, opts) {
    const { flag_heure, flag_jour, flag_mois, estampille } = message
    debug("Traiter cedule, message : %s", message.date_string)


    if( flag_jour ) {
        // Entretien fichiers supprimes
        _storeConsignation.entretienFichiersSupprimes()
            .catch(err=>console.error("entretien ERROR entretienFichiersSupprimes a echoue : %O", err))
    }

}

module.exports = { init, on_connecter }
