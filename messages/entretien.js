const debug = require('debug')('messages:entretien')

var _mq = null,
    _consignationManager

function init(mq, consignationManager) {
    debug("entretien init()")
    _mq = mq
    _consignationManager = consignationManager
}

function on_connecter() {
    debug("entretien Enregistrer 'evenement.global.cedule'")
    ajouterCb('evenement.global.cedule', traiterCedule, {direct: true})
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
  
    let paramsSup = {exchange: '2.prive'}
    if(!opts.direct) paramsSup.qCustom = 'publication'
  
    _mq.routingKeyManager.addRoutingKeyCallback(
        (routingKey, message, opts)=>{return cb(parseMessage(message), routingKey, opts)},
        [rk],
        paramsSup
    )
}

function traiterCedule(message, rk, opts) {
    const { flag_heure, flag_jour, flag_mois, estampille } = message
    debug("Traiter cedule, message : %s", message.date_string)


    // if( flag_jour ) {
    //     // Entretien fichiers supprimes
    //     _consignationManager.entretienFichiersSupprimes()
    //         .catch(err=>console.error("entretien ERROR entretienFichiersSupprimes a echoue : %O", err))
    // }

    // Evenement trigger de backup
    emettreMessagesBackup(message).catch(err=>debug("Erreur emettre messages backup : %O", err))

}

async function emettreMessagesBackup(message) {
    const { estampille } = message
    const date = new Date(estampille * 1000)
    const hours = date.getHours()
    const minutes = date.getMinutes()
    const dow = date.getDay()

    if(dow === 0 && hours === 4) {
        debug("emettreMessagesBackup Emettre trigger backup complet, dimanche 4:00")
        const evenement = { complet: true }
        await _mq.emettreEvenement(evenement, {domaine: 'fichiers', action: 'declencherBackup', attacherCertificat: true})
    } else if(minutes % 20 === 0) {
        debug("emettreMessagesBackup Emettre trigger backup incremental")
        const evenement = { complet: false }
        await _mq.emettreEvenement(evenement, {domaine: 'fichiers', action: 'declencherBackup', attacherCertificat: true})
    }
}

module.exports = { init, on_connecter }
