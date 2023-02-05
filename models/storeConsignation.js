const debug = require('debug')('consignation:store:root')
const path = require('path')
const fsPromises = require('fs/promises')

const FichiersTransfertBackingStore = require('@dugrema/millegrilles.nodejs/src/fichiersTransfertBackingstore')

const StoreConsignationLocal = require('./storeConsignationLocal')
const StoreConsignationSftp = require('./storeConsignationSftp')

const BATCH_SIZE = 100
const CONST_CHAMPS_CONFIG = ['typeStore', 'urlDownload', 'consignationUrl']

let _mq = null,
    _storeConsignation = null,
    _storeConsignationLocal = null

async function init(mq, opts) {
    opts = opts || {}
    _mq = mq

    // Toujours initialiser le type local - utilise pour stocker/charger la configuration
    _storeConsignationLocal = StoreConsignationLocal
    _storeConsignationLocal.init(opts)
    const configuration = await _storeConsignationLocal.chargerConfiguration(opts)
    const typeStore = configuration.typeStore

    const params = {...configuration, ...opts}  // opts peut faire un override de la configuration

    FichiersTransfertBackingStore.configurerThreadPutFichiersConsignation(
        mq, 
        {...opts, consignerFichier: transfererFichierVersConsignation}
    )

    await changerStoreConsignation(typeStore, params)

    // Emettre la presence (premiere apres 10 secs, apres sous intervalles)
    setTimeout(emettrePresence, 10_000)
    setInterval(emettrePresence, 180_000)
}

async function changerStoreConsignation(typeStore, params, opts) {
    opts = opts || {}
    params = params || {}
    typeStore = typeStore?typeStore.toLowerCase():'local'
    debug("changerStoreConsignation type: %s, params: %O", typeStore, params)

    if(_storeConsignation && _storeConsignation.fermer) await _storeConsignation.fermer()

    switch(typeStore) {
        case 'sftp': _storeConsignation = StoreConsignationSftp; break
        case 'awss3': throw new Error('todo'); break
        case 'millegrille': _storeConsignation = _storeConsignationLocal; break
        default: _storeConsignation = _storeConsignationLocal
    }

    // Changer methode de consignation
    await _storeConsignation.init(params)

    await _storeConsignationLocal.modifierConfiguration({...params, typeStore})
}

async function chargerConfiguration(opts) {
    opts = opts || {}
    return await _storeConsignationLocal.chargerConfiguration(opts)
}

async function modifierConfiguration(params, opts) {
    if(params.typeStore) {
        return await changerStoreConsignation(params.typeStore, params, opts)
    }
    return await _storeConsignationLocal.modifierConfiguration(params, opts)
}

async function transfererFichierVersConsignation(mq, pathReady, item) {
    const transactions = await FichiersTransfertBackingStore.traiterTransactions(mq, pathReady, item)
    const {etat, transaction: transactionGrosFichiers, cles: commandeMaitreCles} = transactions
    
    // const fuuid = commandeMaitreCles.hachage_bytes
    const fuuid = etat.hachage

    // Conserver cle
    if(commandeMaitreCles) {
        // Transmettre la cle
        debug("Transmettre commande cle pour le fichier: %O", commandeMaitreCles)
        try {
            await mq.transmettreEnveloppeCommande(commandeMaitreCles)
        } catch(err) {
            console.error("%O ERROR Erreur sauvegarde cle fichier %s : %O", new Date(), fuuid, err)
            return
        }
    }

    // Conserver le fichier
    const pathFichierStaging = path.join(pathReady, item)
    try {
        await _storeConsignation.consignerFichier(pathFichierStaging, fuuid)
    } catch(err) {
        console.error("%O ERROR Erreur consignation fichier : %O", new Date(), err)
        return
    }

    // Conserver transaction contenu (grosfichiers)
    // Note : en cas d'echec, on laisse le fichier en place. Il sera mis dans la corbeille automatiquement au besoin.
    if(transactionGrosFichiers) {
        debug("Transmettre commande fichier nouvelleVersion : %O", transactionGrosFichiers)
        try {
            const domaine = transactionGrosFichiers['en-tete'].domaine
            const reponseGrosfichiers = await mq.transmettreEnveloppeCommande(transactionGrosFichiers, domaine, {exchange: '2.prive'})
            debug("Reponse message grosFichiers : %O", reponseGrosfichiers)
        } catch(err) {
            console.error("%O ERROR Erreur sauvegarde fichier (commande) %s : %O", new Date(), fuuid, err)
            return
        }
    }

    // Emettre un evenement de consignation, peut etre utilise par domaines connexes (e.g. messagerie)
    try {
        const domaine = 'fichiers',
              action = 'consigne'
        const contenu = { 'hachage_bytes': etat.hachage }
        mq.emettreEvenement(contenu, domaine, {action, exchange: '2.prive'}).catch(err=>{
            console.error("%O ERROR Erreur Emission evenement nouveau fichier %s : %O", new Date(), fuuid, err)
        })
    } catch(err) {
        console.error("%O ERROR Erreur Emission evenement nouveau fichier %s : %O", new Date(), fuuid, err)
    }

    // Le fichier a ete transfere avec succes (aucune exception)
    // On peut supprimer le repertoire ready local
    fsPromises.rm(pathFichierStaging, {recursive: true})
        .catch(err=>console.error("Erreur suppression repertoire %s apres consignation reussie : %O", fuuid, err))

}

var _batchFichiersFuuids = null, // Dict { [fuuid]: false/{fuuid, supprime: bool} }
    _triggerPromiseBatch = null  // Fonction, invoquer pour continuer batch avant timeout (e.g. all files accounted for)

async function entretienFichiersSupprimes() {
    debug("Debut entretien des fichiers supprimes")

    // Detecter les fichiers qui devraient etre mis en attente de suppression    
    await traiterSupprimer()

    // Verifier les fichiers dans la corbeille (pour les recuperer au besoin)
    traiterRecuperer()

    // Supprimer les fichiers en attente depuis plus de 14 jours

}

async function traiterSupprimer() {
    debug("Traitement des fichiers a supprimer")
    let batchFichiers = []
    
    const callbackActionSupprimer = async item => {
        const {fuuid, supprime} = item
        if(supprime === true) {
            debug("Le fichier %s est supprime, on le deplace vers la corbeille", fuuid)
            await _storeConsignation.marquerSupprime(fuuid)
        }
    }

    const callbackTraiterFichiersASupprimer = async item => {
        if(!item) {
            // Derniere batch
            if(batchFichiers.length > 0) await traiterBatch(batchFichiers, callbackActionSupprimer)
        } else {
            batchFichiers.push(item.filename)
            while(batchFichiers.length > BATCH_SIZE) {
                const batchCourante = batchFichiers.slice(0, BATCH_SIZE)
                batchFichiers = batchFichiers.slice(BATCH_SIZE)
                await traiterBatch(batchCourante, callbackActionSupprimer)
            }
        }
    }
    
    try {
        const filtre = item => !item.filename.endsWith('.corbeille')
        await _storeConsignation.parourirFichiers(callbackTraiterFichiersASupprimer, {filtre})
    } catch(err) {
        console.error(new Date() + " ERROR traiterRecuperer() : %O", err)
    }
}

async function traiterRecuperer() {
    debug("Traitement des fichiers a recuperer")
    let batchFichiers = []
    
    const callbackActionRecuperer = async item => {
        const {fuuid, supprime} = item
        if(supprime === false) {
            debug("Le fichier supprime %s est requis par un module, on le recupere", fuuid)
            await _storeConsignation.recoverFichierSupprime(fuuid)
        }
    }

    const callbackTraiterFichiersARecuperer = async item => {
        if(!item) {
            // Derniere batch
            if(batchFichiers.length > 0) await traiterBatch(batchFichiers, callbackActionRecuperer)
        } else {
            batchFichiers.push(path.basename(item.filename, '.corbeille'))
            while(batchFichiers.length > BATCH_SIZE) {
                const batchCourante = batchFichiers.slice(0, BATCH_SIZE)
                batchFichiers = batchFichiers.slice(BATCH_SIZE)
                await traiterBatch(batchCourante, callbackActionRecuperer)
            }
        }
    }
    
    try {
        const filtre = item => item.filename.endsWith('.corbeille')
        await _storeConsignation.parourirFichiers(callbackTraiterFichiersARecuperer, {filtre})
    } catch(err) {
        console.error(new Date() + " ERROR traiterRecuperer() : %O", err)
    }
}

async function traiterBatch(fuuids, callbackAction) {
    debug("Traiter batch : %O", fuuids)

    _batchFichiersFuuids = fuuids.reduce((acc, item)=>{
        acc[item]=false 
        return acc
    }, {})
    debug("Traiter batch fichiers : %O", _batchFichiersFuuids)

    const evenement = { fuuids }
    //const domaine = 'GrosFichiers',
    const action = 'confirmerEtatFuuids',
          domaine = 'fichiers'
    // const reponse = await _mq.transmettreRequete(domaine, requete, {action})
    await _mq.emettreEvenement(evenement, domaine, {action})

    // Attendre reponses, timeout de 10 secondes pour collecter tous les messages
    await new Promise(resolve=>{
        let timeoutBatch = setTimeout(resolve, 10000)
        _triggerPromiseBatch = () => {
            clearTimeout(timeoutBatch)
            resolve()
        }
    })

    const resultatBatch = _batchFichiersFuuids
    _batchFichiersFuuids = null
    _triggerPromiseBatch = null
    debug("Resultat verification : %O", resultatBatch)

    // Reassembler resultat
    const resultatFuuids = {}
    for(const fuuid in resultatBatch) {
        const resultat = resultatBatch[fuuid]
        if(resultat === false) {
            resultatFuuids[fuuid] = {fuuid, supprime: true}
        } else {
            resultatFuuids[fuuid] = resultat
        }
    }
    
    const resultatListe = Object.values(resultatFuuids)

    debug("Appliquer callback a liste : %O", resultatListe)

    for await (const reponseFichier of resultatListe) {
        await callbackAction(reponseFichier)
    }

}

// Callback via commande pour que multiple domaines/modules puissent confirmer leur utilisation
// courante de fichiers
async function confirmerActiviteFuuids(fuuids) {
    debug("confirmerActiviteFuuids fuuids %O", fuuids)
    if(fuuids) {
        fuuids.forEach(item=>{
            _batchFichiersFuuids[item.fuuid] = item
        })
    }
    // debug("Liste fuuids locale : %O", _batchFichiersFuuids)

    // Detecter si la liste est complete
    let complete = Object.values(_batchFichiersFuuids).reduce((acc, item)=>{
        acc = acc && item?true:false
        return acc
    }, true)
    if(complete) {
        debug("Liste fichiers est complete : %s", complete)
        _triggerPromiseBatch()
    }
}

async function emettrePresence() {
    try {
        debug("emettrePresence Configuration fichiers")
        const configuration = await chargerConfiguration()
        
        const info = {}
        for(const champ of Object.keys(configuration)) {
            if(CONST_CHAMPS_CONFIG.includes(champ)) info[champ] = configuration[champ]
        }
        
        await _mq.emettreEvenement(info, 'fichiers', {action: 'presence', attacherCertificat: true})
    } catch(err) {
        console.error("storeConsignation.emettrePresence Erreur emission presence : ", err)
    }
}

function supprimerFichier(fuuid) {
    return _storeConsignation.marquerSupprime(fuuid)
}

function recupererFichier(fuuid) {
    return _storeConsignation.recoverFichierSupprime(fuuid)
}

function getInfoFichier(fuuid, opts) {
    opts = opts || {}
    return _storeConsignation.getInfoFichier(fuuid, opts)
}

function middlewareRecevoirFichier(opts) {
    return FichiersTransfertBackingStore.middlewareRecevoirFichier(opts)
}

function middlewareReadyFichier(amqpdao, opts) {
    return FichiersTransfertBackingStore.middlewareReadyFichier(amqpdao, opts)
}

function middlewareDeleteStaging(opts) {
    return FichiersTransfertBackingStore.middlewareDeleteStaging(opts)
}

function sauvegarderBackupTransactions(message) {
    return _storeConsignation.sauvegarderBackupTransactions(message)
}

function rotationBackupTransactions(message) {
    return _storeConsignation.rotationBackupTransactions(message)
}

function getFichiersBackupTransactionsCourant(mq, replyTo) {
    return _storeConsignation.getFichiersBackupTransactionsCourant(mq, replyTo)
}

function getBackupTransaction(pathBackupTransaction) {
    return _storeConsignation.getBackupTransaction(pathBackupTransaction)
}

module.exports = { 
    init, changerStoreConsignation, chargerConfiguration, modifierConfiguration, getInfoFichier,
    entretienFichiersSupprimes, supprimerFichier, recupererFichier, confirmerActiviteFuuids,
    middlewareRecevoirFichier, middlewareReadyFichier, middlewareDeleteStaging, 
    sauvegarderBackupTransactions, rotationBackupTransactions,
    getFichiersBackupTransactionsCourant, getBackupTransaction,
}
