const debug = require('debug')('consignation:store:root')
const path = require('path')
const fsPromises = require('fs/promises')

const FichiersTransfertBackingStore = require('@dugrema/millegrilles.nodejs/src/fichiersTransfertBackingstore')

const StoreConsignationLocal = require('./storeConsignationLocal')
const StoreConsignationSftp = require('./storeConsignationSftp')

const BATCH_SIZE = 100

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
        'https://localhost', mq, 
        {...opts, consignerFichier: transfererFichierVersConsignation}
    )

    await changerStoreConsignation(typeStore, params)
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
        case 'local': _storeConsignation = _storeConsignationLocal; break
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
    const {transaction: transactionGrosFichiers, cles: commandeMaitreCles} = transactions
    const fuuid = commandeMaitreCles.hachage_bytes

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
            console.error("%O ERROR Erreur sauvegarde cle fichier %s : %O", new Date(), fuuid, err)
            return
        }
    }

    // Le fichier a ete transfere avec succes (aucune exception)
    // On peut supprimer le repertoire ready local
    fsPromises.rm(pathFichierStaging, {recursive: true})
        .catch(err=>console.error("Erreur suppression repertoire %s apres consignation reussie : %O", fuuid, err))

}

async function entretienFichiersSupprimes() {
    debug("Debut entretien des fichiers supprimes")

    // Detecter les fichiers qui devraient etre mis en attente de suppression
    let batchFichiers = []
    const callbackTraiterFichiers = async item => {
        if(!item) {
            // Derniere batch
            if(batchFichiers.length > 0) await traiterBatch(batchFichiers)
        } else {
            batchFichiers.push(item)
            while(batchFichiers.length > BATCH_SIZE) {
                const batchCourante = batchFichiers.slice(0, BATCH_SIZE)
                batchFichiers = batchFichiers.slice(BATCH_SIZE)
                await traiterBatch(batchCourante)
            }
        }
    }
    const filtre = item => !item.filename.endsWith('.corbeille')
    await _storeConsignation.parourirFichiers(callbackTraiterFichiers, {filtre})

    // Supprimer les fichiers en attente depuis plus de 14 jours

}

async function traiterBatch(batchFichiers) {
    debug("Traiter batch : %O", batchFichiers)

    const fuuids = batchFichiers.map(item=>item.filename)

    const requete = { fuuids }
    const domaine = 'GrosFichiers',
          action = 'confirmerEtatFuuids'
    const reponse = await _mq.transmettreRequete(domaine, requete, {action})
    debug("Reponse verification : %O", reponse.confirmation)

    const confirmation = reponse.confirmation || {},
          fichiersResultat = confirmation.fichiers || []

    debug("Reponse verification : %O", confirmation)

    if(fichiersResultat) {
        for await (const reponseFichier of fichiersResultat) {
            const {fuuid, supprime} = reponseFichier
            if(supprime) {
                debug("Le fichier %s est supprime, on le deplace vers la corbeille", fuuid)
                await _storeConsignation.marquerSupprime(fuuid)
            }
        }
    }

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

module.exports = { 
    init, changerStoreConsignation, chargerConfiguration, modifierConfiguration, getInfoFichier,
    entretienFichiersSupprimes,
    middlewareRecevoirFichier, middlewareReadyFichier, middlewareDeleteStaging, 
}
