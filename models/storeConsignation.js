const debug = require('debug')('consignation:store:root')
const path = require('path')
const fsPromises = require('fs/promises')
const fs = require('fs')
const readline = require('readline')
const axios = require('axios')
const { exec } = require('child_process')

const FichiersTransfertBackingStore = require('@dugrema/millegrilles.nodejs/src/fichiersTransfertBackingstore')

const StoreConsignationLocal = require('./storeConsignationLocal')
const StoreConsignationSftp = require('./storeConsignationSftp')
const StoreConsignationAwsS3 = require('./storeConsignationAwsS3')

const TransfertPrimaire = require('./transfertPrimaire')
const { dechiffrerConfiguration } = require('./pki')

const { startConsuming: startConsumingBackup, stopConsuming: stopConsumingBackup } = require('../messages/backup')
const { startConsuming: startConsumingActions, stopConsuming: stopConsumingActions } = require('../messages/actions')

const BATCH_SIZE = 100
const CONST_CHAMPS_CONFIG = ['type_store', 'url_download', 'consignation_url']
const INTERVALLE_SYNC = 1_800_000,  // 30 minutes
      DOWNLOAD_PRIMAIRE = 60_000  // 1 minute

var _mq = null,
    _storeConsignation = null,
    // _storeConsignationLocal = null,
    _estPrimaire = false,
    _sync_lock = false,
    _derniere_sync = 0,
    _transfertPrimaire = null,
    _queueDownloadFuuids = new Set(),
    _timeoutStartThreadDownload = null,
    _intervalleSync = INTERVALLE_SYNC,
    _syncActif = true

async function init(mq, opts) {
    opts = opts || {}
    _mq = mq

    // Toujours initialiser le type local - utilise pour stocker/charger la configuration
    _storeConsignation = StoreConsignationLocal
    _storeConsignation.init(opts)
    const configuration = await _storeConsignation.chargerConfiguration(opts)
    const typeStore = configuration.type_store

    const params = {...configuration, ...opts}  // opts peut faire un override de la configuration

    // Configuration thread
    // Pour consignationFichiers, toujours faire un lien vers la consignation primaire
    FichiersTransfertBackingStore.configurerThreadPutFichiersConsignation(
        mq, 
        {...opts, consignerFichier: transfererFichierVersConsignation}
    )

    await changerStoreConsignation(typeStore, params)

    // Objet responsable de l'upload vers le primaire (si local est secondaire)
    _transfertPrimaire = new TransfertPrimaire(mq, this)
    _transfertPrimaire.threadPutFichiersConsignation()  // Premiere run, initialise loop

    // Entretien - emet la presence (premiere apres 10 secs, apres sous intervalles)
    setTimeout(entretien, 10_000)
    setInterval(entretien, 180_000)
    _threadDownloadFichiersDuPrimaire()
        .catch(err=>console.error("init Erreur initialisation _threadDownloadFichiersDuPrimaire ", err))
}

async function changerStoreConsignation(typeStore, params, opts) {
    opts = opts || {}
    params = params || {}
    typeStore = typeStore?typeStore.toLowerCase():'millegrille'
    debug("changerStoreConsignation type: %s, params: %O", typeStore, params)

    if(params.sync_actif !== undefined) {
        _syncActif = params.sync_actif
        debug("changerStoreConsignation Set sync actif ", _syncActif)
    }
    if(params.sync_intervalle !== undefined) {
        _intervalleSync = params.sync_intervalle
        debug("changerStoreConsignation Set sync intervalle %d secs", _intervalleSync)
    }

    if(_storeConsignation && _storeConsignation.fermer) await _storeConsignation.fermer()

    switch(typeStore) {
        case 'sftp': _storeConsignation = StoreConsignationSftp; break
        case 'awss3': _storeConsignation = StoreConsignationAwsS3; break
        case 'local':
        case 'millegrille': 
            _storeConsignation = StoreConsignationLocal
            break
        default: _storeConsignation = StoreConsignationLocal
    }

    // Changer methode de consignation
    await _storeConsignation.init(params)

    await _storeConsignation.modifierConfiguration({...params, type_store: typeStore})
}

async function chargerConfiguration(opts) {
    opts = opts || {}

    // Recuperer configuration a partir de CoreTopologie
    try {
        const instanceId = _mq.pki.cert.subject.getField('CN').value
        const requete = {instance_id: instanceId}
        const action = 'getConsignationFichiers'
        const configuration = await _mq.transmettreRequete('CoreTopologie', requete, {action, exchange: '2.prive'})
        // await _mq.transmettreRequete({instance_id: FichiersTransfertBackingStore.getInstanceId()})
        debug("Configuration recue ", configuration)

        const data_chiffre = configuration.data_chiffre
        if(data_chiffre) {
            // Dechiffrer configuration - mis dans configuration.data_dechiffre
            configuration.data_dechiffre = await dechiffrerConfiguration(_mq, configuration)
        }

        await _storeConsignation.modifierConfiguration(configuration, {override: true})
        return configuration
    } catch(err) {
        console.warn("Erreur chargement configuration via CoreTopologie, chargement local ", err)
        return await _storeConsignation.chargerConfiguration(opts)
    }
    
}

async function modifierConfiguration(params, opts) {
    if(params.type_store) {
        return await changerStoreConsignation(params.type_store, params, opts)
    }
    return await _storeConsignation.modifierConfiguration(params, opts)
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

    if(_estPrimaire) {
        // Emettre un message
        await evenementFichierPrimaire(mq, fuuid)
    } else {
        // Le fichier a ete transfere avec succes (aucune exception)
        _transfertPrimaire.ajouterItem(fuuid)
    }

    fsPromises.rm(pathFichierStaging, {recursive: true})
        .catch(err=>console.error("Erreur suppression repertoire %s apres consignation reussie : %O", fuuid, err))

}

async function evenementFichierPrimaire(mq, fuuid) {
    // Emettre evenement aux secondaires pour indiquer qu'un nouveau fichier est pret
    debug("Evenement consignation primaire sur", fuuid)
    const evenement = {fuuid}
    try {
    mq.emettreEvenement(evenement, 'fichiers', {action: 'consignationPrimaire', exchange: '2.prive', attacherCertificat: true})
    } catch(err) {
        console.error(new Date() + " uploadFichier.evenementFichierPrimaire Erreur ", err)
    }
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
        await _storeConsignation.parcourirFichiers(callbackTraiterFichiersASupprimer, {filtre})
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
        await _storeConsignation.parcourirFichiers(callbackTraiterFichiersARecuperer, {filtre})
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

async function entretien() {
    try {
        // Determiner si on est la consignation primaire
        const instance_id_consignation = FichiersTransfertBackingStore.getInstanceId()
        const instance_id_local = _mq.pki.cert.subject.getField('CN').value
        debug("entretien Instance consignation : %s, instance_id local %s", instance_id_consignation, instance_id_local)
        
        const courant = _estPrimaire
        await setEstConsignationPrimaire(instance_id_consignation === instance_id_local)
    } catch(err) {
        console.error("storeConsignation.entretien() Erreur emettrePresence ", err)
    }

    try {
        await emettrePresence()
    } catch(err) {
        console.error("storeConsignation.entretien() Erreur emettrePresence ", err)
    }

    const now = new Date().getTime()
    if(_syncActif && now > _derniere_sync + _intervalleSync) {
        _derniere_sync = now  // Temporaire, pour eviter loop si un probleme survient

        demarrerSynchronization()
            .catch(err=>console.error("storeConsignation.entretien() Erreur processusSynchronisation(1) ", err))
    }
}

async function demarrerSynchronization() {
    await genererListeLocale()
    await processusSynchronisation()
    
    debug("Sync complete, re-emettre presence")
    _derniere_sync = new Date().getTime()

    await emettrePresence()
}

async function processusSynchronisation() {
    if( _estPrimaire === true ) return  // Le primaire n'effectue pas de sync
    if( _sync_lock !== false ) return   // Abort, sync deja en cours

    try {
        _sync_lock = true
        const infoData = await getDataSynchronisation()
        await downloadFichiersSync()
        await marquerFichiersCorbeille()
        await uploaderFichiersVersPrimaire()
        await downloadFichiersBackup()
    } catch(err) {
        console.error("storeConsignation.entretien() Erreur processusSynchronisation(2) ", err)
    } finally {
        _sync_lock = false
    }
}

async function getDataSynchronisation() {
    const httpsAgent = FichiersTransfertBackingStore.getHttpsAgent()
    if(!httpsAgent) throw new Error("processusSynchronisation: httpsAgent n'est pas initialise")

    const urlTransfert = new URL(FichiersTransfertBackingStore.getUrlTransfert())

    const urlData = new URL(urlTransfert.href)
    urlData.pathname = urlData.pathname + '/data/data.json'
    debug("Download %s", urlData.href)
    const reponse = await axios({
        method: 'GET',
        httpsAgent,
        url: urlData.href,
    })
    debug("Reponse GET data.json %s :\n%O", reponse.status, reponse.data)
    
    // Charger listes
    {
        const urlData = new URL(urlTransfert.href)
        urlData.pathname = urlData.pathname + '/data/fuuidsActifs.txt'
        debug("Download %s", urlData.href)
        const fichierActifsPrimaire = path.join(getPathDataFolder(), 'actifsPrimaire.txt.work')
        try {
            const actifStream = fs.createWriteStream(fichierActifsPrimaire)
            const reponseActifs = await axios({ method: 'GET', httpsAgent, url: urlData.href, responseType: 'stream' })
            debug("Reponse GET actifs %s", reponseActifs.status)

            await new Promise((resolve, reject)=>{
                actifStream.on('close', resolve)
                actifStream.on('error', err=>{
                    actifStream.close()
                    reject(err)
                })
                reponseActifs.data.pipe(actifStream)
            })
        } catch(err) {
            const response = err.response
            if(response && response.status === 416) {
                // OK, le fichier est vide
            } else {
                throw err
            }
        } finally {
            try { await fsPromises.unlink(fichierActifsPrimaireDest) } catch(err) {}
            const fichierActifsPrimaireDest = path.join(getPathDataFolder(), 'actifsPrimaire.txt')
            try { 
                // await fsPromises.rename(fichierActifsPrimaire, fichierActifsPrimaireDest) 
                await new Promise((resolve, reject)=>{
                    exec(`sort -o ${fichierActifsPrimaireDest} ${fichierActifsPrimaire}`, error=>{
                        if(error) return reject(error)
                        else resolve()
                    })
                })

                // Detecter fichiers locaux (actifs) qui ne sont pas sur le primaire
                const fichierActifs = path.join(getPathDataFolder(), 'fuuidsActifs.txt')
                const fichierMissing = path.join(getPathDataFolder(), 'fuuidsMissing.txt')
                await new Promise((resolve, reject)=>{
                    exec(`comm -3 ${fichierActifs} ${fichierActifsPrimaireDest} > ${fichierMissing}`, error=>{
                        if(error) return reject(error)
                        else resolve()
                    })
                })

            } 
            catch(err) {
                console.error("storeConsignation.getDataSynchronisation Erreur renaming actifs ", err)
            }
        }
    }

    const fichierCorbeillePrimaire = path.join(getPathDataFolder(), 'corbeillePrimaire.txt.work')
    try {
        const urlData = new URL(urlTransfert.href)
        urlData.pathname = urlData.pathname + '/data/fuuidsCorbeille.txt'
        debug("Download %s", urlData.href)
        const corbeilleStream = fs.createWriteStream(fichierCorbeillePrimaire)
        const reponseCorbeille = await axios({method: 'GET', httpsAgent, url: urlData.href, responseType: 'stream'})
        debug("Reponse GET corbeille %s", reponseCorbeille.status)
        await new Promise((resolve, reject)=>{
            corbeilleStream.on('close', resolve)
            corbeilleStream.on('error', err=>{
                corbeilleStream.close()
                reject(err)
            })
            reponseCorbeille.data.pipe(corbeilleStream)
        })
    } catch(err) {
        const response = err.response
        if(response ) {
            if(response.status === 416) {
                // OK, fichier vide
            } else {
                console.info("Erreur recuperation fichier corbeille HTTP %d", err.response.status)
            }
        } else {
            console.warn("Erreur recuperation fichier corbeille - ", err)
        }
    } finally {
        try { await fsPromises.unlink(fichierCorbeillePrimaireDest) } catch(err) {}
        const fichierCorbeillePrimaireDest = path.join(getPathDataFolder(), 'corbeillePrimaire.txt')
        try { 
            // await fsPromises.rename(fichierCorbeillePrimaire, fichierCorbeillePrimaireDest) 
            await new Promise((resolve, reject)=>{
                exec(`sort -o ${fichierCorbeillePrimaireDest} ${fichierCorbeillePrimaire}`, error=>{
                    if(error) return reject(error)
                    else resolve()
                })
            })
        } 
        catch(err) {
            console.error("storeConsignation.getDataSynchronisation Erreur renaming corbeille ", err)
        }
    }
    
    return reponse.data
}

function ajouterDownloadPrimaire(fuuid) {
    _queueDownloadFuuids.add(fuuid)
    if(_timeoutStartThreadDownload) {
        _threadDownloadFichiersDuPrimaire()
            .catch(err=>{console.error(new Date() + ' storeConsignation._threadDownloadFichiersDuPrimaire Erreur ', err)})
    }
}

async function downloadFichiersSync() {
    const httpsAgent = FichiersTransfertBackingStore.getHttpsAgent()
    if(!httpsAgent) throw new Error("processusSynchronisation: httpsAgent n'est pas initialise")

    const repertoireDownloadSync = path.join(getPathDataFolder(), 'syncDownload')
    await fsPromises.mkdir(repertoireDownloadSync, {recursive: true})

    // const fichierActifsPrimaire = path.join(getPathDataFolder(), 'actifsPrimaire.txt')
    const fichierMissing = path.join(getPathDataFolder(), 'fuuidsMissing.txt')
    const readStreamFichiers = fs.createReadStream(fichierMissing)
    const rlFichiers = readline.createInterface({input: readStreamFichiers, crlfDelay: Infinity})
    for await (const line of rlFichiers) {
        // Detecter fichiers manquants localement par espaces vide au debut de la ligne
        if(line.startsWith('	')) {
            const fuuid = line.trim()
            _queueDownloadFuuids.add(fuuid)
        }
    }

    if(_timeoutStartThreadDownload) {
        _threadDownloadFichiersDuPrimaire()
            .catch(err=>{console.error(new Date() + ' storeConsignation._threadDownloadFichiersDuPrimaire Erreur ', err)})
    }
}

async function _threadDownloadFichiersDuPrimaire() {
    if(_timeoutStartThreadDownload) clearTimeout(_timeoutStartThreadDownload)
    _timeoutStartThreadDownload = null

    debug(new Date() + ' _threadDownloadFichiersDuPrimaire Demarrer download fichiers')

    try {
        while(true) {
            // Recuperer un fuuid a partir du Set
            let fuuid = null
            for(fuuid of _queueDownloadFuuids.values()) break
            if(!fuuid) break

            _queueDownloadFuuids.delete(fuuid)

            try {
                await downloadFichierDuPrimaire(fuuid)
            } catch(err) {
                console.error(new Date() + " Erreur download %s du primaire %O", fuuid, err)
            }
        }

    } catch(err) {
        console.error(new Date() + ' _threadDownloadFichiersDuPrimaire Erreur ', err)
    } finally {
        _timeoutStartThreadDownload = setTimeout(()=>{
            _timeoutStartThreadDownload = null
            _threadDownloadFichiersDuPrimaire()
                .catch(err=>{console.error(new Date() + ' storeConsignation._threadDownloadFichiersDuPrimaire Erreur ', err)})
        }, DOWNLOAD_PRIMAIRE)
        debug(new Date() + ' _threadDownloadFichiersDuPrimaire Fin')
    }
}

async function downloadFichierDuPrimaire(fuuid) {

    // Tenter de recuperer le fichier localement
    const recuperation = await recupererFichier(fuuid)
    if(recuperation !== null)  {
        debug("downloadFichierDuPrimaire Fichier %O recupere avec succes sans download", recuperation)
        return
    }

    debug("storeConsignation.downloadFichiersSync Fuuid %s manquant, debut download", fuuid)
    const urlTransfert = new URL(FichiersTransfertBackingStore.getUrlTransfert())
    const urlFuuid = new URL(urlTransfert.href)
    urlFuuid.pathname = urlFuuid.pathname + '/' + fuuid
    debug("Download %s", urlFuuid.href)

    const dirFuuid = path.join(getPathDataFolder(), 'syncDownload', fuuid)
    await fsPromises.mkdir(dirFuuid, {recursive: true})
    const fuuidFichier = path.join(dirFuuid, '0.part')  // Fichier avec position initiale - 1 seul fichier
    const fuuidStream = fs.createWriteStream(fuuidFichier)

    try {
        const httpsAgent = getHttpsAgent()
        const reponseActifs = await axios({ method: 'GET', httpsAgent, url: urlFuuid.href, responseType: 'stream' })
        debug("Reponse GET actifs %s", reponseActifs.status)
        await new Promise((resolve, reject)=>{
            fuuidStream.on('close', resolve)
            fuuidStream.on('error', err=>{
                fuuidStream.close()
                fsPromises.unlink(fuuidFichier)
                    .catch(err=>console.warn("Erreur suppression fichier %s : %O", fuuidFichier, err))
                reject(err)
            })
            reponseActifs.data.pipe(fuuidStream)
        })

        debug("Fichier %s download complete", fuuid)
        await _storeConsignation.consignerFichier(dirFuuid, fuuid)
    } catch(err) {
        console.info("Erreur sync fuuid %s : %O", fuuid, err)
    } finally {
        await fsPromises.rm(dirFuuid, {recursive: true, force: true})
    }
}

async function marquerFichiersCorbeille() {
    const repertoireDownloadSync = path.join(getPathDataFolder(), 'syncDownload')
    await fsPromises.mkdir(repertoireDownloadSync, {recursive: true})

    const fichierCorbeillePrimaire = path.join(getPathDataFolder(), 'corbeillePrimaire.txt')
    try {
        const readStreamFichiers = fs.createReadStream(fichierCorbeillePrimaire)
        const rlFichiers = readline.createInterface({input: readStreamFichiers, crlfDelay: Infinity})

        for await (const line of rlFichiers) {
            const fuuid = line.trim()
            try {
                await _storeConsignation.marquerSupprime(fuuid)
                debug("Fichier %s transfere a la corbeille", fuuid)
            } catch(err) {
                debug("Erreur transfert %s vers corbeille", fuuid, err)
            }
        }
    } catch(err) {
        debug("Erreur traitement sync corbeille ", err)
    }

}

async function uploaderFichiersVersPrimaire() {
    debug("uploaderFichiersVersPrimaire Debut")

    // // Detecter fichiers locaux (actifs) qui ne sont pas sur le primaire
    const fichierMissing = path.join(getPathDataFolder(), 'fuuidsMissing.txt')

    try {
        const readStreamFichiers = fs.createReadStream(fichierMissing)
        const rlFichiers = readline.createInterface({input: readStreamFichiers, crlfDelay: Infinity})
        for await (const line of rlFichiers) {
            // Detecter changement distant avec un fuuid dans la premiere colonne du fichier (pas d'espaces vides)
            if( ! line.startsWith(' ')) {
                const fuuid = line.trim()
                debug("uploaderFichiersVersPrimaire Transferer fichier manquant %s vers primaire", fuuid)
                _transfertPrimaire.ajouterItem(fuuid)
            }
        }
    } catch(err) {
        debug("uploaderFichiersVersPrimaire Erreur traitement ", err)
    }


    debug("uploaderFichiersVersPrimaire Fin")
}

async function downloadFichiersBackup() {
    const urlTransfert = new URL(FichiersTransfertBackingStore.getUrlTransfert())
    const httpsAgent = FichiersTransfertBackingStore.getHttpsAgent()

    const urlListe = new URL(urlTransfert.href)
    urlListe.pathname = urlListe.pathname + '/backup/liste'
    let reponse = await axios({method: 'GET', url: urlListe.href, httpsAgent})
    debug("Reponse fichiers backup :\n%s", reponse.data)
    reponse = reponse.data.split('\n')
    debug("Reponse fichiers backup liste : %O", reponse)

    // Faire la liste des fichiers de backup locaux
    const fichiersBackupLocaux = new Set()
    const addFichierLocal = fichier => {
        if(!fichier) return  // Derniere entree
    
        const pathFichierSplit = fichier.directory.split('/')
        const pathBase = pathFichierSplit.slice(pathFichierSplit.length-2).join('/')
    
        // Conserver uniquement le contenu de transaction/ (transaction_archive/ n'est pas copie)
        if(pathBase.startsWith('transactions/')) {
            const fichierPath = path.join(pathBase, fichier.filename)
            fichiersBackupLocaux.add(fichierPath)
        }
    }
    await parcourirBackup(addFichierLocal)

    for(let fichierBackup of reponse) {
        if(!fichierBackup) continue  // Ligne vide, skip

        try {
            if( ! fichiersBackupLocaux.has(fichierBackup) ) {
                // Downloader fichier
                debug("downloadFichiersBackup Fichier backup manquant '%s'", fichierBackup)
                const urlFichier = new URL(urlTransfert.href)
                urlFichier.pathname = path.join(urlFichier.pathname, 'backup', fichierBackup)
                const reponse = await axios({method: 'GET', url: urlFichier.href, httpsAgent, responseType: 'stream'})
                debug("Reponse fichier backup ", reponse.status)

                const pathFichierBase = fichierBackup.replace('transactions/', '')

                const downloadStream = reponse.data
                // Ouvrir fichier pour conserver bytes
                await _storeConsignation.pipeBackupTransactionStream(pathFichierBase, downloadStream)

            } else {
                debug("downloadFichiersBackup Ficher backup existe localement (OK) '%s'", fichierBackup)
            }
        } catch(err) {
            console.error(new Date() + " storeConsignation Erreur download fichier backup %s : %O", fichierBackup, err)
        } finally {
            // Retirer le fichier du Set
            fichiersBackupLocaux.delete(fichierBackup)
        }
    }

    // Cleanup des fichiers restants localement (qui ne sont pas sur le serveur remote)
    for(let fichierBackup of fichiersBackupLocaux) {
        try {
            debug("Retirer fichier ", fichierBackup)
            const pathFichierBase = fichierBackup.replace('transactions/', '')
            await _storeConsignation.deleteBackupTransaction(pathFichierBase)
        } catch(err) {
            console.error(new Date() + ' Erreur suppression fichier backup ', fichierBackup)
        }
    }
}

function getPathDataFolder() {
    return path.join(FichiersTransfertBackingStore.getPathStaging(), 'liste')
}

async function emettrePresence() {
    try {
        debug("emettrePresence Configuration fichiers")
        
        // Creer info avec defaults, override avec config locale
        const info = {
            type_store: 'millegrille',
            consignation_url: 'https://fichiers:443'
        }

        const configuration = await chargerConfiguration()
        for(const champ of Object.keys(configuration)) {
            if(CONST_CHAMPS_CONFIG.includes(champ)) info[champ] = configuration[champ]
        }
        
        try {
            const pathData = path.join(getPathDataFolder(), 'data.json')
            const fichierData = await fsPromises.readFile(pathData, 'utf-8')
            const data = JSON.parse(fichierData)
            info.fichiers_nombre = data.nombreFichiersActifs
            info.corbeille_nombre = data.nombreFichiersCorbeille
            info.fichiers_taille = data.tailleActifs
            info.corbeille_taille = data.tailleCorbeille

        } catch(err) {
            console.error("storeConsignationLocal.emettrePresence ERROR Erreur chargement fichier data.json : %O", err)
        }

        await _mq.emettreEvenement(info, 'fichiers', {action: 'presence', attacherCertificat: true})
    } catch(err) {
        console.error("storeConsignation.emettrePresence Erreur emission presence : ", err)
    }
}

/** Genere une liste locale de tous les fuuids */
async function genererListeLocale() {
    debug("genererListeLocale Debut")

    const pathStaging = FichiersTransfertBackingStore.getPathStaging()
    const pathFichiers = path.join(pathStaging, 'liste')
    debug("genererListeLocale Fichiers sous ", pathFichiers)
    await fsPromises.mkdir(pathFichiers, {recursive: true})

    const fichierActifsNew = path.join(pathFichiers, '/fuuidsActifs.txt.new'),
          fichierCorbeilleNew = path.join(pathFichiers, 'fuuidsCorbeille.txt.new')

    const fichierFuuidsActifsHandle = await fsPromises.open(fichierActifsNew, 'w'),
          fichierFuuidsCorbeilleHandle = await fsPromises.open(fichierCorbeilleNew, 'w')

    let ok = true,
        nombreFichiersActifs = 0,
        nombreFichiersCorbeille = 0,
        tailleActifs = 0,
        tailleCorbeille = 0
    try {
        const streamFuuidsActifs = fichierFuuidsActifsHandle.createWriteStream(),
              streamFuuidsCorbeille = fichierFuuidsCorbeilleHandle.createWriteStream()

        const callbackTraiterFichier = async item => {
            if(!item) {
                streamFuuidsActifs.close()
                streamFuuidsCorbeille.close()
                return  // Dernier fichier
            }

            const corbeille = item.filename.endsWith('.corbeille')
            if(corbeille) {
                streamFuuidsCorbeille.write(item.filename + '\n')
                nombreFichiersCorbeille++
                tailleCorbeille += item.size
            } else {
                streamFuuidsActifs.write(item.filename + '\n')
                nombreFichiersActifs++
                tailleActifs += item.size
            }
        }

        await _storeConsignation.parcourirFichiers(callbackTraiterFichier)
    } catch(err) {
        console.error(new Date() + " ERROR genererListeLocale() : %O", err)
        ok = false
    } finally {
        await fichierFuuidsActifsHandle.close()
        await fichierFuuidsCorbeilleHandle.close()
    }

    if(ok) {
        debug("genererListeLocale Terminer information liste")
        const info = {
            nombreFichiersActifs, 
            nombreFichiersCorbeille,
            tailleActifs,
            tailleCorbeille,
        }
        const messageFormatte = await _mq.pki.formatterMessage(info, 'fichiers', {action: 'liste', ajouterCertificat: true})
        debug("genererListeLocale messageFormatte : ", messageFormatte)
        fsPromises.writeFile(path.join(pathFichiers, 'data.json'), JSON.stringify(messageFormatte))

        // Renommer fichiers .new
        const fichierActifs = path.join(pathFichiers, '/fuuidsActifs.txt'),
              fichierCorbeille = path.join(pathFichiers, 'fuuidsCorbeille.txt')
        try { await fsPromises.rm(fichierActifs) } catch(err) { }
        try { await fsPromises.rm(fichierCorbeille) } catch(err) { }
        
        try { 
            // Copier le fichier de .work.txt a .txt, trier en meme temps
            await new Promise((resolve, reject)=>{
                exec(`sort -o ${fichierActifs} ${fichierActifsNew} && gzip -fk ${fichierActifs}`, error=>{
                    if(error) return reject(error)
                    else resolve()
                })
            })
        } catch(err) {
            console.error("storeConsignation.genererListeLocale Erreur copie fichiers actifs : ", err)
        } finally {
            // Supprimer .work.txt
            try { await fsPromises.rm(fichierActifsNew) }
            catch(err) {
                if(err.code === 'ENOENT') {
                    // Ok, fichier n'existe pas
                } else {
                    console.error("storeConsignation.genererListeLocale Erreur suppression fichiers actifs work : ", err)
                }
            }
        }

        try { 
            // Copier le fichier de .work.txt a .txt, trier en meme temps
            await new Promise((resolve, reject)=>{
                exec(`sort -o ${fichierCorbeille} ${fichierCorbeilleNew} && gzip -fk ${fichierCorbeille}`, error=>{
                    if(error) return reject(error)
                    else resolve()
                })
            })
        } catch(err) { 
            if(err.code === 'ENOENT') {
                // Ok, fichier n'existe pas
            } else {
                console.error("storeConsignation.genererListeLocale Erreur copie fichiers corbeille : ", err)
            }
        } finally {
            // Supprimer .work.txt
            try { await fsPromises.rm(fichierCorbeilleNew) }
            catch(err) {
                if(err.code === 'ENOENT') {
                    // Ok, fichier n'existe pas
                } else {
                    console.error("storeConsignation.genererListeLocale Erreur suppression fichiers actifs work : ", err)
                }
            }
        }

        if(_estPrimaire) {
            debug("Emettre evenement de fin du creation de liste du primaire")
            await _mq.emettreEvenement(messageFormatte, 'fichiers', {action: 'syncPret', ajouterCertificat: true})
        }
    }

    debug("genererListeLocale Fin")
}

function parcourirFichiers(callback, opts) {
    return _storeConsignation.parcourirFichiers(callback, opts)
}

function parcourirBackup(callback, opts) {
    return _storeConsignation.parcourirBackup(callback, opts)
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

function getInstanceId() {
    return FichiersTransfertBackingStore.getInstanceId()
}

function getUrlTransfert() {
    return new URL(FichiersTransfertBackingStore.getUrlTransfert())
}

function getHttpsAgent() {
    return FichiersTransfertBackingStore.getHttpsAgent()
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

function getBackupTransactionStream(pathBackupTransaction) {
    return _storeConsignation.getBackupTransactionStream(pathBackupTransaction)
}

function estPrimaire() {
    return _estPrimaire
}

async function setEstConsignationPrimaire(primaire) {
    debug('setEstConsignationPrimaire %s', primaire)
    FichiersTransfertBackingStore.setEstPrimaire(primaire)
    const courant = _estPrimaire
    _estPrimaire = primaire
    if(courant !== primaire) {
        FichiersTransfertBackingStore.setEstPrimaire(_estPrimaire)
        debug("Changement role consignation : primaire => %s", primaire)
        FichiersTransfertBackingStore.setEstPrimaire(primaire)
        if(_estPrimaire === true) {
            // Ecouter Q de backup sur MQ
            startConsumingActions().catch(err=>console.error(new Date() + ' Erreur start consuming actions', err))
            startConsumingBackup().catch(err=>console.error(new Date() + ' Erreur start consuming backup', err))
        } else {
            // Arret ecoute de Q de backup sur MQ
            stopConsumingActions().catch(err=>console.error(new Date() + ' Erreur stop consuming actions', err))
            stopConsumingBackup().catch(err=>console.error(new Date() + ' Erreur stop consuming backup', err))
        }
    }
}

module.exports = { 
    init, changerStoreConsignation, chargerConfiguration, modifierConfiguration, getInfoFichier,
    entretienFichiersSupprimes, supprimerFichier, recupererFichier, confirmerActiviteFuuids,
    middlewareRecevoirFichier, middlewareReadyFichier, middlewareDeleteStaging, 
    sauvegarderBackupTransactions, rotationBackupTransactions,
    getFichiersBackupTransactionsCourant, getBackupTransaction, getBackupTransactionStream,
    getPathDataFolder, estPrimaire, setEstConsignationPrimaire,
    getUrlTransfert, getHttpsAgent, getInstanceId, ajouterDownloadPrimaire,
    processusSynchronisation, demarrerSynchronization, 
    parcourirFichiers, parcourirBackup, 
}
