const debug = require('debug')('consignation:store:root')
const path = require('path')
const fsPromises = require('fs/promises')
const fs = require('fs')
const readline = require('readline')
const axios = require('axios')
const { exec } = require('child_process')
const readdirp = require('readdirp')
const tmpPromises = require('tmp-promise')

const FichiersTransfertBackingStore = require('@dugrema/millegrilles.nodejs/src/fichiersTransfertBackingstore')
const { VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')

const StoreConsignationLocal = require('./storeConsignationLocal')
const StoreConsignationSftp = require('./storeConsignationSftp')
const StoreConsignationAwsS3 = require('./storeConsignationAwsS3')

const TransfertPrimaire = require('./transfertPrimaire')
const BackupSftp = require('./backupSftp')
const { dechiffrerConfiguration } = require('./pki')

const { startConsuming: startConsumingBackup, stopConsuming: stopConsumingBackup } = require('../messages/backup')
const { startConsuming: startConsumingActions, stopConsuming: stopConsumingActions } = require('../messages/actions')

const BATCH_SIZE = 100,
      TIMEOUT_AXIOS = 30_000

const CONST_CHAMPS_CONFIG = ['type_store', 'url_download', 'consignation_url']
const INTERVALLE_SYNC = 3_600_000,  // 60 minutes
      INTERVALLE_THREAD_TRANSFERT = 1_200_000  // 20 minutes,
      NOMBRE_ARCHIVES_ORPHELINS = 4

const FICHIER_FUUIDS_ACTIFS = 'fuuidsActifs.txt',
      FICHIER_FUUIDS_ACTIFS_PRIMAIRE = 'fuuidsActifsPrimaire.txt',
      FICHIER_FUUIDS_MANQUANTS = 'fuuidsManquants.txt',
      FICHIER_FUUIDS_MANQUANTS_PRIMAIRE = 'fuuidsManquantsPrimaire.txt',
      FICHIER_FUUIDS_PRIMAIRE = 'fuuidsPrimaire.txt',
      FICHIER_FUUIDS_UPLOAD_PRIMAIRE = 'fuuidsUploadPrimaire.txt',
      FICHIER_FUUIDS_RECLAMES_ACTIFS = 'fuuidsReclamesActifs.txt',
      FICHIER_FUUIDS_RECLAMES_ARCHIVES = 'fuuidsReclamesArchives.txt'

var _mq = null,
    _storeConsignation = null,
    // _storeConsignationLocal = null,
    _estPrimaire = false,
    _sync_lock = false,
    _derniere_sync = 0,
    _transfertPrimaire = null,
    _backupSftp = null,
    _queueDownloadFuuids = new Set(),
    _timeoutStartThreadDownload = null,
    _intervalleSync = INTERVALLE_SYNC,
    _syncActif = true,
    _timeoutTraiterConfirmes = null

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

    // Handler backup sftp
    _backupSftp = new BackupSftp(mq, this)
    _backupSftp.setTimeout(15_000)  // Demarrer thread dans 15 secondes

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
        _intervalleSync = params.sync_intervalle * 1000  // Convertir de secondes en millisecs
        debug("changerStoreConsignation Set sync intervalle %d msecs", _intervalleSync)
    }

    if(_storeConsignation && _storeConsignation.fermer) await _storeConsignation.fermer()

    let storeConsignation = null
    switch(typeStore) {
        case 'sftp': storeConsignation = StoreConsignationSftp; break
        case 'awss3': storeConsignation = StoreConsignationAwsS3; break
        case 'local':
        case 'millegrille': 
            storeConsignation = StoreConsignationLocal
            break
        default: storeConsignation = StoreConsignationLocal
    }

    if(params.data_chiffre && !params.data_dechiffre) {
        // Dechiffrer configuration - mis dans configuration.data_dechiffre
        params.data_dechiffre = await dechiffrerConfiguration(_mq, params)
    }

    // Changer methode de consignation
    try {
        await storeConsignation.init(params)
        _storeConsignation = storeConsignation

        await _storeConsignation.modifierConfiguration({...params, type_store: typeStore})
    } catch(err) {
        debug("Erreur setup store configuration ", err)
        console.error(new Date() + ' changerStoreConsignation Configuration invalide ', err)
    }
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
    debug("transfererFichierVersConsignation Fichier %s/%s", pathReady, item)
    const transactions = await FichiersTransfertBackingStore.traiterTransactions(mq, pathReady, item)
    debug("transfererFichierVersConsignation Info ", transactions)
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

// async function traiterRecuperer() {
//     debug("Traitement des fichiers a recuperer")
//     let batchFichiers = []
    
//     const callbackActionRecuperer = async item => {
//         const {fuuid, supprime} = item
//         if(supprime === false) {
//             debug("Le fichier supprime %s est requis par un module, on le recupere", fuuid)
//             await _storeConsignation.recoverFichierSupprime(fuuid)
//         }
//     }

//     const callbackTraiterFichiersARecuperer = async item => {
//         if(!item) {
//             // Derniere batch
//             if(batchFichiers.length > 0) await traiterBatch(batchFichiers, callbackActionRecuperer)
//         } else {
//             batchFichiers.push(path.basename(item.filename, '.corbeille'))
//             while(batchFichiers.length > BATCH_SIZE) {
//                 const batchCourante = batchFichiers.slice(0, BATCH_SIZE)
//                 batchFichiers = batchFichiers.slice(BATCH_SIZE)
//                 await traiterBatch(batchCourante, callbackActionRecuperer)
//             }
//         }
//     }
    
//     try {
//         const filtre = item => item.filename.endsWith('.corbeille')
//         await _storeConsignation.parcourirFichiers(callbackTraiterFichiersARecuperer, {filtre})
//     } catch(err) {
//         console.error(new Date() + " ERROR traiterRecuperer() : %O", err)
//     }
// }

// async function traiterBatch(fuuids, callbackAction) {
//     debug("Traiter batch : %O", fuuids)

//     _batchFichiersFuuids = fuuids.reduce((acc, item)=>{
//         acc[item]=false 
//         return acc
//     }, {})
//     debug("Traiter batch fichiers : %O", _batchFichiersFuuids)

//     const evenement = { fuuids }
//     //const domaine = 'GrosFichiers',
//     const action = 'confirmerEtatFuuids',
//           domaine = 'fichiers'
//     // const reponse = await _mq.transmettreRequete(domaine, requete, {action})
//     await _mq.emettreEvenement(evenement, domaine, {action})

//     // Attendre reponses, timeout de 10 secondes pour collecter tous les messages
//     await new Promise(resolve=>{
//         let timeoutBatch = setTimeout(resolve, 10000)
//         _triggerPromiseBatch = () => {
//             clearTimeout(timeoutBatch)
//             resolve()
//         }
//     })

//     const resultatBatch = _batchFichiersFuuids
//     _batchFichiersFuuids = null
//     _triggerPromiseBatch = null
//     debug("Resultat verification : %O", resultatBatch)

//     // Reassembler resultat
//     const resultatFuuids = {}
//     for(const fuuid in resultatBatch) {
//         const resultat = resultatBatch[fuuid]
//         if(resultat === false) {
//             resultatFuuids[fuuid] = {fuuid, supprime: true}
//         } else {
//             resultatFuuids[fuuid] = resultat
//         }
//     }
    
//     const resultatListe = Object.values(resultatFuuids)

//     debug("Appliquer callback a liste : %O", resultatListe)

//     for await (const reponseFichier of resultatListe) {
//         await callbackAction(reponseFichier)
//     }

// }

async function entretien() {
    try {
        // Determiner si on est la consignation primaire
        const instance_id_consignation = FichiersTransfertBackingStore.getInstanceId()
        const instance_id_local = _mq.pki.cert.subject.getField('CN').value
        debug("entretien Instance consignation : %s, instance_id local %s", instance_id_consignation, instance_id_local)
        await setEstConsignationPrimaire(instance_id_consignation === instance_id_local)
    } catch(err) {
        console.error("storeConsignation.entretien() Erreur emettrePresence ", err)
    }

    try {
        await emettrePresence()
    } catch(err) {
        console.error("storeConsignation.entretien() Erreur emettrePresence ", err)
    }

    // Entretien specifique a la consignation
    try {
        if(_storeConsignation.entretien) await _storeConsignation.entretien()
    } catch(err) {
        console.error(new Date() + ' Erreur entretien _storeConsignation ', err)
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
        await getDataSynchronisation()
        
        try {
            await uploaderFichiersVersPrimaire()
        } catch(err) {
            console.error(new Date() + " ERROR uploadFichiersVersPrimaire ", err)
        }
        
        try {
            await downloadFichiersBackup()
        } catch(err) {
            console.error(new Date() + " ERROR downloadFichiersBackup ", err)
        }

        try {
            await downloadFichiersSync()
        } catch(err) {
            console.error(new Date() + " ERROR downloadFichiersSync ", err)
        }

        try {
            await traiterOrphelinsSecondaire()
        } catch(err) {
            console.error(new Date() + " ERROR traiterOrphelinsSecondaire ", err)
        }
    } catch(err) {
        console.error("storeConsignation.entretien() Erreur processusSynchronisation(2) ", err)
    } finally {
        _sync_lock = false
    }
}

async function traiterOrphelinsSecondaire() {
    const pathOrphelins = path.join(getPathDataFolder(), 'orphelins')
    await fsPromises.mkdir(pathOrphelins, {recursive: true})

    // Generer une liste combinee de tous les fichiers requis (primaire actifs + manquants)


    // await rotationOrphelins(pathOrphelins, fichierReclames)

}

async function downloadFichierListe(fichierDestination, remotePathnameFichier) {
    const httpsAgent = FichiersTransfertBackingStore.getHttpsAgent()
    const urlTransfert = new URL(FichiersTransfertBackingStore.getUrlTransfert())
    const urlData = new URL(urlTransfert.href)
    urlData.pathname = urlData.pathname + remotePathnameFichier
    debug("downloadFichierListe Download %s", urlData.href)
    const fichierTmp = await tmpPromises.file()
    try {
        const writeStream = fs.createWriteStream('', {fd: fichierTmp.fd})
        const reponseActifs = await axios({ 
            method: 'GET', 
            httpsAgent, 
            url: urlData.href, 
            responseType: 'stream',
            timeout: TIMEOUT_AXIOS,
        })
        debug("Reponse GET actifs %s", reponseActifs.status)

        await new Promise((resolve, reject)=>{
            writeStream.on('close', resolve)
            writeStream.on('error', reject)
            reponseActifs.data.pipe(writeStream)
        })

        try {
            debug("Sort fichier %s vers %s", fichierTmp.path, fichierDestination)
            await sortFile(fichierTmp.path, fichierDestination, {gzipsrc: true})
        } 
        catch(err) {
            console.error("storeConsignation.getDataSynchronisation Erreur renaming actifs ", err)
        }

    } catch(err) {
        const response = err.response
        if(response && response.status === 416) {
            // OK, le fichier est vide
        } else {
            throw err
        }
    } finally {
        fichierTmp.cleanup().catch(err=>console.error(new Date() + "ERROR Cleanup fichier tmp : ", err))
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
        timeout: TIMEOUT_AXIOS,
    })
    debug("Reponse GET data.json %s :\n%O", reponse.status, reponse.data)
    
    // Charger listes
    const fuuidsActifsPrimaire = path.join(getPathDataFolder(), FICHIER_FUUIDS_ACTIFS_PRIMAIRE)
    const fuuidsManquantsPrimaire = path.join(getPathDataFolder(), FICHIER_FUUIDS_MANQUANTS_PRIMAIRE)
    await downloadFichierListe(fuuidsActifsPrimaire, '/data/fuuidsActifs.txt.gz')
    await downloadFichierListe(fuuidsManquantsPrimaire, '/data/fuuidsManquants.txt.gz')
    
    // Combiner listes, dedupe
    const fuuidsPrimaire = path.join(getPathDataFolder(), FICHIER_FUUIDS_PRIMAIRE)
    await new Promise((resolve, reject)=>{
        exec(`cat ${fuuidsActifsPrimaire} ${fuuidsManquantsPrimaire} | sort -u -o ${fuuidsPrimaire}`, error=>{
            if(error) return reject(error)
            else resolve()
        })
    })

    return reponse.data
}

async function recevoirFuuidsDomaines(fuuids, opts) {
    opts = opts || {}
    const archive = opts.archive || false
    debug("recevoirFuuidsDomaines %d fuuids (archive %s)", fuuids.length, archive)

    let fichierFuuids = null
    if(archive) {
        fichierFuuids = path.join(getPathDataFolder(), FICHIER_FUUIDS_RECLAMES_ARCHIVES)
    } else {
        fichierFuuids = path.join(getPathDataFolder(), FICHIER_FUUIDS_RECLAMES_ACTIFS)
    }

    const writeStream = fs.createWriteStream(fichierFuuids, {flags: 'a'})
    await new Promise((resolve, reject) => {
        writeStream.on('error', reject)
        writeStream.on('close', resolve)
        for (let fuuid of fuuids) {
            writeStream.write(fuuid + '\n')
        }
        writeStream.close()
    })

    if(_timeoutTraiterConfirmes) clearTimeout(_timeoutTraiterConfirmes)
    debug("traiterFichiersConfirmes dans 5 secs")
    _timeoutTraiterConfirmes = setTimeout(()=>traiterFichiersConfirmes().catch(err=>console.error("ERREUR ", err)), 5_000)
}

async function traiterFichiersConfirmes() {
    try {
        debug("traiterFichiersConfirmes")
        if(_timeoutTraiterConfirmes) clearTimeout(_timeoutTraiterConfirmes)
        _timeoutTraiterConfirmes = null

        const fichierActifs = path.join(getPathDataFolder(), FICHIER_FUUIDS_ACTIFS)

        const fichierFuuidsReclamesActifs = path.join(getPathDataFolder(), FICHIER_FUUIDS_RECLAMES_ACTIFS)
        const fichierFuuidsReclamesActifsTmp = path.join(getPathDataFolder(), FICHIER_FUUIDS_RECLAMES_ACTIFS + '.tmp')
        const fichierFuuidsReclamesActifsCourants = path.join(getPathDataFolder(), 'fuuidsReclamesActifs.courant.txt')

        const pathOrphelins = path.join(getPathDataFolder(), 'orphelins')
        await fsPromises.mkdir(pathOrphelins, {recursive: true})

        //const fichierFuuidsReclamesArchives = path.join(getPathDataFolder(), FICHIER_FUUIDS_RECLAMES_ARCHIVES)

        try {
            // Trier, retirer doubles des fuuids reclames actifs
            await fsPromises.rename(fichierFuuidsReclamesActifs, fichierFuuidsReclamesActifsTmp)
            await sortFile(fichierFuuidsReclamesActifsTmp, fichierFuuidsReclamesActifsCourants)
            await fsPromises.rm(fichierFuuidsReclamesActifsTmp)
        } catch(err) {
            debug("Aucun fichier de fuuids reclames, skip")
            return
        }

        try {
            // Faire la liste des fuuids inconnus (reclames mais pas dans actifs / archives)
            const fuuidsManquants = path.join(getPathDataFolder(), 'fuuidsManquants.txt')
            await new Promise((resolve, reject)=>{
                exec(`comm -13 ${fichierActifs} ${fichierFuuidsReclamesActifsCourants} > ${fuuidsManquants} && gzip -9fk ${fuuidsManquants}`, error=>{
                    if(error) return reject(error)
                    else resolve()
                })
            })
        } catch(err) {
            console.error(new Date() + " traiterFichiersConfirmes ERROR Traitement fichiers inconnus : ", err)
        }

        try {
            // Faire une rotation des fichiers orphelins existants (fait la diff avec actifs pour retirer fichiers reactives)
            await rotationOrphelins(pathOrphelins, fichierFuuidsReclamesActifsCourants)

            // Extraire liste de fuuids orphelins (reclames par aucun domaine, e.g. fichiers supprimes)
            // Les orphelins (non reclames par un domaine) sont dans la liste 1 (actifs) mais pas dans la liste 2 (reclames)
            const fichierOrphelins = path.join(pathOrphelins, 'orphelins.00')
            await new Promise((resolve, reject)=>{
                exec(`comm -23 ${fichierActifs} ${fichierFuuidsReclamesActifsCourants} > ${fichierOrphelins}`, error=>{
                    if(error) return reject(error)
                    else resolve()
                })
            })

        } catch(err) {
            console.error(new Date() + " traiterFichiersConfirmes ERROR Traitement orphelins : ", err)
        }
    } catch(err) {
        console.error(new Date() + " traiterFichiersConfirmes ERROR Erreur traitement : ", err)
    }

}

/** Fait une rotation des fichiers orphelins. Compare le resultat avec fuuids reclames courants */
async function rotationOrphelins(pathOrphelins, fichierReclames) {
    const fichiersOrphelins = []
    for await (const entry of readdirp(pathOrphelins, { type: 'files', depth: 0 })) {
        fichiersOrphelins.push(entry.fullPath)
    }
    
    // Les fichiers sont nommes avec extension numerique "orphelins.00, orphelins.01, etc"
    // Trier en ordre inverse pour traiter le plus vieux en premier
    fichiersOrphelins.sort((a, b) => -1 * a.localeCompare(b))
    debug("Liste trie (desc) de fichiers orphelins : ", fichiersOrphelins)

    for await (let fichierOrphelin of fichiersOrphelins) {
        const pathFichierOrphelinSuivant = path.parse(fichierOrphelin)
        debug("pathFichierOrphelin (prep suivant) ", pathFichierOrphelinSuivant)
        let extSuivante = Number.parseInt(pathFichierOrphelinSuivant.ext.slice(1))

        if(extSuivante >= NOMBRE_ARCHIVES_ORPHELINS) {
            // Nombre d'archives est trop grand, supprimer le fichier et passer au prochain
            await fsPromises.rm(fichierOrphelin)
            continue
        }

        extSuivante++  // Prochain numero
        if(extSuivante<10) pathFichierOrphelinSuivant.ext = '.0' + extSuivante
        else pathFichierOrphelinSuivant.ext = '.' + extSuivante
        pathFichierOrphelinSuivant.base = undefined
        const pathFichierOrphelinSuivantStr = path.format(pathFichierOrphelinSuivant)
        debug("pathFichierOrphelinSuivantStr ", pathFichierOrphelinSuivantStr)

        // Conserver les fuuids qui sont uniquement presents dans le fichier orphelin
        // Retirer ceux nouvellement reclames du fichier orphelin
        await new Promise((resolve, reject)=>{
            exec(`comm -13 ${fichierReclames} ${fichierOrphelin} > ${pathFichierOrphelinSuivantStr}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })
    }

    if(fichiersOrphelins.length >= NOMBRE_ARCHIVES_ORPHELINS) {
        const fichierOrphelins = fichiersOrphelins[0]  // Premier fichier est le plus vieux
        debug("Deplacer fichiers de la derniere archive orphelins : %s", fichierOrphelins)
        const readStreamFichiers = fs.createReadStream(fichierOrphelins)
        const rlFichiers = readline.createInterface({input: readStreamFichiers, crlfDelay: Infinity})
        for await (let fuuid of rlFichiers) {
            fuuid = fuuid.trim()
            if(!fuuid) continue  // Ligne vide
            debug("Deplacer fuuid vers orphelins : ", fuuid)
            try {
                await _storeConsignation.marquerOrphelin(fuuid)
            } catch(err) {
                if(err.code === 'ENOENT') {
                    // Ok, fichier deja retire
                } else {
                    console.warn(new Date() + " WARN Echec marquer fichier %s comme orphelin", fuuid)
                }
            }
        }
    }

    try {
        await _storeConsignation.purgerOrphelinsExpires()
    } catch(err) {
        console.error(new Date() + " ERROR Purger orphelins ", err)
    }

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

    const fichierActifsPrimaire = path.join(getPathDataFolder(), FICHIER_FUUIDS_ACTIFS_PRIMAIRE)
    _queueDownloadFuuids.clear()
    await chargerListeFichiersMissing(fuuid=>_queueDownloadFuuids.add(fuuid))
    
    // const fichierMissing = path.join(getPathDataFolder(), 'fuuidsMissing.txt')
    // const readStreamFichiers = fs.createReadStream(fichierMissing)
    // const rlFichiers = readline.createInterface({input: readStreamFichiers, crlfDelay: Infinity})
    // for await (const line of rlFichiers) {
    //     // Detecter fichiers manquants localement par espaces vide au debut de la ligne
    //     if( line.startsWith('	') || line.startsWith('\t') ) {
    //         const fuuid = line.trim()
    //         debug('downloadFichiersSync ajouter ', fuuid)
    //         _queueDownloadFuuids.add(fuuid)
    //     }
    // }

    if(_timeoutStartThreadDownload) {
        _threadDownloadFichiersDuPrimaire()
            .catch(err=>{console.error(new Date() + ' storeConsignation._threadDownloadFichiersDuPrimaire Erreur ', err)})
    }
}

async function chargerListeFichiersMissing(cb) {

    const fuuidsPrimaire = path.join(getPathDataFolder(), FICHIER_FUUIDS_PRIMAIRE)
    const fuuidsLocaux = path.join(getPathDataFolder(), FICHIER_FUUIDS_ACTIFS)
    const fuuidsUploadPrimaire = path.join(getPathDataFolder(), FICHIER_FUUIDS_UPLOAD_PRIMAIRE)

    await new Promise((resolve, reject) => {
        exec(`comm -23 ${fuuidsLocaux} ${fuuidsPrimaire} > ${fuuidsUploadPrimaire}`, error=>{
            if(error) return reject(error)
            else resolve()
        })
    })

    const readStreamFichiers = fs.createReadStream(fuuidsUploadPrimaire)
    const rlFichiers = readline.createInterface({input: readStreamFichiers, crlfDelay: Infinity})
    for await (let fuuid of rlFichiers) {
        // Detecter fichiers manquants localement par espaces vide au debut de la ligne
        fuuid = fuuid.trim()
        if(!fuuid) continue  // Ligne vide

        debug('chargerListeFichiersMissing fuuid manquant du primaire ', fuuid)
        await cb(fuuid)
    }    
}

async function _threadDownloadFichiersDuPrimaire() {
    if(_timeoutStartThreadDownload) clearTimeout(_timeoutStartThreadDownload)
    _timeoutStartThreadDownload = null

    debug(new Date() + ' _threadDownloadFichiersDuPrimaire Demarrer download fichiers')

    let intervalleRedemarrageThread = INTERVALLE_THREAD_TRANSFERT

    try {
        // Charger liste a downloader
        try {
            _queueDownloadFuuids.clear()
            await chargerListeFichiersMissing(fuuid=>_queueDownloadFuuids.add(fuuid))
        } catch(err) {
            console.error(new Date() + ' storeConsignation._threadDownloadFichiersDuPrimaire Erreur chargerListeFichiersMissing ', err)
        }
    
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
                if(err.response && err.response.status >= 500 && err.response.status <= 600) {
                    console.warn("Abandon _threadDownloadFichiersDuPrimaire sur erreur serveur, redemarrage pending")
                    intervalleRedemarrageThread = 60_000  // Reessayer dans 1 minute
                    return
                }
            }
        }

    } catch(err) {
        console.error(new Date() + ' _threadDownloadFichiersDuPrimaire Erreur ', err)
    } finally {
        _timeoutStartThreadDownload = setTimeout(()=>{
            _timeoutStartThreadDownload = null
            _threadDownloadFichiersDuPrimaire()
                .catch(err=>{console.error(new Date() + ' storeConsignation._threadDownloadFichiersDuPrimaire Erreur ', err)})
        }, intervalleRedemarrageThread)
        debug(new Date() + ' _threadDownloadFichiersDuPrimaire Fin')
    }
}

async function downloadFichierDuPrimaire(fuuid) {

    // Tenter de recuperer le fichier localement
    // const recuperation = await recupererFichier(fuuid)
    // if(recuperation !== null)  {
    //     debug("downloadFichierDuPrimaire Fichier %O recupere avec succes sans download", recuperation)
    //     return
    // }

    debug("storeConsignation.downloadFichiersSync Fuuid %s manquant, debut download", fuuid)
    const urlTransfert = new URL(FichiersTransfertBackingStore.getUrlTransfert())
    const urlFuuid = new URL(urlTransfert.href)
    urlFuuid.pathname = urlFuuid.pathname + '/' + fuuid
    debug("Download %s", urlFuuid.href)

    const dirFuuid = path.join(getPathDataFolder(), 'syncDownload', fuuid)
    await fsPromises.mkdir(dirFuuid, {recursive: true})
    const fuuidFichier = path.join(dirFuuid, fuuid)  // Fichier avec position initiale - 1 seul fichier

    const fichierActifsWork = path.join(getPathDataFolder(), path.join(FICHIER_FUUIDS_ACTIFS + '.work'))
    const writeCompletes = fs.createWriteStream(fichierActifsWork, {flags: 'a', encoding: 'utf8'})

    try {
        const verificateurHachage = new VerificateurHachage(fuuid)
        const httpsAgent = getHttpsAgent()
        const reponseActifs = await axios({ 
            method: 'GET', 
            httpsAgent, 
            url: urlFuuid.href, 
            responseType: 'stream',
            timeout: TIMEOUT_AXIOS,
        })
        debug("Reponse GET actifs %s", reponseActifs.status)
        const fuuidStream = fs.createWriteStream(fuuidFichier)
        await new Promise((resolve, reject)=>{
            reponseActifs.data.on('data', chunk=>verificateurHachage.update(chunk))
            fuuidStream.on('close', resolve)
            fuuidStream.on('error', err=>{
                reject(err)
                // fuuidStream.close()
                fsPromises.unlink(fuuidFichier)
                    .catch(err=>console.warn("Erreur suppression fichier %s : %O", fuuidFichier, err))
            })
            reponseActifs.data.on('error', err=>{
                reject(err)
            })
            reponseActifs.data.pipe(fuuidStream)
        })

        // Verifier hachage - lance une exception si la verification echoue
        await verificateurHachage.verify()
        // Aucune exception, hachage OK

        debug("Fichier %s download complete", fuuid)
        await _storeConsignation.consignerFichier(dirFuuid, fuuid)

        // Ajouter a la liste de downloads completes
        writeCompletes.write(fuuid + '\n')

    } finally {
        await fsPromises.rm(dirFuuid, {recursive: true, force: true})
    }
}

async function uploaderFichiersVersPrimaire() {
    debug("uploaderFichiersVersPrimaire Debut")

    // // Detecter fichiers locaux (actifs) qui ne sont pas sur le primaire
    const actifsPrimaire = path.join(getPathDataFolder(), FICHIER_FUUIDS_ACTIFS_PRIMAIRE)
    const fuuidsLocaux = path.join(getPathDataFolder(), FICHIER_FUUIDS_ACTIFS)
    const fichierMissing = path.join(getPathDataFolder(), FICHIER_FUUIDS_MANQUANTS)

    try {
        await comparerFichiers(fuuidsLocaux, actifsPrimaire, fichierMissing)

        const readStreamFichiers = fs.createReadStream(fichierMissing)
        const rlFichiers = readline.createInterface({input: readStreamFichiers, crlfDelay: Infinity})
        for await (const line of rlFichiers) {
            // Detecter changement distant avec un fuuid dans la premiere colonne du fichier (pas d'espaces vides)
            if( ! line.startsWith('	') && ! line.startsWith('\t') ) {
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
    let reponse = await axios({method: 'GET', url: urlListe.href, httpsAgent, timeout: TIMEOUT_AXIOS})
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

    try {
        await parcourirBackup(addFichierLocal)
    } catch(err) {
        console.error(new Date() + " storeConsignation.downloadFichiersBackup Erreur parcourir backup ", err)
        return
    }

    for(let fichierBackup of reponse) {
        if(!fichierBackup) continue  // Ligne vide, skip

        try {
            if( ! fichiersBackupLocaux.has(fichierBackup) ) {
                // Downloader fichier
                debug("downloadFichiersBackup Fichier backup manquant '%s'", fichierBackup)
                const urlFichier = new URL(urlTransfert.href)
                urlFichier.pathname = path.join(urlFichier.pathname, 'backup', fichierBackup)
                const reponse = await axios({
                    method: 'GET', 
                    url: urlFichier.href, 
                    httpsAgent, 
                    responseType: 'stream',
                    timeout: TIMEOUT_AXIOS,
                })
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
            // info.corbeille_nombre = data.nombreFichiersCorbeille
            info.fichiers_taille = data.tailleActifs
            // info.corbeille_taille = data.tailleCorbeille

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

    const fichierActifsNew = path.join(pathFichiers, FICHIER_FUUIDS_ACTIFS + '.work')
    const fichierFuuidsActifsHandle = await fsPromises.open(fichierActifsNew, 'w')

    let ok = true,
        nombreFichiersActifs = 0,
        tailleActifs = 0
    try {
        const streamFuuidsActifs = fichierFuuidsActifsHandle.createWriteStream()

        const callbackTraiterFichier = async item => {
            if(!item) {
                streamFuuidsActifs.close()
                return  // Dernier fichier
            }

            const fuuid = item.filename.split('.').shift()
            streamFuuidsActifs.write(fuuid + '\n')
            nombreFichiersActifs++
            tailleActifs += item.size
        }

        await _storeConsignation.parcourirFichiers(callbackTraiterFichier)
    } catch(err) {
        console.error(new Date() + " storeConsignation.genererListeLocale ERROR : %O", err)
        ok = false
    } finally {
        await fichierFuuidsActifsHandle.close()
    }

    if(ok) {
        debug("genererListeLocale Terminer information liste")
        const info = {
            nombreFichiersActifs, 
            tailleActifs
        }
        const messageFormatte = await _mq.pki.formatterMessage(info, 'fichiers', {action: 'liste', ajouterCertificat: true})
        debug("genererListeLocale messageFormatte : ", messageFormatte)
        fsPromises.writeFile(path.join(pathFichiers, 'data.json'), JSON.stringify(messageFormatte))

        // Renommer fichiers .new
        const fichierActifs = path.join(pathFichiers, '/fuuidsActifs.txt')
        
        try { 
            // Copier le fichier de .work.txt a .txt, trier en meme temps
            await sortFile(fichierActifsNew, fichierActifs, {gzip: true})
            await fsPromises.rm(fichierActifsNew)
        } catch(err) {
            console.error("storeConsignation.genererListeLocale Erreur copie fichiers actifs : ", err)
        }

        if(_estPrimaire) {
            debug("Emettre evenement de fin du creation de liste du primaire")
            await _mq.emettreEvenement(messageFormatte, 'fichiers', {action: 'syncPret', ajouterCertificat: true})
        }
    }

    debug("genererListeLocale Fin")
}

/** Trie et compare 2 fichiers avec OS sort et comm */
async function comparerFichiers(source1, source2, destination) {

    const source1Sorted = source1 + '.sorted',
          source2Sorted = source2 + '.sorted'

    await sortFile(source1, source1Sorted)
    await sortFile(source2, source2Sorted)

    await new Promise((resolve, reject)=>{
        exec(`comm -3 ${source1Sorted} ${source2Sorted} > ${destination}`, error=>{
            if(error) return reject(error)
            else resolve()
        })
    })

    await Promise.all([
        fsPromises.unlink(source1Sorted),
        fsPromises.unlink(source2Sorted),
    ])
}

async function sortFile(src, dest, opts) {
    opts = opts || {}
    const gzip = opts.gzip || false

    let command = null
    if(src.endsWith('.gz') || opts.gzipsrc ) {
        command = `zcat ${src} | sort -u -o ${dest}`
    } else {
        command = `sort -u -o ${dest} ${src}`
    }
    if(gzip) command += ` && gzip -9fk ${dest}`

    await new Promise((resolve, reject)=>{
        exec(command, error=>{
            if(error) return reject(error)
            else resolve()
        })
    })
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

function getPathStaging() {
    return FichiersTransfertBackingStore.getPathStaging()
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

function getFichierStream(fuuid) {
    return _storeConsignation.getFichierStream(fuuid)
}

async function setEstConsignationPrimaire(primaire) {
    debug('setEstConsignationPrimaire %s', primaire)
    const courant = _estPrimaire
    _estPrimaire = primaire
    if(courant !== primaire) {
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

function ajouterFichierConsignation(item) {
    FichiersTransfertBackingStore.ajouterFichierConsignation(item)
}

module.exports = { 
    init, changerStoreConsignation, chargerConfiguration, modifierConfiguration, getInfoFichier,
    supprimerFichier, recupererFichier, 
    middlewareRecevoirFichier, middlewareReadyFichier, middlewareDeleteStaging, 
    sauvegarderBackupTransactions, rotationBackupTransactions,
    getFichiersBackupTransactionsCourant, getBackupTransaction, getBackupTransactionStream,
    getPathDataFolder, estPrimaire, setEstConsignationPrimaire,
    getUrlTransfert, getHttpsAgent, getInstanceId, ajouterDownloadPrimaire,
    processusSynchronisation, demarrerSynchronization, 
    parcourirFichiers, parcourirBackup, 
    getPathStaging,
    downloadFichiersBackup,
    getFichierStream,
    recevoirFuuidsDomaines,

    ajouterFichierConsignation,
}
