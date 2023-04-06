const debug = require('debug')('consignation:store:manager')
const path = require('path')
const https = require('https')
const fsPromises = require('fs/promises')
const fs = require('fs')
const axios = require('axios')
const { exec } = require('child_process')
const readdirp = require('readdirp')

// const FichiersTransfertBackingStore = require('@dugrema/millegrilles.nodejs/src/fichiersTransfertBackingstore')
// const { VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')

const { chargerFuuidsListe, sortFile, combinerSortFiles } = require('./fileutils')

const StoreConsignationLocal = require('./storeConsignationLocal')
const StoreConsignationSftp = require('./storeConsignationSftp')
const StoreConsignationAwsS3 = require('./storeConsignationAwsS3')

const StoreConsignationThread = require('./storeConsignationThread')
const TransfertPrimaire = require('./transfertPrimaire')
const BackupSftp = require('./backupSftp')
const { dechiffrerConfiguration } = require('./pki')

const { startConsuming: startConsumingBackup, stopConsuming: stopConsumingBackup } = require('../messages/backup')
const { startConsuming: startConsumingPrimaire, stopConsuming: stopConsumingPrimaire } = require('../messages/primaire')

const BATCH_SIZE = 100,
      TIMEOUT_AXIOS = 30_000,
      INTERVALLE_SYNC = 3_600_000,  // 60 minutes
      INTERVALLE_THREAD_TRANSFERT = 1_200_000  // 20 minutes,
      NOMBRE_ARCHIVES_ORPHELINS = 4,
      DUREE_ATTENTE_RECLAMATIONS = 10_000

const CONST_CHAMPS_CONFIG = ['type_store', 'url_download', 'consignation_url']

const FICHIER_FUUIDS_ACTIFS = 'fuuidsActifs.txt',
      FICHIER_FUUIDS_ACTIFS_PRIMAIRE = 'fuuidsActifsPrimaire.txt',
      FICHIER_FUUIDS_NOUVEAUX = 'fuuidsNouveaux.txt',
      FICHIER_FUUIDS_MANQUANTS = 'fuuidsManquants.txt',
      FICHIER_FUUIDS_MANQUANTS_PRIMAIRE = 'fuuidsManquantsPrimaire.txt',
      FICHIER_FUUIDS_PRIMAIRE = 'fuuidsPrimaire.txt',
      FICHIER_FUUIDS_ORPHELINS = 'fuuidsOrphelins.txt',
      FICHIER_FUUIDS_ARCHIVES = 'fuuidsArchives.txt',
      FICHIER_FUUIDS_ACTIFS_ARCHIVES = 'fuuidsActifsArchives.txt',
      FICHIER_FUUIDS_VERS_ARCHIVES = 'fuuidsVersArchives.txt',
      FICHIER_FUUIDS_UPLOAD_PRIMAIRE = 'fuuidsUploadPrimaire.txt',
      FICHIER_FUUIDS_DOWNLOAD_PRIMAIRE = 'fuuidsDownloadPrimaire.txt',
      FICHIER_FUUIDS_RECLAMES_ACTIFS = 'fuuidsReclamesActifs.txt',
      FICHIER_FUUIDS_RECLAMES_ARCHIVES = 'fuuidsReclamesArchives.txt',
      PATH_STAGING_DEFAUT = '/var/opt/millegrilles/consignation/staging/fichiers'

var _mq = null,
    _storeConsignationHandler = null,  // Local, sftp, aws3, etc
    _threadConsignation = null,
    _estPrimaire = false,
    _sync_lock = false,
    _derniere_sync = 0,
    _transfertPrimaire = null,
    _backupSftp = null,
    // _queueDownloadFuuids = new Set(),
    // _timeoutStartThreadDownload = null,
    _intervalleSync = INTERVALLE_SYNC,
    _syncActif = true,
    _timeoutTraiterConfirmes = null,
    _pathStaging = PATH_STAGING_DEFAUT,
    _httpsAgent = null,
    _supporteArchives = true  // Consignation conserve les fichiers archives

class ManagerFacade {

    estPrimaire() { return _estPrimaire }
    getTransfertPrimaire() { return _transfertPrimaire }
    getPathStaging() { return getPathStaging() }
    consignerFichier(pathFichierStaging, fuuid) { return consignerFichier(pathFichierStaging, fuuid) }
}

function _preparerHttpsAgent(mq) {
    const pki = mq.pki
    const {chainePEM: cert, cle: key } = pki
    if(!cert) throw new Error("storeConsignation._preparerHttpsAgent Certificat non disponible")
    if(!key) throw new Error("storeConsignation._preparerHttpsAgent Cle non disponible")
    debug("storeConsignation._preparerHttpsAgent _https.Agent cert : %s", '\n' + cert)
    _httpsAgent = new https.Agent({
        rejectUnauthorized: false,
        cert, key,
        ca: pki.ca,
    })
}

async function init(mq, opts) {
    opts = opts || {}
    _mq = mq

    try {
        // Toujours initialiser le type local - utilise pour stocker/charger la configuration
        _storeConsignationHandler = StoreConsignationLocal
        _storeConsignationHandler.init(opts)

        _pathStaging = opts.PATH_STAGING || PATH_STAGING_DEFAUT

        debug("Debut init")

        _preparerHttpsAgent(mq)  // Genere _httpsAgent

        const configuration = await _storeConsignationHandler.chargerConfiguration(opts)
        const typeStore = configuration.type_store

        const params = {...configuration, ...opts}  // opts peut faire un override de la configuration

        await modifierConfiguration(configuration)
        // await changerStoreConsignation(typeStore, params)

        // Objet responsable de l'upload vers le primaire (si local est secondaire)
        try {
            _transfertPrimaire = new TransfertPrimaire(mq, this)
            _transfertPrimaire.threadUploadFichiersConsignation()  // Premiere run, initialise loop
            await _transfertPrimaire.ready
        } catch(err) {
            console.warn("Erreur initialisation transfert primaire - assumer qu'on est en mode d'initialisation")
            // Donner 15 secondes pour initialiser la configuration de la consignation dans CoreTopologie
            setTimeout(()=>{
                _transfertPrimaire.reloadUrlTransfert()
                .catch(err=>{
                    console.error("storeConsignationManager.init Echec de chargement de la configuration de transfert - arreter")
                    // Si echec, va faire arreter l'execution
                    throw err
                })
            }, 30_000)
        }

        const managerFacade = new ManagerFacade()

        // Creer thread qui transfere les fichiers recus vers le systeme de consignation
        _threadConsignation = new StoreConsignationThread(mq, managerFacade)

        // Handler backup sftp
        _backupSftp = new BackupSftp(mq, this)
        _backupSftp.setTimeout(15_000)  // Demarrer thread dans 15 secondes

        // Entretien - emet la presence (premiere apres 10 secs, apres sous intervalles)
        debug("Entretien initial")
        setTimeout(entretien, 10_000)
        setInterval(entretien, 180_000)

        debug("init complete")
    } catch(err) {
        console.error(" !!! FATAL !!! Erreur init storeConsignationManager et handler", err)
    }

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

    if('fermer' in _storeConsignationHandler) await _storeConsignationHandler.fermer()

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
        _storeConsignationHandler = storeConsignation

        await _storeConsignationHandler.modifierConfiguration({...params, type_store: typeStore})
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

        await _storeConsignationHandler.modifierConfiguration(configuration, {override: true})
        return configuration
    } catch(err) {
        console.warn("Erreur chargement configuration via CoreTopologie, chargement local ", err)
        return await _storeConsignationHandler.chargerConfiguration(opts)
    }
    
}

async function modifierConfiguration(params, opts) {

    debug("modifierConfiguration : ", params)

    if(params.supporte_archives !== undefined) {
        _supporteArchives = params.supporte_archives
    } else {
        _supporteArchives = true
    }

    debug("modifierConfiguration Supporte archives? ", _supporteArchives)

    if(params.type_store) {
        return await changerStoreConsignation(params.type_store, params, opts)
    } else {
        return await _storeConsignationHandler.modifierConfiguration(params, opts)
    }
}

async function entretien() {
    try {
        // Determiner si on est la consignation primaire
        if(await _transfertPrimaire.ready === true) {
            const instance_id_primaire = _transfertPrimaire.getInstanceIdPrimaire()
            const instance_id_local = _mq.pki.cert.subject.getField('CN').value
            debug("entretien Instance consignation : %s, instance_id local %s", instance_id_primaire, instance_id_local)
            await setEstConsignationPrimaire(instance_id_primaire === instance_id_local)

            // Demarrer thread de consignation (aucun effet si deja en cours)
            _threadConsignation.demarrer()

            const now = new Date().getTime()
            if(_syncActif && now > _derniere_sync + _intervalleSync) {
                _derniere_sync = now  // Temporaire, pour eviter loop si un probleme survient
        
                demarrerSynchronization()
                    .catch(err=>console.error("storeConsignation.entretien() Erreur processusSynchronisation(1) ", err))
            }
        } else {
            debug("Erreur chargement information transfertPrimaire - pas de sync")
        }
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
        if(_storeConsignationHandler.entretien) await _storeConsignationHandler.entretien()
    } catch(err) {
        console.error(new Date() + ' Erreur entretien _storeConsignation ', err)
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
        await _transfertPrimaire.getDataSynchronisation()
        
        try {
            await _transfertPrimaire.uploaderFichiersVersPrimaire()
        } catch(err) {
            console.error(new Date() + " ERROR uploadFichiersVersPrimaire ", err)
        }

        try {
            await _transfertPrimaire.downloaderFichiersDuPrimaire()
        } catch(err) {
            console.error(new Date() + " ERROR downloaderFichiersDuPrimaire ", err)
        }

        try {
            await downloadFichiersBackup()
        } catch(err) {
            console.error(new Date() + " ERROR downloadFichiersBackup ", err)
        }

        try {
            await deplacementVersArchives()
        } catch(err) {
            console.error(new Date() + " ERROR deplacementVersArchives ", err)
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
    const pathFichierOrphelins = path.join(getPathDataFolder(), FICHIER_FUUIDS_ORPHELINS)
    const pathOrphelins = path.join(getPathDataFolder(), 'orphelins')
    await fsPromises.mkdir(pathOrphelins, {recursive: true})

    // Supprimer tous les orphelins dans la liste
    let pathFichiersPrimaire = null
    if(estSupporteArchives()) {
        pathFichiersPrimaire = path.join(getPathDataFolder(), FICHIER_FUUIDS_PRIMAIRE)
    } else {
        pathFichiersPrimaire = path.join(getPathDataFolder(), 'fuuidsActifsPrimaire.txt.original')
    }
    
    await rotationOrphelins(pathOrphelins, pathFichiersPrimaire)
    await fsPromises.rename(pathFichierOrphelins, path.join(pathOrphelins, 'orphelins.00'))
}

/** Reception de listes de fuuids a partir de chaque domaine. */
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
    debug("traiterFichiersConfirmes dans %d secs", DUREE_ATTENTE_RECLAMATIONS / 1000)
    _timeoutTraiterConfirmes = setTimeout(()=>{
        traiterFichiersConfirmes()
            .catch(err=>console.error("ERREUR ", err))
    }, DUREE_ATTENTE_RECLAMATIONS)
}

/** Compare les fichiers reclames (confirmes) par chaque domaine au contenu consigne. */
async function traiterFichiersConfirmes() {
    try {
        debug("traiterFichiersConfirmes")
        if(_timeoutTraiterConfirmes) clearTimeout(_timeoutTraiterConfirmes)
        _timeoutTraiterConfirmes = null

        const fichierActifs = path.join(getPathDataFolder(), FICHIER_FUUIDS_ACTIFS)

        const fichierFuuidsReclamesActifs = path.join(getPathDataFolder(), FICHIER_FUUIDS_RECLAMES_ACTIFS)
        const fichierFuuidsReclamesActifsTmp = path.join(getPathDataFolder(), FICHIER_FUUIDS_RECLAMES_ACTIFS + '.tmp')
        const fichierFuuidsReclamesActifsCourants = path.join(getPathDataFolder(), 'fuuidsReclamesActifs.courant.txt')
        const fuuidsReclamesArchives = path.join(getPathDataFolder(), FICHIER_FUUIDS_RECLAMES_ARCHIVES)
        const fuuidsReclamesArchivesTmp = path.join(getPathDataFolder(), FICHIER_FUUIDS_RECLAMES_ARCHIVES + '.tmp')
        const fichierFuuidsReclamesArchivesCourants = path.join(getPathDataFolder(), 'fuuidsReclamesArchives.courant.txt')

        const fichierActifsArchives = path.join(getPathDataFolder(), FICHIER_FUUIDS_ACTIFS_ARCHIVES)

        const pathOrphelins = path.join(getPathDataFolder(), 'orphelins')
        await fsPromises.mkdir(pathOrphelins, {recursive: true})

        // Traitement actifs
        try {
            // Trier, retirer doubles des fuuids reclames actifs
            await fsPromises.rename(fichierFuuidsReclamesActifs, fichierFuuidsReclamesActifsTmp)
            await sortFile(fichierFuuidsReclamesActifsTmp, fichierFuuidsReclamesActifsCourants)
            await fsPromises.rm(fichierFuuidsReclamesActifsTmp)
        } catch(err) {
            debug("Aucun fichier de fuuids reclames, skip")
            return
        }

        // Traitement archives
        try {
            await fsPromises.rename(fuuidsReclamesArchives, fuuidsReclamesArchivesTmp)
            await sortFile(fuuidsReclamesArchivesTmp, fichierFuuidsReclamesArchivesCourants, {gzip: true})
            await fsPromises.rm(fuuidsReclamesArchivesTmp)

            debug("Traitement des fichiers archives")
            try {
                if(estSupporteArchives()) {
                    // Ajouter fichiers archives a fichierFuuidsReclamesActifsCourants (empecher ajout dans orphelins)
                    await combinerSortFiles(
                        [fichierFuuidsReclamesActifsCourants, fichierFuuidsReclamesArchivesCourants], 
                        fichierFuuidsReclamesActifsCourants
                    )

                    // Extraire liste de fichiers actifs a transferer vers archives
                    const fuuidsVersArchives = path.join(getPathDataFolder(), FICHIER_FUUIDS_VERS_ARCHIVES)
                    await new Promise((resolve, reject)=>{
                        exec(`comm -12 ${fichierActifs} ${fichierFuuidsReclamesArchivesCourants} > ${fuuidsVersArchives}`, error=>{
                            if(error) return reject(error)
                            else resolve()
                        })
                    })

                    await deplacementVersArchives()
                }
            } catch(err) {
                console.error(new Date() + " traiterFichiersConfirmes ERROR Traitement fichiers archives : ", err)
            }
        } catch(err) {
            if(err.code === 'ENOENT') {
                debug("Aucun fichier de fuuids archives reclames, skip")
            } else {
                throw err
            }
        }

        try {
            // Faire la liste des fuuids inconnus (reclames mais pas dans actifs / archives)
            const fuuidsManquants = path.join(getPathDataFolder(), FICHIER_FUUIDS_MANQUANTS)
            await new Promise((resolve, reject)=>{
                exec(`comm -13 ${fichierActifsArchives} ${fichierFuuidsReclamesActifsCourants} > ${fuuidsManquants} && gzip -9fk ${fuuidsManquants}`, error=>{
                    if(error) return reject(error)
                    else resolve()
                })
            })
        } catch(err) {
            console.error(new Date() + " traiterFichiersConfirmes ERROR Traitement fichiers inconnus : ", err)
            return
        }

        // Traitement orphelins
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
            return
        }

        // Declencher sync sur les consignations secondaires
        await _mq.emettreEvenement({}, 'fichiers', {action: 'declencherSyncSecondaire', ajouterCertificat: true})

    } catch(err) {
        console.error(new Date() + " traiterFichiersConfirmes ERROR Erreur traitement : ", err)
    }

}

async function deplacementVersArchives() {
    const fuuidsVersArchives = path.join(getPathDataFolder(), FICHIER_FUUIDS_VERS_ARCHIVES)
    debug("Deplacer fichiers de la liste a archvier : %s", fuuidsVersArchives)

    const pathArchives = path.join(getPathDataFolder(), 'archives')
    await fsPromises.mkdir(pathArchives, {recursive: true})

    const cb = async fuuid => {
        debug("Deplacer fuuid vers archives : ", fuuid)
        try {
            await _storeConsignationHandler.archiverFichier(fuuid)
        } catch(err) {
            if(err.code === 'ENOENT') {
                // Ok, fichier deja archive ou inconnu
            } else {
                console.warn(new Date() + " WARN Echec marquer fichier %s comme archive : %O", fuuid, err)
            }
        }            
    }
    
    await chargerFuuidsListe(fuuidsVersArchives, cb)
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
        
        const cb = async fuuid => {
            debug("Deplacer fuuid vers orphelins : ", fuuid)
            try {
                await _storeConsignationHandler.marquerOrphelin(fuuid)
            } catch(err) {
                if(err.code === 'ENOENT') {
                    // Ok, fichier deja retire
                } else {
                    console.warn(new Date() + " WARN Echec marquer fichier %s comme orphelin : %O", fuuid, err)
                }
            }            
        }
        
        await chargerFuuidsListe(fichierOrphelins, cb)
    }

    try {
        await _storeConsignationHandler.purgerOrphelinsExpires()
    } catch(err) {
        console.error(new Date() + " ERROR Purger orphelins ", err)
    }

}

function ajouterDownloadPrimaire(fuuid) {
    if(_transfertPrimaire.ready === true) {
        _transfertPrimaire.ajouterDownload(fuuid)
    } else {
        _transfertPrimaire.ready.then(()=>{
            _transfertPrimaire.ajouterDownload(fuuid)
        })
        .catch(err=>console.error("Erreur ajouterDownloadPrimaire %s : %O", fuuid, err))
    }
}

async function downloadFichiersBackup() {
    const urlTransfert = new URL(_transfertPrimaire.getUrlTransfert())
    const httpsAgent = getHttpsAgent()

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
                await _storeConsignationHandler.pipeBackupTransactionStream(pathFichierBase, downloadStream)

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
            await _storeConsignationHandler.deleteBackupTransaction(pathFichierBase)
        } catch(err) {
            console.error(new Date() + ' Erreur suppression fichier backup ', fichierBackup)
        }
    }
}

function getPathDataFolder() {
    // return path.join(FichiersTransfertBackingStore.getPathStaging(), 'liste')
    return path.join(_pathStaging, 'liste')
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
            info.archives_nombre = data.nombreFichiersArchives
            info.orphelins_nombre = data.nombreFichiersOrphelins
            info.fichiers_taille = data.tailleActifs
            info.archives_taille = data.tailleArchives
            info.orphelins_taille = data.tailleOrphelins

        } catch(err) {
            console.error("storeConsignationLocal.emettrePresence ERROR Erreur chargement fichier data.json : %O", err)
        }

        await _mq.emettreEvenement(info, 'fichiers', {action: 'presence', attacherCertificat: true})
    } catch(err) {
        console.error("storeConsignation.emettrePresence Erreur emission presence : ", err)
    }
}

function evenementConsignationFichierPrimaire(mq, fuuid) {
    // Emettre evenement aux secondaires pour indiquer qu'un nouveau fichier est pret
    debug("Evenement consignation primaire sur", fuuid)
    const evenement = {fuuid}
    mq.emettreEvenement(evenement, 'fichiers', {action: 'consignationPrimaire', exchange: '2.prive', attacherCertificat: true})
        .catch(err => console.error(new Date() + " uploadFichier.evenementFichierPrimaire Erreur ", err))
}

/** Genere une liste locale de tous les fuuids */
async function genererListeLocale() {
    debug("genererListeLocale Debut")

    const pathFichiers = getPathDataFolder()
    const pathFichierNouveaux = path.join(pathFichiers, FICHIER_FUUIDS_NOUVEAUX)
    fsPromises.rm(pathFichierNouveaux)
        .catch(()=>debug("Echec suppression fichier fuuidsNouveaux.txt (OK)"))
    debug("genererListeLocale Fichiers sous ", pathFichiers)
    await fsPromises.mkdir(pathFichiers, {recursive: true})

    const fichierActifsNew = path.join(pathFichiers, FICHIER_FUUIDS_ACTIFS + '.work')
    const fichierFuuidsActifsHandle = await fsPromises.open(fichierActifsNew, 'w')
    const fichierArchivesNew = path.join(pathFichiers, FICHIER_FUUIDS_ARCHIVES + '.work')
    const fichierFuuidsArchivesHandle = await fsPromises.open(fichierArchivesNew, 'w')

    let nombreFichiersActifs = 0,
        tailleActifs = 0,
        nombreFichiersArchives = 0,
        tailleArchives = 0,
        nombreFichiersOrphelins = 0,
        tailleOrphelins = 0

    // Calculer actifs
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
        await _storeConsignationHandler.parcourirFichiers(callbackTraiterFichier)
    } catch(err) {
        console.error(new Date() + " storeConsignation.genererListeLocale ERROR Actifs : %O", err)
        throw err
    } finally {
        await fichierFuuidsActifsHandle.close()
    }

    // Calculer archives
    try {
        const streamFuuidsArchives = fichierFuuidsArchivesHandle.createWriteStream()
        const callbackTraiterFichier = async item => {
            if(!item) {
                streamFuuidsArchives.close()
                return  // Dernier fichier
            }
            const fuuid = item.filename.split('.').shift()
            streamFuuidsArchives.write(fuuid + '\n')
            nombreFichiersArchives++
            tailleArchives += item.size
        }
        await _storeConsignationHandler.parcourirArchives(callbackTraiterFichier)
    } catch(err) {
        console.error(new Date() + " storeConsignation.genererListeLocale ERROR Archives : %O", err)
    } finally {
        await fichierFuuidsArchivesHandle.close()
    }

    // Calculer orphelins
    try {
        const callbackTraiterFichier = async item => {
            if(!item) {
                return  // Dernier fichier
            }
            nombreFichiersOrphelins++
            tailleOrphelins += item.size
        }
        await _storeConsignationHandler.parcourirOrphelins(callbackTraiterFichier)
    } catch(err) {
        console.error(new Date() + " storeConsignation.genererListeLocale ERROR Orphelins : %O", err)
    }

    debug("genererListeLocale Terminer information liste")
    const info = {
        nombreFichiersActifs, tailleActifs,
        nombreFichiersArchives, tailleArchives,
        nombreFichiersOrphelins, tailleOrphelins,
    }
    const messageFormatte = await _mq.pki.formatterMessage(info, 'fichiers', {action: 'liste', ajouterCertificat: true})
    debug("genererListeLocale messageFormatte : ", messageFormatte)
    fsPromises.writeFile(path.join(pathFichiers, 'data.json'), JSON.stringify(messageFormatte))

    // Copier le fichier de .work.txt a .txt, trier en meme temps
    try { 
        // Actifs
        const fichierActifs = path.join(pathFichiers, FICHIER_FUUIDS_ACTIFS)
        await sortFile(fichierActifsNew, fichierActifs, {gzip: true})
        await fsPromises.rm(fichierActifsNew)

        // Archives
        const fichierArchives = path.join(pathFichiers, FICHIER_FUUIDS_ARCHIVES)
        await sortFile(fichierArchivesNew, fichierArchives, {gzip: true})
        await fsPromises.rm(fichierArchivesNew)

        // Combinaison des fichiers actifs et archives (presents sur le systeme)
        const fichierActifsArchives = path.join(pathFichiers, FICHIER_FUUIDS_ACTIFS_ARCHIVES)
        await combinerSortFiles([fichierActifs, fichierArchives], fichierActifsArchives)

    } catch(err) {
        console.error("storeConsignation.genererListeLocale Erreur copie fichiers actifs : ", err)
    }

    if(_estPrimaire) {
        debug("Emettre evenement de fin du creation de liste du primaire")
        await _mq.emettreEvenement(messageFormatte, 'fichiers', {action: 'syncPret', ajouterCertificat: true})
    }

    debug("genererListeLocale Fin")
}

function parcourirFichiers(callback, opts) {
    return _storeConsignationHandler.parcourirFichiers(callback, opts)
}

function parcourirBackup(callback, opts) {
    return _storeConsignationHandler.parcourirBackup(callback, opts)
}

function supprimerFichier(fuuid) {
    return _storeConsignationHandler.marquerSupprime(fuuid)
}

// function recupererFichier(fuuid) {
//     return _storeConsignationHandler.recoverFichierSupprime(fuuid)
// }

async function reactiverFuuids(fuuids) {
    const recuperes = [], inconnus = [], errors = []

    for await (const fuuid of fuuids) {
        try {
            await _storeConsignationHandler.reactiverFichier(fuuid)
            recuperes.push(fuuid)
        } catch(err) {
            if(err.code === 1) {
                inconnus.push(fuuid)
            } else {
                errors.push({fuuid, err: ''+err})
            }
        }
    }

    return {recuperes, inconnus, errors}
}

function getInfoFichier(fuuid, opts) {
    opts = opts || {}
    return _storeConsignationHandler.getInfoFichier(fuuid, {...opts, supporteArchives: estSupporteArchives()})
}

// function getInstanceId() {
//     return FichiersTransfertBackingStore.getInstanceId()
// }

function getPathStaging() {
    return _pathStaging
}

// function getUrlTransfert() {
//     return new URL(FichiersTransfertBackingStore.getUrlTransfert())
// }

function getHttpsAgent() {
    return _httpsAgent
}

function sauvegarderBackupTransactions(message) {
    return _storeConsignationHandler.sauvegarderBackupTransactions(message)
}

function rotationBackupTransactions() {
    return _storeConsignationHandler.rotationBackupTransactions()
}

function getFichiersBackupTransactionsCourant(mq, replyTo) {
    return _storeConsignationHandler.getFichiersBackupTransactionsCourant(mq, replyTo)
}

function getBackupTransaction(pathBackupTransaction) {
    return _storeConsignationHandler.getBackupTransaction(pathBackupTransaction)
}

function getBackupTransactionStream(pathBackupTransaction) {
    return _storeConsignationHandler.getBackupTransactionStream(pathBackupTransaction)
}

function estPrimaire() {
    return _estPrimaire
}

function getFichierStream(fuuid) {
    return _storeConsignationHandler.getFichierStream(fuuid, {supporteArchives: estSupporteArchives()})
}

function estSupporteArchives() {
    return _estPrimaire || _supporteArchives
}

async function setEstConsignationPrimaire(primaire, instanceIdPrimaire) {
    debug('setEstConsignationPrimaire %s', primaire)
    const courant = _estPrimaire
    _estPrimaire = primaire
    if(instanceIdPrimaire) {
        _transfertPrimaire.setInstanceIdPrimaire(instanceIdPrimaire)
    }
    if(courant !== primaire) {
        debug("Changement role consignation : primaire => %s", primaire)
        // FichiersTransfertBackingStore.setEstPrimaire(primaire)
        if(_estPrimaire === true) {
            // Ecouter Q de backup sur MQ
            startConsumingPrimaire().catch(err=>console.error(new Date() + ' Erreur start consuming primaire', err))
            startConsumingBackup().catch(err=>console.error(new Date() + ' Erreur start consuming backup', err))
        } else {
            // Arret ecoute de Q de backup sur MQ
            stopConsumingPrimaire().catch(err=>console.error(new Date() + ' Erreur stop consuming primaire', err))
            stopConsumingBackup().catch(err=>console.error(new Date() + ' Erreur stop consuming backup', err))
        }
    }
}

function ajouterFichierConsignation(fuuid) {
    _threadConsignation.ajouterFichierConsignation(fuuid)
}

async function consignerFichier(pathFichierStaging, fuuid) {
    debug("storeConsignationManager Consigner %s a partir de %s", fuuid, pathFichierStaging)
    await _storeConsignationHandler.consignerFichier(pathFichierStaging, fuuid)

    // Ajouter fichier a la fuuidsActifs.nouveau.txt
    const pathFichierNouveaux = path.join(getPathDataFolder(), FICHIER_FUUIDS_NOUVEAUX)
    const writeStream = fs.createWriteStream(pathFichierNouveaux, {flags: 'a'})
    writeStream.write(fuuid + '\n')
}

module.exports = { 
    init, changerStoreConsignation, chargerConfiguration, modifierConfiguration, getInfoFichier,
    supprimerFichier, 

    sauvegarderBackupTransactions, rotationBackupTransactions,
    getFichiersBackupTransactionsCourant, getBackupTransaction, getBackupTransactionStream,
    
    getPathDataFolder, estPrimaire, setEstConsignationPrimaire, estSupporteArchives,
    getHttpsAgent, ajouterDownloadPrimaire,
    consignerFichier, reactiverFuuids,
    
    processusSynchronisation, demarrerSynchronization, 
    parcourirFichiers, parcourirBackup, ajouterFichierConsignation,
    
    getPathStaging,
    downloadFichiersBackup,
    getFichierStream,
    recevoirFuuidsDomaines,
}
