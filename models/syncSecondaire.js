const debug = require('debug')('sync:syncSecondaire')
const axios = require('axios')
const zlib = require('zlib')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')

const { VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')

const { SynchronisationConsignation } = require('./synchronisationConsignation')
const fileutils = require('./fileutils')

const FICHIER_FUUIDS_LOCAUX = 'fuuidsLocaux.txt',
      FICHIER_FUUIDS_ARCHIVES = 'fuuidsArchives.txt',
      FICHIER_FUUIDS_PRIMAIRE = 'fuuidsPrimaire.txt',
      FICHIER_FUUIDS_ORPHELINS = 'fuuidsOrphelins.txt',
      FICHIER_FUUIDS_PRESENTS = 'fuuidsPresents.txt'

const FICHIERS_LISTE_PATH = '/var/opt/millegrilles/consignation/staging/fichiers/liste'
const FICHIERS_LISTING_PATH = path.join(FICHIERS_LISTE_PATH, '/listings')

const EXPIRATION_ORPHELINS_SECONDAIRES = 86_400_000 * 7,
      LIMITE_TRANFERT_ITEMS = 10_000,
      TIMEOUT_AXIOS = 60_000,
      CONST_TAILLE_SPLIT_MAX_DEFAULT = 1024 * 1024 * 100

/** Gere les fichiers, catalogues et la synchronisation avec la consignation primaire pour un serveur secondaire */
class SynchronisationSecondaire extends SynchronisationConsignation {

    constructor(mq, consignationManager, syncManager) {
        super(mq, consignationManager, syncManager)

        this.pathOperationsListings = path.join(this._path_listings, 'operations')

        this.downloadPrimaireHandler = new DownloadPrimaireHandler(consignationManager, this)
        this.uploadPrimaireHandler = new UploadPrimaireHandler(consignationManager, this)
    }

    async init() {
        await super.init()
        await this.downloadPrimaireHandler.init()
        await this.uploadPrimaireHandler.init()
    }

    async runSync() {
        this.emettreEvenementActivite()
        const intervalActivite = setInterval(()=>this.emettreEvenementActivite(), 5_000)
        try {
            let infoConsignation = await this.genererListeFichiers()

            debug("runSync Download fichiers listing du primaire")
            await this.getFichiersSync()

            // Deplacer les fichiers entre local, archives et orphelins
            // Ne pas deplacer vers orphelins si reclamationComplete est false (tous les domaines n'ont pas repondus)
            await this.genererListeOperations()
            const nombreOperations = await this.moveFichiers({traiterOrphelins: true, expirationOrphelins: EXPIRATION_ORPHELINS_SECONDAIRES})
            if(nombreOperations > 0) {
                debug("runSync Regenerer information de consignation apres %d operations", nombreOperations)
                infoConsignation = await this.genererListeFichiers({emettreBatch: false})
            }
            debug("runSync Information de consignation courante : ", infoConsignation)

            debug("runSync Faire la liste des fichiers a downloader et uploader avec le primaire")
            await this.genererOperationsTransfertPrimaire()

            // Indiquer aux transfert que de nouvelles listes sont disponibles
            await this.downloadPrimaireHandler.update()
            await this.uploadPrimaireHandler.update()

        } catch(err) {
            console.error(new Date() + " runSync Erreur sync : ", err)
        } finally {
            clearInterval(intervalActivite)
            this.emettreEvenementActivite({termine: true})
            this.manager.emettrePresence()
                .catch(err=>console.error(new Date() + " SynchronisationPrimaire.runSync Erreur emettre presence : ", err))
        }        
    }

    arreter() {
        throw new Error('todo')
    }

    /**
     * Charge les fichiers d'information a partir du primaire
     */
    async getFichiersSync() {
        const httpsAgent = this.manager.getHttpsAgent()
        const urlConsignationTransfert = this.syncManager.urlConsignationTransfert

        const outputPath = FICHIERS_LISTING_PATH
        try { await fsPromises.rm(outputPath, {recursive: true}) } catch(err) { console.info("getFichiersSync Erreur suppression %s : %O", outputPath, err) }
        await fsPromises.mkdir(outputPath, {recursive: true})

        debug("getFichiersSync Get fichiers a partir du url ", urlConsignationTransfert.href)
        await downloadFichierSync(httpsAgent, urlConsignationTransfert, FICHIER_FUUIDS_LOCAUX, {outputPath})
        await downloadFichierSync(httpsAgent, urlConsignationTransfert, FICHIER_FUUIDS_ARCHIVES, {outputPath})
        await downloadFichierSync(httpsAgent, urlConsignationTransfert, 'fuuidsManquants.txt', {outputPath})
        try {
            await downloadFichierSync(httpsAgent, urlConsignationTransfert, 'fuuidsNouveaux.txt', {outputPath, gzip: false})

            // Ajouter tous les fuuids nouveaux au fichier fuuidsLocaux.txt
            await fsPromises.rename(path.join(outputPath, FICHIER_FUUIDS_LOCAUX), path.join(outputPath, FICHIER_FUUIDS_LOCAUX+'.original'))
            await fileutils.combinerSortFiles([
                path.join(outputPath, FICHIER_FUUIDS_LOCAUX+'.original'), 
                path.join(outputPath, 'fuuidsNouveaux.txt'),
            ], path.join(outputPath, FICHIER_FUUIDS_LOCAUX))
        } catch(err) {
            if(err.response && err.response.status === 404) {
                // Fichier absent, OK
                const writeStream = fs.createWriteStream(path.join(outputPath, 'fuuidsNouveaux.txt'))
                writeStream.close()  // Creer fichier vide
            } else {
                throw err
            }
        }

        // Combiner les fichiers locaux et archives pour complete liste de traitements (presents)
        const pathTraitementListings = path.join(this._path_listings, 'traitements')
        // Cleanup fichiers precedents
        try {
            await fsPromises.rm(pathTraitementListings, {recursive: true})
        } catch(err) {
            console.error(new Date() + " Erreur suppression %s : %O", pathTraitementListings, err)
        }
        await fsPromises.mkdir(pathTraitementListings, {recursive: true})

        const fichiersPrimaire = path.join(pathTraitementListings, FICHIER_FUUIDS_PRIMAIRE)
        await fileutils.combinerSortFiles([
            path.join(outputPath, FICHIER_FUUIDS_LOCAUX), 
            path.join(outputPath, FICHIER_FUUIDS_ARCHIVES),
        ], fichiersPrimaire)

        // Calculer nouveaux orphelins
        const pathConsignationListings = path.join(this._path_listings, 'consignation')
        const fichiersPresents = path.join(pathTraitementListings, FICHIER_FUUIDS_PRESENTS)
        await fileutils.combinerSortFiles([
            path.join(pathConsignationListings, FICHIER_FUUIDS_LOCAUX), 
            path.join(pathConsignationListings, FICHIER_FUUIDS_ARCHIVES),
        ], fichiersPresents)

        const fichiersOrphelins = path.join(pathTraitementListings, FICHIER_FUUIDS_ORPHELINS)
        await fileutils.trouverManquants(fichiersPrimaire, fichiersPresents, fichiersOrphelins)
    }

    async genererListeOperations() {
        const pathOperationsListings = path.join(this._path_listings, 'operations')
        // Cleanup fichiers precedents
        try {
            await fsPromises.rm(pathOperationsListings, {recursive: true})
        } catch(err) {
            console.error(new Date() + " Erreur suppression %s : %O", pathOperationsListings, err)
        }
        await fsPromises.mkdir(pathOperationsListings, {recursive: true})

        const listingPath = FICHIERS_LISTING_PATH
        const pathConsignationListings = path.join(this._path_listings, 'consignation')
        const pathTraitementListings = path.join(this._path_listings, 'traitements')

        const fichierLocalPath = path.join(pathConsignationListings, 'fuuidsLocaux.txt')
        const fichierArchivesPath = path.join(pathConsignationListings, 'fuuidsArchives.txt')

        const fichierPrimaireLocalPath = path.join(listingPath, 'fuuidsLocaux.txt')
        const fichierPrimaireArchivesPath = path.join(listingPath, 'fuuidsArchives.txt')

        const fichierOrphelinsPath = path.join(pathConsignationListings, FICHIER_FUUIDS_ORPHELINS)
        const fichierOrphelinsTraitementPath = path.join(pathTraitementListings, FICHIER_FUUIDS_ORPHELINS)
        
        const pathMove = this.getPathMove()

        // Transfert de orphelins vers local
        await fileutils.trouverPresentsTous(fichierPrimaireLocalPath, fichierOrphelinsPath, pathMove.orphelinsVersLocal)

        // Transfert de orphelins vers archives
        await fileutils.trouverPresentsTous(fichierPrimaireArchivesPath, fichierOrphelinsPath, pathMove.orphelinsVersArchives)

        // Transfert de archives vers local
        await fileutils.trouverPresentsTous(fichierPrimaireLocalPath, fichierArchivesPath, pathMove.archivesVersLocal)

        // Transfert de local vers archives
        await fileutils.trouverPresentsTous(fichierPrimaireArchivesPath, fichierLocalPath, pathMove.localVersArchives)

        // Transfert de archives vers orphelins
        await fileutils.trouverPresentsTous(fichierOrphelinsTraitementPath, fichierArchivesPath, pathMove.archivesVersOrphelins)

        // Transfert de local vers orphelins
        await fileutils.trouverPresentsTous(fichierOrphelinsTraitementPath, fichierLocalPath, pathMove.localVersOrphelins)
    }

    emettreEvenementActivite(opts) {
        opts = opts || {}
        
        const termine = opts.termine || false

        const message = {
            termine,
        }
        const domaine = 'fichiers', action = 'syncSecondaire'
        this.mq.emettreEvenement(message, {domaine, action, ajouterCertificat: true})
            .catch(err=>console.error("emettreEvenementActivite Erreur : ", err))
    }

    async genererOperationsTransfertPrimaire() {
        const pathConsignationListings = path.join(this._path_listings, 'consignation')
        const fichierLocalPath = path.join(pathConsignationListings, 'fuuidsLocaux.txt')
        const fichierArchivesPath = path.join(pathConsignationListings, 'fuuidsArchives.txt')

        const pathPrimaireListings = path.join(this._path_listings, 'listings')
        const fichierPrimaireLocalPath = path.join(pathPrimaireListings, 'fuuidsLocaux.txt')
        const fichierPrimaireArchivesPath = path.join(pathPrimaireListings, 'fuuidsArchives.txt')
        const fichierPrimaireManquantsPath = path.join(pathPrimaireListings, 'fuuidsManquants.txt')

        const pathOperationsListings = path.join(this._path_listings, 'operations')

        // Trouver fichiers a downloader de "local"
        const fichierDownloadLocalPath = path.join(pathOperationsListings, 'fuuidsDownloadsLocal.txt')
        await fileutils.trouverManquants(fichierLocalPath, fichierPrimaireLocalPath, fichierDownloadLocalPath)

        // Trouver fichiers a downloader de archives
        const fichierDownloadArchivesPath = path.join(pathOperationsListings, 'fuuidsDownloadsArchives.txt')
        await fileutils.trouverManquants(fichierArchivesPath, fichierPrimaireArchivesPath, fichierDownloadArchivesPath)

        // Trouver fichiers a uploader vers "local"
        const fichierUploadsLocalPath = path.join(pathOperationsListings, 'fuuidsUploadsLocal.txt')
        await fileutils.trouverPresentsTous(fichierLocalPath, fichierPrimaireManquantsPath, fichierUploadsLocalPath)

        // Trouver fichiers a uploader vers archives
        const fichierUploadsArchivesPath = path.join(pathOperationsListings, 'fuuidsUploadsArchives.txt')
        await fileutils.trouverPresentsTous(fichierArchivesPath, fichierPrimaireManquantsPath, fichierUploadsArchivesPath)
    }

    ajouterDownload(fuuid) {
        this.downloadPrimaireHandler.ajouterTransfert({fuuid, dateAjout: new Date()})
        this.downloadPrimaireHandler.demarrerThread()
    }

    ajouterUpload(fuuid) {
        this.uploadPrimaireHandler.ajouterTransfert({fuuid, dateAjout: new Date()})
        this.uploadPrimaireHandler.demarrerThread()
    }

}

async function downloadFichierSync(httpsAgent, urlConsignationTransfert, nomFichier, opts) {
    opts = opts || {}
    const outputPath = opts.outputPath || FICHIERS_LISTING_PATH
    const gzipFlag = opts.gzip===false?false:true

    const pathFichiersLocal = new URL(urlConsignationTransfert.href)
    if(gzipFlag) {
        pathFichiersLocal.pathname += `/sync/${nomFichier}.gz`
    } else {
        pathFichiersLocal.pathname += `/sync/${nomFichier}`
    }

    const reponse = await axios({
        method: 'GET', 
        url: pathFichiersLocal.href, 
        httpsAgent,
        responseType: 'stream',
        timeout: TIMEOUT_AXIOS,
    })
    debug("Reponse fichier %s status : %d", pathFichiersLocal.href, reponse.status)

    const writeStream = fs.createWriteStream(path.join(outputPath, nomFichier))
    const gunzip = zlib.createGunzip()
    await new Promise((resolve, reject)=>{
        writeStream.on('error', reject)
        writeStream.on('close', resolve)
        if(gzipFlag) {
            gunzip.pipe(writeStream)
            reponse.data.pipe(gunzip)
        } else {
            reponse.data.pipe(writeStream)
        }
    })
}

class TransfertHandler {

    constructor(manager, syncConsignation) {
        this.manager = manager
        this.syncConsignation = syncConsignation

        this.enCours = false
        this.pathStaging = '/var/opt/millegrilles/consignation/staging/fichiers/transferts'

        this.pending = []           // Liste ordonnee des transferts
        this.transfertsInfo = {}    // Information sur les transferts pending
    }

    async init() {
        await fsPromises.mkdir(this.pathStaging, {recursive: true})
    }

    ajouterTransfert(infoFichier, opts) {
        opts = opts || {}
        const archive = opts.archive || infoFichier.archive || false,
              demarrerThread = opts.demarrer===false?false:true

        const fuuid = infoFichier.fuuid || infoFichier

        debug("Ajouter transfert local de %s", fuuid)

        if(this.pending.length > LIMITE_TRANFERT_ITEMS) {
            debug("DownloadPrimaireHandler.update Ajouter download local de %s - SKIP, limite atteinte", fuuid)
            return
        }

        if(this.transfertsInfo[fuuid]) {
            debug("DownloadPrimaireHandler.update Ajouter download local de %s - SKIP, deja dans la liste", fuuid)
            return
        }

        this.transfertsInfo[fuuid] = { fuuid, archive, dateAjout: new Date() }
        this.pending.push(fuuid)

        if(demarrerThread) this.demarrerThread()
    }

    demarrerThread() {
        debug("TransfertHandler.demarrerThread Start")
        this._thread()
            .catch(err=>{
                console.error("demarrerThread Erreur demarrage _thread : ", err)
            })
    }

    async _thread() {
        if(this.enCours) {
            debug("_thread deja en cours, SKIP")
            return
        }

        const intervalEtatTransfert = setInterval(()=>this.emettreEtat(), 5_000)
        try {
            this.enCours = true

            this.trierPending()

            while(this.pending.length > 0) {
                const fuuid = this.pending.shift()  // Methode FIFO

                this.emettreEtat({fuuid})
                    .catch(err=>console.error("Erreur emettre etat transfert : ", err))

                debug("TransfertHandler._thread Traiter fuuid : %O", fuuid)
                const transfert = this.transfertsInfo[fuuid]
                try {
                    await this.transfererFichier(transfert)
                } catch(err) {
                    debug("TransfertHandler._thread Erreur execution operation transfert %O, passer a next() : %O", transfert, err)
                } finally {
                    debug("TransfertHandler._thread Cleanup de l'information de transfert en memoire pour %s", transfert.fuuid)
                    delete this.transfertsInfo[transfert.fuuid]
                }
            }

            await this.syncConsignation.genererListeFichiers()

            this.manager.emettrePresence()
                .catch(err=>console.error(new Date() + " SynchronisationPrimaire.runSync Erreur emettre presence : ", err))

        } finally {
            clearInterval(intervalEtatTransfert)
            this.enCours = false
            this.emettreEtat({termine: true})
                .catch(err=>console.error("Erreur emettre fin transfert : ", err))
        }
    }

    /** Transfere un fichier */
    async transfererFichier(fichier) {
        throw new Error('must override')
    }

    /** Met a jour la liste de transferts. */
    async update() {
        throw new Error('must override')
    }

    async emettreEtat(opts) {
        throw new Error('must override')
    }

    trierPending() {
        debug("Trier/dedupe la liste de fichiers pending")

        // Recuperer liste a partir du dict - pas de doublons possibles (la cle est le fuuid)
        const listeValues = Object.values(this.transfertsInfo).filter(item=>!item.enCours)

        // Trier par type (local en premier, archive) puis par date d'ajout au dict
        listeValues.sort(trierPending)

        // Remplacer la liste de pending
        this.pending = listeValues.map(item=>item.fuuid)
    }

}

function trierPending(a, b) {

    const dateA = a.dateAjout.getTime(), dateB = b.dateAjout.getTime(),
          archiveA = a.archive || false, archiveB = b.archive || false,
          fuuidA = a.fuuid, fuuidB = b.fuuid

    if(archiveA !== archiveB) {
        if(archiveA) return 1
        else return -1
    }

    if(dateA !== dateB) {
        return dateA - dateB
    }

    return fuuidA.localeCompare(fuuidB)
}

class DownloadPrimaireHandler extends TransfertHandler {

    constructor(manager, syncConsignation) {
        super(manager, syncConsignation)

        // Override pathStaging
        this.pathStaging = '/var/opt/millegrilles/consignation/staging/fichiers/download'

        this.fetchInformationEnCours = false
    }

    demarrerThread() {
        if(this.enCours) return  // Deja en cours
        debug("TransfertHandler.demarrerThread Start")

        this.fetchInformationDownloads()
            .catch(err=>console.error("TransfertHandler.demarrerThread Erreur fetchInformationDownloads", err))

        super.demarrerThread()
    }


    async update() {
        debug("DownloadPrimaireHandler update liste de fichiers a downloader")
        const fichierDownloadLocalPath = path.join(this.syncConsignation.pathOperationsListings, 'fuuidsDownloadsLocal.txt')
        const fichierDownloadArchivesPath = path.join(this.syncConsignation.pathOperationsListings, 'fuuidsDownloadsArchives.txt')

        await fileutils.chargerFuuidsListe(fichierDownloadLocalPath, fuuid=>this.ajouterTransfert(fuuid, {demarrer: false}))
        await fileutils.chargerFuuidsListe(fichierDownloadArchivesPath, fuuid=>this.ajouterTransfert(fuuid, {demarrer: false, archive: true}))

        this.trierPending()

        this.demarrerThread()

        this.fetchInformationDownloads()
            .catch(err=>console.error("Erreur fetchInformationDownloads : ", err))
    }

    async transfererFichier(transfertInfo) {
        debug("DownloadPrimaireHandler.transfererFichier fichier ", transfertInfo)
        const { fuuid } = transfertInfo

        transfertInfo.enCours = true

        // Verifier si le fichier est deja present localement
        try {
            const infoFichier = await this.manager.getInfoFichier(fuuid)
            debug("Le fichier %s existe deja (SKIP) : %O", fuuid, infoFichier)
            return
        } catch(err) {
            if(err.code === 'ENOENT') {
                // Ok, fichier n'existe pas deja
                debug("transfererFichier Verification absence fichier avant download fuuid %s OK", fuuid)
            } else {
                throw err
            }
        }

        await this.downloadFichier(transfertInfo)
    }

    async downloadFichier(transfertInfo) {
        const { fuuid, archive } = transfertInfo
        debug("DownloadPrimaireHandler.downloadFichier Debut download ", fuuid)

        const urlDownload = new URL(this.syncConsignation.syncManager.urlConsignationTransfert.href),
              httpsAgent = this.syncConsignation.syncManager.manager.getHttpsAgent()
        urlDownload.pathname += '/' + fuuid
        debug("DownloadPrimaireHandler.downloadFichier URL download fichier ", urlDownload.href)

        const pathFichierDownload = path.join(this.pathStaging, fuuid)
        const writeStream = fs.createWriteStream(pathFichierDownload)

        const controller = new AbortController()

        const reponse = await axios({
            method: 'GET', 
            url: urlDownload.href, 
            httpsAgent,
            responseType: 'stream',
            signal: controller.signal,
        })

        let timeout = setTimeout(controller.abort, 45_000)  // Timeout 15 secondes pour connexion

        // debug("Reponse headers : ", reponse.headers)
        try {
            const taille = Number.parseInt(reponse.headers['content-length'])
            transfertInfo.taille = taille
        } catch(err) {
            debug("DownloadPrimaireHandler.downloadFichier Erreur lecture taille fuuid %s a partir des headers http", fuuid, err)
            transfertInfo.taille = null
        }
        transfertInfo.position = 0

        const verificateurHachage = new VerificateurHachage(fuuid)

        debug("DownloadPrimaireHandler.downloadFichier Reponse fichier %s status : %d", fuuid, reponse.status)
        try {
            await new Promise((resolve, reject)=>{
                reponse.data.on('data', chunk => {
                    verificateurHachage.update(chunk)
                    transfertInfo.position += chunk.length
                    clearTimeout(timeout)
                    timeout = setTimeout(controller.abort, 15_000)  // Timeout 15 secondes entre chunks
                })
                writeStream.on('error', reject)
                writeStream.on('close', () => {
                    // Verifier hachage - lance une exception si la verification echoue
                    verificateurHachage.verify().then(resolve).catch(reject)
                })
                reponse.data.pipe(writeStream)
            })

            clearTimeout(timeout)

            debug("DownloadPrimaireHandler.downloadFichier Resultat transfert : ", transfertInfo)
            await this.manager.consignerFichier(this.pathStaging, fuuid)
            if(archive) {
                await this.manager.archiverFichier(fuuid)
            }
        } catch(err) {
            // Supprimer le fichier temporaire
            fsPromises.unlink(pathFichierDownload)
                .catch(err=>console.info("DownloadPrimaireHandler.downloadFichier Erreur cleanup fichier %s (download en erreur)) : ", err))
            throw err
        } finally {
            clearTimeout(timeout)
        }

    }

    /** 
     * Recupere de l'information sur les downloads a partir du primaire. 
     * Permet de generer un rapport de transfert (bytes, %, etc)
    */
    async fetchInformationDownloads() {
        if(this.fetchInformationEnCours) {
            debug("fetchInformationDownloads Deja en cours, SKIP")
            return
        }

        const urlInfo = new URL(this.syncConsignation.syncManager.urlConsignationTransfert.href),
              httpsAgent = this.syncConsignation.syncManager.manager.getHttpsAgent()
        
        urlInfo.pathname += '/sync/fuuidsInfo'
        debug("DownloadPrimaireHandler.fetchInformationDownloads URL download fichier ", urlInfo.href)

        try {
            this.fetchInformationEnCours = true

            let fuuidsInfo = Object.values(this.transfertsInfo).filter(item=>!item.fetchComplete).map(item=>item.fuuid)
            while(fuuidsInfo.length > 0) {
                const fuuidsBatch = fuuidsInfo.slice(0, 1000)
                fuuidsInfo = fuuidsInfo.slice(1000)

                debug("fetchInformationDownloads Fetch batch %d fuuids", fuuidsBatch.length)
                const requete = { fuuids: fuuidsBatch }
                const reponse = await axios({
                    method: 'POST',
                    url: urlInfo.href,
                    data: requete,
                    httpsAgent,
                    timeout: TIMEOUT_AXIOS,
                })
                const data = reponse.data
                debug("fetchInformationDownloads Info fichiers status : ", reponse.status)
                for(const infoRemote of data) {
                    const {fuuid, status, size} = infoRemote
                    const infoFuuid = this.transfertsInfo[fuuid]
                    if(!infoFuuid || infoFuuid.enCours) continue  // Fichier deja traite
                    
                    if(size) infoFuuid.taille = size
                    infoFuuid.status = status

                    infoFuuid.fetchComplete = true
                }
            }

            this.emettreEtat()
                .catch(err=>console.warn("DownloadPrimaireHandler.fetchInformationDownloads Erreur emettreEtat : ", err))
        } finally {
            this.fetchInformationEnCours = false
        }
    }

    async emettreEtat(opts) {
        const mq = this.syncConsignation.mq

        const liste = Object.values(this.transfertsInfo)
        const taille = liste.reduce((acc, item)=>{
            const taille = item.taille || 0
            acc += taille
            return acc
        }, 0)

        const e = {
            termine: !this.enCours,
            nombre: liste.length,
            taille,
        }

        debug("emettreEtat syncDownload ", e)
        const domaine = 'fichiers', action = 'syncDownload'
        await mq.emettreEvenement(e, {domaine, action, ajouterCertificat: true})
    }

}

class UploadPrimaireHandler extends TransfertHandler {

    constructor(manager, syncConsignation) {
        super(manager, syncConsignation)

        // Override pathStaging
        this.pathStaging = '/var/opt/millegrilles/consignation/staging/fichiers/upload'

        this.preparationInformationEnCours = false
    }

    async update() {
        debug("UploadPrimaireHandler update liste de fichiers a uploader")
        const fichierUploadLocalPath = path.join(this.syncConsignation.pathOperationsListings, 'fuuidsUploadsLocal.txt')
        const fichierUploadArchivesPath = path.join(this.syncConsignation.pathOperationsListings, 'fuuidsUploadsArchives.txt')

        await fileutils.chargerFuuidsListe(fichierUploadLocalPath, fuuid=>this.ajouterTransfert(fuuid))
        await fileutils.chargerFuuidsListe(fichierUploadArchivesPath, fuuid=>this.ajouterTransfert(fuuid, {archive: true}))

        this.trierPending()

        this.demarrerThread()

        this.preparerInformationUpload()
            .catch(err=>console.warn("UploadPrimaireHandler.update Erreur preparation information upload : ", err))
    }

    async transfererFichier(transfertInfo) {
        debug("UploadPrimaireHandler.transfererFichier Debut upload fichier ", transfertInfo)
        transfertInfo.enCours = true

        const { fuuid } = transfertInfo

        const statItem = (await this.manager.getInfoFichier(fuuid))
        statItem.fuuid = fuuid
        debug("UploadPrimaireHandler.putFichier ", statItem)

        if(!statItem) {
            console.error(new Date() + " transfertPrimaire.putFichier Fuuid %s n'existe pas localement, upload annule", fuuid)
            return
        }

        debug("Traiter PUT pour fuuid %s", fuuid)

        try {
            const urlInfo = new URL(this.syncConsignation.syncManager.urlConsignationTransfert.href),
                  httpsAgent = this.syncConsignation.syncManager.manager.getHttpsAgent()
            await putAxios(httpsAgent, urlInfo, fuuid, statItem)
        } catch(err) {
            const response = err.response || {}
            const status = response.status
            console.error(new Date() + " Erreur PUT fichier (status %d) %O", status, err)
            if(status === 409) {
                positionUpload = response.headers['x-position'] || position
            } else {
                throw err
            }
        }
    }

    /** Injecte l'information de taille/presence des fichiers a uploader. */
    async preparerInformationUpload() {
        this.preparationInformationEnCours = true

        try {
            const urlInfo = new URL(this.syncConsignation.syncManager.urlConsignationTransfert.href),
                  httpsAgent = this.syncConsignation.syncManager.manager.getHttpsAgent()

            urlInfo.pathname += '/sync/fuuidsInfo'
            debug("UploadPrimaireHandler.preparerInformationUpload URL download fichier ", urlInfo.href)

            let fuuidsInfo = Object.values(this.transfertsInfo).filter(item=>!item.fetchComplete).map(item=>item.fuuid)
            while(fuuidsInfo.length > 0) {
                const fuuidsBatch = fuuidsInfo.slice(0, 1000)
                fuuidsInfo = fuuidsInfo.slice(1000)

                debug("UploadPrimaireHandler.preparerInformationUpload Fetch batch %d fuuids", fuuidsBatch.length)
                const requete = { fuuids: fuuidsBatch }
                const reponse = await axios({
                    method: 'POST',
                    url: urlInfo.href,
                    data: requete,
                    httpsAgent,
                    timeout: TIMEOUT_AXIOS,
                })
                const data = reponse.data
                debug("UploadPrimaireHandler.preparerInformationUpload Info fichiers status : %d\n%O", reponse.status, data)

                for(const infoRemote of data) {
                    const { fuuid, status } = infoRemote
                    const infoFuuid = this.transfertsInfo[fuuid]
                    if(!infoFuuid || infoFuuid.fetchComplete) continue  // Fichier deja traite

                    // Verifier si le fichier est deja sur le primaire
                    if(status === 200) {
                        debug("UploadPrimaireHandler.preparerInformationUpload Le fichier %s existe deja sur le primaire, annuler transfert", fuuid)
                        delete this.transfertsInfo[fuuid]
                        continue
                    }

                    // Recuperer information locale (taille du fichier) pour stats
                    try {
                        const infoFichier = await this.manager.getInfoFichier(fuuid)
                        debug("UploadPrimaireHandler.preparerInformationUpload Info fichier %s : %O", fuuid, infoFichier)
                        infoFuuid.taille = infoFichier.stat.size
                    } catch(err) {
                        console.warn("UploadPrimaireHandler.preparerInformationUpload Le fichier %s n'existe pas ou autre erreur pour upload vers primaire, SKIP\n%O", fuuid, err)
                        delete this.transfertsInfo[fuuid]
                        continue
                    } finally {
                        infoFuuid.fetchComplete = true  // Marquer complete, en cas d'erreur d'acces a l'info
                    }

                    debug("UploadPrimaireHandler.preparerInformationUpload Fichier upload info ", infoFuuid)
                }
            }

        } finally {
            this.preparationInformationEnCours = false
        }

    }

    async emettreEtat(opts) {
        this.preparerInformationUpload()
            .catch(err=>console.warn("UploadPrimaireHandler.update Erreur preparation information upload : ", err))

        const mq = this.syncConsignation.mq

        const liste = Object.values(this.transfertsInfo)
        const taille = liste.reduce((acc, item)=>{
            const taille = item.taille || 0
            acc += taille
            return acc
        }, 0)

        const e = {
            termine: !this.enCours,
            nombre: liste.length,
            taille,
        }

        debug("UploadPrimaireHandler.emettreEtat syncDownload ", e)
        const domaine = 'fichiers', action = 'syncUpload'
        await mq.emettreEvenement(e, {domaine, action, ajouterCertificat: true})
    }

}

async function putAxios(httpsAgent, urlConsignationTransfert, fuuid, statItem) {
    debug("PUT Axios %s info %O", fuuid, statItem)
    const filePath = statItem.filePath
    const statContent = statItem.stat || {}
    const size = statContent.size
    const fileRedirect = statItem.fileRedirect

    // debug("PUT Axios %s size %d", fuuid, size)

    const correlation = fuuid

    const urlPrimaireFuuid = new URL(urlConsignationTransfert.href)
    urlPrimaireFuuid.pathname = path.join(urlPrimaireFuuid.pathname, fuuid)

    // S'assurer que le fichier n'existe pas deja
    try {
        await axios({method: 'HEAD', url: urlPrimaireFuuid.href, httpsAgent, timeout: TIMEOUT_AXIOS})
        console.error(new Date() + "transfertPrimaire.putAxios Fichier %s existe deja sur le primaire, annuler transfert", fuuid)
        return false
    } catch(err) {
        const response = err.response
        if(response && response.status === 404) {
            // OK, le fuuid n'existe pas deja
        } else {
            debug("Erreur axios : ", err)
            throw err
        }
    }

    let srcFilePath = null,
        tmpFile = null

    try {
        if(fileRedirect) {
            debug("putAxios Source transfert via %s", fileRedirect)
            tmpFile = path.join('/tmp/', fuuid + '.primaire')
            const sourceReponse = await axios({method: 'GET', url: fileRedirect, responseType: 'stream', timeout: TIMEOUT_AXIOS })
            if(sourceReponse.status !== 200) {
                throw new Error("Mauvaise reponse source (code %d) %s", sourceReponse.status, fileRedirect)
            }
            const writeStream = fs.createWriteStream(tmpFile)
            await new Promise((resolve, reject)=>{
                writeStream.on('close', resolve)
                writeStream.on('error', reject)
                sourceReponse.data.pipe(writeStream)
            })
            srcFilePath = tmpFile
        } else {
            srcFilePath = filePath
        }

        debug("putAxios Source transfert %s", srcFilePath)
        for (let position=0; position < size; position += CONST_TAILLE_SPLIT_MAX_DEFAULT) {
            // Creer output stream
            const start = position,
                end = Math.min(position+CONST_TAILLE_SPLIT_MAX_DEFAULT, size) - 1
            debug("PUT fuuid %s positions %d a %d", correlation, start, end)
    
            const urlPosition = new URL(urlConsignationTransfert.href)
            urlPosition.pathname = path.join(urlPosition.pathname, correlation, ''+position)
    
            const readStream = fs.createReadStream(srcFilePath, {start, end})
            await axios({
                method: 'PUT',
                httpsAgent,
                url: urlPosition.href,
                headers: {'content-type': 'application/stream', 'X-fuuid': fuuid},
                data: readStream,
                timeout: TIMEOUT_AXIOS,
                maxRedirects: 0,  // Eviter redirect buffer (max 10mb)
            })
        }        

    } finally {
        if(tmpFile) {
            debug("putAxios Cleanup ", tmpFile)
            await fsPromises.unlink(tmpFile)
        }
    }
        
    // Emettre message POST pour indiquer que le fichier est complete
    const transactions = { etat: { hachage: fuuid } }
    const urlCorrelation = new URL(urlConsignationTransfert.href)
    urlCorrelation.pathname = path.join(urlCorrelation.pathname, correlation)
    await axios({
        method: 'POST',
        httpsAgent,
        url: urlCorrelation.href,
        data: transactions,
        timeout: TIMEOUT_AXIOS,
    })
}

module.exports = SynchronisationSecondaire
