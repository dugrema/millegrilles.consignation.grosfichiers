const debug = require('debug')('sync:syncSecondaire')
const axios = require('axios')
const zlib = require('zlib')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')

const { SynchronisationConsignation } = require('./synchronisationConsignation')
const fileutils = require('./fileutils')

const FICHIER_FUUIDS_LOCAUX = 'fuuidsLocaux.txt',
      FICHIER_FUUIDS_ARCHIVES = 'fuuidsArchives.txt',
      FICHIER_FUUIDS_PRIMAIRE = 'fuuidsPrimaire.txt',
      FICHIER_FUUIDS_ORPHELINS = 'fuuidsOrphelins.txt',
      FICHIER_FUUIDS_PRESENTS = 'fuuidsPresents.txt'

const FICHIERS_LISTE_PATH = '/var/opt/millegrilles/consignation/staging/fichiers/liste'
const FICHIERS_LISTING_PATH = path.join(FICHIERS_LISTE_PATH, '/listings')

const EXPIRATION_ORPHELINS_SECONDAIRES = 86_400_000 * 7

/** Gere les fichiers, catalogues et la synchronisation avec la consignation primaire pour un serveur secondaire */
class SynchronisationSecondaire extends SynchronisationConsignation {

    constructor(mq, consignationManager) {
        super(mq, consignationManager)

        this.downloadPrimaireHandler = new DownloadPrimaireHandler()
        this.uploadPrimaireHandler = new UploadPrimaireHandler()
    }

    async runSync(syncManager) {
        this.emettreEvenementActivite()
        const intervalActivite = setInterval(()=>this.emettreEvenementActivite(), 5_000)
        try {
            const infoConsignation = await this.genererListeFichiers()

            debug("runSync Download fichiers listing du primaire")
            await this.getFichiersSync(syncManager.urlConsignationTransfert)

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
    async getFichiersSync(urlConsignationTransfert) {
        const httpsAgent = this.manager.getHttpsAgent()

        const outputPath = FICHIERS_LISTING_PATH
        try { await fsPromises.rm(outputPath, {recursive: true}) } catch(err) { console.info("getFichiersSync Erreur suppression %s : %O", outputPath, err) }
        await fsPromises.mkdir(outputPath, {recursive: true})

        debug("getFichiersSync Get fichiers a partir du url ", urlConsignationTransfert.href)
        await downloadFichierSync(httpsAgent, urlConsignationTransfert, FICHIER_FUUIDS_LOCAUX, {outputPath})
        await downloadFichierSync(httpsAgent, urlConsignationTransfert, FICHIER_FUUIDS_ARCHIVES, {outputPath})
        await downloadFichierSync(httpsAgent, urlConsignationTransfert, 'fuuidsManquants.txt', {outputPath})

        // Combiner les fichiers locaux et archives pour complete liste de traitements (presents)
        const pathTraitementListings = path.join(this._path_listings, 'traitements')
        // Cleanup fichiers precedents
        try {
            await fsPromises.rm(pathTraitementListings, {recursive: true})
        } catch(err) {
            console.error(new Date() + " Erreur suppression %s : %O", pathTraitementListings, err)
        }
        await fsPromises.mkdir(pathTraitementListings, {recursive: true})

        const pathPrimaireListings = path.join(this._path_listings, 'listings')
        const fichiersPrimaire = path.join(pathTraitementListings, FICHIER_FUUIDS_PRIMAIRE)
        await fileutils.combinerSortFiles([
            path.join(pathPrimaireListings, FICHIER_FUUIDS_LOCAUX), 
            path.join(pathPrimaireListings, FICHIER_FUUIDS_ARCHIVES),
        ], fichiersPrimaire)

        // Calculer nouveaux orphelins
        const fichiersPresents = path.join(pathTraitementListings, FICHIER_FUUIDS_PRESENTS)
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
        const fichierDownloadArchivesPath = path.join(pathOperationsListings, 'fuuidsDownloadArchives.txt')
        await fileutils.trouverManquants(fichierArchivesPath, fichierPrimaireArchivesPath, fichierDownloadArchivesPath)

        // Trouver fichiers a uploader vers "local"
        const fichierUploadsLocalPath = path.join(pathOperationsListings, 'fuuidsUploadsLocal.txt')
        await fileutils.trouverManquants(fichierPrimaireManquantsPath, fichierLocalPath, fichierUploadsLocalPath)

        // Trouver fichiers a uploader vers archives
        const fichierUploadsArchivesPath = path.join(pathOperationsListings, 'fuuidsUploadsArchives.txt')
        await fileutils.trouverManquants(fichierPrimaireManquantsPath, fichierArchivesPath, fichierUploadsArchivesPath)
    }

}

async function downloadFichierSync(httpsAgent, urlConsignationTransfert, nomFichier, opts) {
    opts = opts || {}
    const outputPath = opts.outputPath || FICHIERS_LISTING_PATH
    const pathFichiersLocal = new URL(urlConsignationTransfert.href)
    pathFichiersLocal.pathname += `/sync/${nomFichier}.gz`
    const reponse = await axios({
        method: 'GET', 
        url: pathFichiersLocal.href, 
        httpsAgent,
        responseType: 'stream',
    })
    debug("Reponse fichier %s status : %d", pathFichiersLocal.href, reponse.status)
    const writeStream = fs.createWriteStream(path.join(outputPath, nomFichier))
    const gunzip = zlib.createGunzip()
    await new Promise((resolve, reject)=>{
        writeStream.on('error', reject)
        writeStream.on('close', resolve)
        gunzip.pipe(writeStream)
        reponse.data.pipe(gunzip)
    })
}

class TransfertHandler {

    constructor() {
        this.enCours = false
        this.pending = []
    }

    async _thread() {
        if(this.enCours) {
            debug("_thread deja en cours, SKIP")
            return
        }
        try {
            this.enCours = true
            while(this.pending.length > 0) {
                const transfert = this.pending.unshift()  // Methode FIFO
                try {
                    await this.transfererFichier(transfert)
                } catch(err) {
                    debug("_thread Erreur execution operation transfert %O, passer a next() : %O", transfert, err)
                }
            }
        } finally {
            this.enCours = false
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

}

class DownloadPrimaireHandler extends TransfertHandler {

    async update() {
        debug("DownloadPrimaireHandler update liste de fichiers a downloader")
    }

}

class UploadPrimaireHandler extends TransfertHandler {

    async update() {
        debug("UploadPrimaireHandler update liste de fichiers a uploader")
    }

}

module.exports = SynchronisationSecondaire
