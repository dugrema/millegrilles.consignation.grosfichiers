const debug = require('debug')('sync:synchronisationConsignation')
const path = require('path')
const fs = require('fs')
const fsPromises = require('fs/promises')

const { MESSAGE_KINDS } = require('@dugrema/millegrilles.utiljs/src/constantes')
const fileutils = require('./fileutils')

const FICHIER_FUUIDS_RECLAMES_LOCAUX = 'fuuidsReclamesLocaux.txt',
      FICHIER_FUUIDS_RECLAMES_ARCHIVES = 'fuuidsReclamesArchives.txt',
      FICHIER_FUUIDS_LOCAUX = 'fuuidsLocaux.txt',
      FICHIER_FUUIDS_ARCHIVES = 'fuuidsArchives.txt',
      FICHIER_FUUIDS_ORPHELINS = 'fuuidsOrphelins.txt',
      FICHIER_FUUIDS_PRESENTS = 'fuuidsPresents.txt',
      FICHIER_FUUIDS_RECLAMES = 'fuuidsReclames.txt',
      FICHIER_FUUIDS_MANQUANTS = 'fuuidsManquants.txt',
      DIR_RECLAMATIONS = 'reclamations',
      DIR_LISTINGS_EXPOSES = 'listings',
      FICHIER_DATA = 'data.json'

const DUREE_ATTENTE_RECLAMATIONS = 10_000

class SynchronisationConsignation {

    constructor(mq, consignationManager) {
        if(!mq) throw new Error("mq null")
        this.mq = mq
        if(!consignationManager) throw new Error("consignationManager null")
        this.manager = consignationManager

        this._path_listings = path.join(this.manager.getPathStaging(), 'liste')

        this.timerThread = false  // null => actif, true => init en cours, int => en attente timeout, false => arrete
        
        this.resolveRecevoirFuuidsDomaine = null
        this.rejectRecevoirFuuidsDomaine = null
    }

    async runSync() {
        throw new Error('must override')
    }

    arreter() {
        throw new Error('must override')
    }

    /** Genere une liste tous les fichiers locaux */
    async genererListeFichiers(opts) {
        opts = opts || {}
        const flagEmettreBatch = opts.emettreBatch===false?false:true

        debug("genererListeFichiers Debut")
        const pathConsignationListings = path.join(this._path_listings, 'consignation')
        debug("genererListeFichiers Path des listings consignation : ", pathConsignationListings)

        // Cleanup fichiers precedents
        try {
            await fsPromises.rm(pathConsignationListings, {recursive: true})
        } catch(err) {
            console.error(new Date() + " Erreur suppression %s : %O", pathConsignationListings, err)
        }
        await fsPromises.mkdir(pathConsignationListings, {recursive: true})

        let emettreBatch = null
        if(flagEmettreBatch) {
            emettreBatch = async fuuids => {
                try {
                    await this.emettreBatchFuuidsVisites(fuuids)
                } catch(err) {
                    console.warn(new Date() + " SynchronisationPrimaire.genererListeFichiers Erreur emission batch fuuids visite : ", err)
                }
            }
        }

        // Generer listing repertoire local
        const repertoireLocal = new ConsignationRepertoireFuuids(this.manager)
        const fichierLocalPath = path.join(pathConsignationListings, FICHIER_FUUIDS_LOCAUX)
        const infoLocal = await repertoireLocal.genererOutputListing(fichierLocalPath, {emettreBatch})

        // Generer listing repertoire archives
        const repertoireArchives = new ConsignationRepertoireFuuids(
            this.manager, {parcourirFichiers: this.manager.parcourirArchives})
        const fichierArchivesPath = path.join(pathConsignationListings, FICHIER_FUUIDS_ARCHIVES)
        const infoArchives = await repertoireArchives.genererOutputListing(fichierArchivesPath, {emettreBatch})
        
        const repertoireOrphelins = new ConsignationRepertoireFuuids(
            this.manager, {parcourirFichiers: this.manager.parcourirOrphelins})
        const fichierOrphelinsPath = path.join(pathConsignationListings, FICHIER_FUUIDS_ORPHELINS)
        const infoOrphelins = await repertoireOrphelins.genererOutputListing(fichierOrphelinsPath)

        debug("genererListeFichiers Information local : %O\nArchives : %O\nOrphelins : %O", infoLocal, infoArchives, infoOrphelins)

        const resultat = {
            local: infoLocal,
            archives: infoArchives,
            orphelins: infoOrphelins,
        }

        await this.sauvegarderEtat(resultat)

        return resultat
    }

    async genererListeCombinees() {
        // Combinaison des fichiers local et archives (presents sur le systeme)
        const pathTraitementListings = path.join(this._path_listings, 'traitements')
        // Cleanup fichiers precedents
        try {
            await fsPromises.rm(pathTraitementListings, {recursive: true})
        } catch(err) {
            console.error(new Date() + " Erreur suppression %s : %O", pathTraitementListings, err)
        }
        await fsPromises.mkdir(pathTraitementListings, {recursive: true})

        // Generer un fichier combine de tous les fuuids reclames
        const fichierReclamesLocalPath = path.join(this._path_listings, DIR_RECLAMATIONS, FICHIER_FUUIDS_RECLAMES_ARCHIVES)
        const fichierReclamesArchivesPath = path.join(this._path_listings, DIR_RECLAMATIONS, FICHIER_FUUIDS_RECLAMES_LOCAUX)
        const fichierReclamesActifs = path.join(pathTraitementListings, FICHIER_FUUIDS_RECLAMES)
        await fileutils.combinerSortFiles([fichierReclamesLocalPath, fichierReclamesArchivesPath], fichierReclamesActifs)

        // Generer un fichier combine de tous les fuuids deja presents localement
        const pathConsignationListings = path.join(this._path_listings, 'consignation')
        const fichierLocalPath = path.join(pathConsignationListings, FICHIER_FUUIDS_LOCAUX)
        const fichierArchivesPath = path.join(pathConsignationListings, FICHIER_FUUIDS_ARCHIVES)
        const fichierOrphelinsPath = path.join(pathConsignationListings, FICHIER_FUUIDS_ORPHELINS)
        const fichiersPresents = path.join(pathTraitementListings, FICHIER_FUUIDS_PRESENTS)
        await fileutils.combinerSortFiles([fichierLocalPath, fichierArchivesPath, fichierOrphelinsPath], fichiersPresents)

        // Generer un fichier des fuuids orphelins (non reclames)
        const fichiersOrphelins = path.join(pathTraitementListings, FICHIER_FUUIDS_ORPHELINS)
        await fileutils.trouverManquants(fichierReclamesActifs, fichiersPresents, fichiersOrphelins)

        // Generer un fichier des fuuids manquants (reclames, non presents localement - possiblement sur un secondaire)
        const fichiersManquants = path.join(pathTraitementListings, FICHIER_FUUIDS_MANQUANTS)
        await fileutils.trouverManquants(fichiersPresents, fichierReclamesActifs, fichiersManquants)
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

        const pathReclamationsListings = path.join(this._path_listings, 'reclamations')
        const pathTraitementListings = path.join(this._path_listings, 'traitements')
        const pathConsignationListings = path.join(this._path_listings, 'consignation')

        const fichierLocalPath = path.join(pathConsignationListings, FICHIER_FUUIDS_LOCAUX)
        const fichierArchivesPath = path.join(pathConsignationListings, FICHIER_FUUIDS_ARCHIVES)

        const fichierReclamesLocalPath = path.join(pathReclamationsListings, FICHIER_FUUIDS_RECLAMES_LOCAUX)
        const fichierReclamesArchivesPath = path.join(pathReclamationsListings, FICHIER_FUUIDS_RECLAMES_ARCHIVES)

        const fichierOrphelinsPath = path.join(pathConsignationListings, FICHIER_FUUIDS_ORPHELINS)
        const fichierOrphelinsTraitementPath = path.join(pathTraitementListings, FICHIER_FUUIDS_ORPHELINS)
        
        const pathMove = this.getPathMove()

        // Transfert de orphelins vers local
        await fileutils.trouverPresentsTous(fichierReclamesLocalPath, fichierOrphelinsPath, pathMove.orphelinsVersLocal)

        // Transfert de orphelins vers archives
        await fileutils.trouverPresentsTous(fichierReclamesArchivesPath, fichierOrphelinsPath, pathMove.orphelinsVersArchives)

        // Transfert de archives vers local
        await fileutils.trouverPresentsTous(fichierReclamesLocalPath, fichierArchivesPath, pathMove.archivesVersLocal)

        // Transfert de archives vers orphelins
        await fileutils.trouverPresentsTous(fichierOrphelinsTraitementPath, fichierArchivesPath, pathMove.archivesVersOrphelins)

        // Transfert de local vers archives
        await fileutils.trouverPresentsTous(fichierReclamesArchivesPath, fichierLocalPath, pathMove.localVersArchives)

        // Transfert de local vers orphelins
        await fileutils.trouverPresentsTous(fichierOrphelinsTraitementPath, fichierLocalPath, pathMove.localVersOrphelins)
    }

    getPathMove() {
        const pathOperationsListings = path.join(this._path_listings, 'operations')

        const orphelinsVersLocal = path.join(pathOperationsListings, 'move_orphelins_vers_local.txt')
        const orphelinsVersArchives = path.join(pathOperationsListings, 'move_orphelins_vers_archives.txt')
        const archivesVersLocal = path.join(pathOperationsListings, 'move_archives_vers_local.txt')
        const archivesVersOrphelins = path.join(pathOperationsListings, 'move_archives_vers_orphelins.txt')
        const localVersArchives = path.join(pathOperationsListings, 'move_local_vers_archives.txt')
        const localVersOrphelins = path.join(pathOperationsListings, 'move_local_vers_orphelins.txt')
        return {
            orphelinsVersLocal,
            orphelinsVersArchives,
            archivesVersLocal,
            archivesVersOrphelins,
            localVersArchives,
            localVersOrphelins,
        }
    }

    /** Execute les operations de deplacements internes (move) */
    async moveFichiers(opts) {
        opts = opts || {}
        const traiterOrphelins = opts.traiterOrphelins,
              expirationOrphelins = opts.expirationOrphelins

        const pathMove = this.getPathMove()

        let operations = 0

        // Archiver fichier (de local vers archives)
        operations += await fileutils.chargerFuuidsListe(pathMove.localVersArchives, fuuid=>this.manager.archiverFichier(fuuid))
        
        // Reactiver fichier orphelin et deplacer vers archives
        operations += await fileutils.chargerFuuidsListe(pathMove.orphelinsVersArchives, async fuuid => {
            // On doit reactiver le fichiers puis le transferer vers archives
            await this.manager.reactiverFichier(fuuid)
            await this.manager.archiverFichier(fuuid)
        })

        // Reactiver fichiers (de orphelins ou archives vers local)
        operations += await fileutils.chargerFuuidsListe(pathMove.orphelinsVersLocal, fuuid=>this.manager.reactiverFichier(fuuid))
        operations += await fileutils.chargerFuuidsListe(pathMove.archivesVersLocal, fuuid=>this.manager.reactiverFichier(fuuid))

        if(traiterOrphelins) {
            // Deplacer vers orphelins
            operations += await fileutils.chargerFuuidsListe(pathMove.localVersOrphelins, fuuid=>this.manager.marquerOrphelin(fuuid))
            operations += await fileutils.chargerFuuidsListe(pathMove.archivesVersOrphelins, async fuuid => {
                // On doit reactiver le fichiers puis le transferer vers orphelins
                await this.manager.reactiverFichier(fuuid)
                await this.manager.marquerOrphelin(fuuid)
            })

            await this.manager.purgerOrphelinsExpires({expiration: expirationOrphelins})
        } else {
            debug("moveFichier - SKIP traitement orphelins")
        }

        return operations
    }

    async emettreBatchFuuidsVisites(listeFuuidsVisites) {
        const message = {
            fuuids: listeFuuidsVisites
        }
        const domaine = 'fichiers', action = 'visiterFuuids'
        await this.mq.emettreEvenement(message, {domaine, action})
    }

    async exposerListings() {
        // Note : le repertoire listings est cree/vide durant la passe de reclamation
        //        Permet de recevoir les fuuidsNouveaux.txt qui pourraient avoir ete echappes.
        const dirListingsExposes = path.join(this._path_listings, DIR_LISTINGS_EXPOSES)

        // Deplacer les fichires gzip
        const pathReclamationsListings = path.join(this._path_listings, 'reclamations')
        const fichierReclamesLocalPath = path.join(pathReclamationsListings, FICHIER_FUUIDS_RECLAMES_LOCAUX + '.gz')
        const fichierReclamesArchivesPath = path.join(pathReclamationsListings, FICHIER_FUUIDS_RECLAMES_ARCHIVES + '.gz')

        const pathTraitementListings = path.join(this._path_listings, 'traitements')
        const fichiersManquants = path.join(pathTraitementListings, FICHIER_FUUIDS_MANQUANTS + '.gz')

        await fsPromises.rename(fichierReclamesLocalPath, path.join(dirListingsExposes, FICHIER_FUUIDS_RECLAMES_LOCAUX + '.gz'))
        await fsPromises.rename(fichierReclamesArchivesPath, path.join(dirListingsExposes, FICHIER_FUUIDS_RECLAMES_ARCHIVES + '.gz'))
        await fsPromises.rename(fichiersManquants, path.join(dirListingsExposes, FICHIER_FUUIDS_MANQUANTS + '.gz'))
    }

    async sauvegarderEtat(data) {
        const dataPath = path.join(this.manager.getPathDataFolder(), FICHIER_DATA)

        let dataCourant = {}
        try {
            const contenuFichier = await fsPromises.readFile(dataPath)
            const dataEnveloppe = JSON.parse(contenuFichier)
            dataCourant = JSON.parse(dataEnveloppe.contenu)
        } catch(err) {
            debug("Fichier data.json n'existe pas, on va le generer")
        }
        dataCourant = {...dataCourant, ...data}

        const dataFormatte = await this.mq.pki.formatterMessage(
            MESSAGE_KINDS.KIND_DOCUMENT, 
            dataCourant,
            {ajouterCertificat: true}
        )

        await fsPromises.writeFile(dataPath, JSON.stringify(dataFormatte))
    }

}

class ConsignationRepertoire {

    constructor(mq) {
        if(!mq) throw new Error("mq null")
        this.mq = mq
    }

    parseFichier(item) {
        return item.filename
    }

    async genererListing(opts) {
        opts = opts || {}

        const outputStream = opts.outputStream,
              emettreBatch = opts.emettreBatch,
              tailleBatch = opts.tailleBatch || BATCH_PRESENCE_NOMBRE_MAX

        let nombreFichiers = 0,
            tailleFichiers = 0

        try {
            let listeFichiersVisites = []

            const callbackTraiterFichier = async item => {
                if(!item) {
                    return  // Dernier fichier
                }

                const nomFichier = this.parseFichier(item)
                if(emettreBatch) listeFichiersVisites.push(nomFichier)  // Cumuler fichiers en batch
                if(outputStream) outputStream.write(nomFichier + '\n')
                
                // Stats cumulatives
                nombreFichiers++
                tailleFichiers += item.size

                if(listeFichiersVisites.length >= tailleBatch) {
                    debug("Emettre batch fuuids reconnus")
                    emettreBatch(listeFichiersVisites)
                        .catch(err=>console.warn(new Date() + " SynchronisationRepertoire.genererListing (actifs loop) Erreur emission batch fichiers visite : ", err))
                    listeFichiersVisites = []
                }
            }
            
            // Lancer operation
            await this.parcourirFichiers(callbackTraiterFichier)

            if(listeFichiersVisites.length > 0) {
                emettreBatch(listeFichiersVisites)
                    .catch(err=>console.warn(new Date() + " SynchronisationRepertoire.genererListing (actifs) Erreur emission batch fichiers visite : ", err))
            }

        } catch(err) {
            console.error(new Date() + " SynchronisationRepertoire.genererListing ERROR : %O", err)
            throw err
        }

        return { nombre: nombreFichiers, taille: tailleFichiers }
    }

    async parcourirFichiers(callback, opts) {
        throw new Error('must override')
    }

    async genererOutputListing(fichierPath, opts) {
        opts = opts || {}
        const fichierWriteStream = fs.createWriteStream(fichierPath + '.work')
        
        const resultat = await this.genererListing({...opts, outputStream: fichierWriteStream})
        
        await new Promise((resolve, reject)=>{
            fichierWriteStream.close(err=>{
                if(err) return reject(err)
                resolve()
            })
        })
        
        await fileutils.sortFile(fichierPath + '.work', fichierPath, opts)  // Trier
        //await fsPromises.unlink(fichierPath + '.work')

        return resultat
    }

}

class ConsignationRepertoireFuuids extends ConsignationRepertoire {

    constructor(consignationManager, opts) {
        opts = opts || {}

        if(!consignationManager) throw new Error("consignationManager null")
        super(consignationManager.getMq())

        this.manager = consignationManager
        this.parcourirFichiers = opts.parcourirFichiers || this.manager.parcourirFichiers
    }

    parseFichier(item) {
        const fuuid = item.filename.split('.').shift()
        return fuuid
    }

}

class ConsignationRepertoireBackup extends ConsignationRepertoire {

    constructor(consignationManager, opts) {
        opts = opts || {}

        if(!consignationManager) throw new Error("consignationManager null")
        super(consignationManager.getMq())

        this.manager = consignationManager
        this.archive = opts.archive || false
    }

    parseFichier(item) {
        const pathFichierSplit = item.directory.split('/')
        const pathBase = pathFichierSplit.slice(pathFichierSplit.length-2).join('/')
    
        // Conserver uniquement le contenu de transaction/ (transaction_archive/ n'est pas copie)
        if(pathBase.startsWith('transactions/')) {
            const fichierPath = path.join(pathBase, item.filename)
            return fichierPath
        }
    }

    async parcourirFichiers(callback, opts) {
        return this.manager.parcourirBackup(callback, opts)
    }

}

module.exports = { SynchronisationConsignation, ConsignationRepertoireFuuids, ConsignationRepertoireBackup }
