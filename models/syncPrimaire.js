const debug = require('debug')('sync:syncPrimaire')
const path = require('path')
const fs = require('fs')
const fsPromises = require('fs/promises')

const fileutils = require('./fileutils')
const { SynchronisationConsignationFuuids } = require('./synchronisationConsignation')

const FICHIER_FUUIDS_RECLAMES_LOCAUX = 'fuuidsReclamesLocaux.txt',
      FICHIER_FUUIDS_RECLAMES_ARCHIVES = 'fuuidsReclamesArchives.txt',
      FICHIER_FUUIDS_NOUVEAUX = 'fuuidsNouveaux.txt',
      FICHIER_FUUIDS_LOCAUX = 'fuuidsLocaux.txt',
      FICHIER_FUUIDS_ARCHIVES = 'fuuidsArchives.txt',
      FICHIER_FUUIDS_ORPHELINS = 'fuuidsOrphelins.txt',
      FICHIER_FUUIDS_PRESENTS = 'fuuidsPresents.txt',
      FICHIER_FUUIDS_RECLAMES = 'fuuidsReclames.txt',
      FICHIER_FUUIDS_MANQUANTS = 'fuuidsManquants.txt',
      DIR_RECLAMATIONS = 'reclamations',
      DIR_LISTINGS_EXPOSES = 'listings'

const DUREE_ATTENTE_RECLAMATIONS = 10_000

/** Gere les fichiers et catalogues de la consignation primaire */
class SynchronisationPrimaire {

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

    /** Reception de listes de fuuids a partir de chaque domaine. */
    async recevoirFuuidsReclames(fuuids, opts) {
        opts = opts || {}
        debug("recevoirFuuidsReclames %d fuuids (%O)", fuuids.length, opts)

        const archive = opts.archive || false,
              termine = opts.termine || false

        if(!this.timerRecevoirFuuidsReclames) throw Error("Message fuuids reclames recus hors d'une synchronisation")
        clearTimeout(this.timerRecevoirFuuidsReclames)

        if(fuuids !== null && fuuids.length > 0) {
            try {
                debug("recevoirFuuidsReclames %d fuuids (archive %s)", fuuids.length, archive)

                let fichierFuuids = null
                if(archive) {
                    fichierFuuids = path.join(this._path_listings, DIR_RECLAMATIONS, FICHIER_FUUIDS_RECLAMES_ARCHIVES + '.work')
                } else {
                    fichierFuuids = path.join(this._path_listings, DIR_RECLAMATIONS, FICHIER_FUUIDS_RECLAMES_LOCAUX + '.work')
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
            } finally {
                debug("recevoirFuuidsReclames dans %d secs", DUREE_ATTENTE_RECLAMATIONS / 1000)
                this.timerRecevoirFuuidsReclames = setTimeout(()=>{
                    this.rejectRecevoirFuuidsDomaine(new Error('timeout'))
                }, DUREE_ATTENTE_RECLAMATIONS)
            }
        }

        if(termine) {
            clearTimeout(this.timerRecevoirFuuidsReclames)
            this.timerRecevoirFuuidsReclames = null
            this.resolveRecevoirFuuidsDomaine()
        }

    }

    async runSync() {
        let infoConsignation = await this.genererListeFichiers()
        const reclamationComplete = await this.reclamerFichiers()
        await this.genererListeCombinees()
        await this.genererListeOperations()
        const nombreOperations = await this.moveFichiers()

        if(nombreOperations > 0) {
            debug("runSync Regenerer information de consignation apres %d operations", nombreOperations)
            infoConsignation = await this.genererListeFichiers()
        }

        debug("Information de consignation courante : ", infoConsignation)
        await this.exposerListings()  // Exposer listings pour download
    }

    arreter() {
        throw new Error('todo')
    }

    /** Genere une liste tous les fichiers locaux */
    async genererListeFichiers() {
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

        const emettreBatch = async fuuids => {
            try {
                await this.emettreBatchFuuidsVisites(fuuids)
            } catch(err) {
                console.warn(new Date() + " SynchronisationPrimaire.genererListeFichiers Erreur emission batch fuuids visite : ", err)
            }
        }

        // Generer listing repertoire local
        const repertoireLocal = new SynchronisationConsignationFuuids(this.manager)
        const fichierLocalPath = path.join(pathConsignationListings, FICHIER_FUUIDS_LOCAUX)
        const infoLocal = await repertoireLocal.genererOutputListing(fichierLocalPath, {emettreBatch})

        // Generer listing repertoire archives
        const repertoireArchives = new SynchronisationConsignationFuuids(
            this.manager, {parcourirFichiers: this.manager.parcourirArchives})
        const fichierArchivesPath = path.join(pathConsignationListings, FICHIER_FUUIDS_ARCHIVES)
        const infoArchives = await repertoireArchives.genererOutputListing(fichierArchivesPath, {emettreBatch})
        
        const repertoireOrphelins = new SynchronisationConsignationFuuids(
            this.manager, {parcourirFichiers: this.manager.parcourirOrphelins})
        const fichierOrphelinsPath = path.join(pathConsignationListings, FICHIER_FUUIDS_ORPHELINS)
        const infoOrphelins = await repertoireOrphelins.genererOutputListing(fichierOrphelinsPath)

        debug("genererListeFichiers Information local : %O\nArchives : %O\nOrphelins : %O", infoLocal, infoArchives, infoOrphelins)

        return {
            local: infoLocal,
            archives: infoArchives,
            orphelins: infoOrphelins,
        }
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
    async moveFichiers() {
        const pathMove = this.getPathMove()

        let operations = 0

        // Archiver fichier (de local vers archives)
        operations += await fileutils.chargerFuuidsListe(pathMove.localVersArchives, fuuid=>this.manager.archiverFichier(fuuid))
        
        // Reactiver fichier orphelin et deplacer vers archives
        operations += await fileutils.chargerFuuidsListe(pathMove.archivesVersOrphelins, async fuuid => {
            // On doit reactiver le fichiers puis le transferer vers archives
            await this.manager.reactiverFichier(fuuid)
            await this.manager.archiverFichier(fuuid)
        })

        // Reactiver fichiers (de orphelins ou archives vers local)
        operations += await fileutils.chargerFuuidsListe(pathMove.orphelinsVersLocal, fuuid=>this.manager.reactiverFichier(fuuid))
        operations += await fileutils.chargerFuuidsListe(pathMove.archivesVersLocal, fuuid=>this.manager.reactiverFichier(fuuid))

        // Deplacer vers orphelins
        operations += await fileutils.chargerFuuidsListe(pathMove.localVersOrphelins, fuuid=>this.manager.marquerOrphelin(fuuid))
        operations += await fileutils.chargerFuuidsListe(pathMove.archivesVersOrphelins, async fuuid => {
            // On doit reactiver le fichiers puis le transferer vers orphelins
            await this.manager.reactiverFichier(fuuid)
            await this.manager.marquerOrphelin(fuuid)
        })

        return operations
    }

    /**
     * Demande a tous les domaines avec des fichiers de relamer leurs fichiers
     */
    async reclamerFichiers() {
        debug("reclamerFichiers Debut reclamation fichiers")

        let reclamationComplete = true

        const domaines = await getListeDomainesFuuids(this.mq)

        const pathListingLocal = path.join(this._path_listings, DIR_RECLAMATIONS, FICHIER_FUUIDS_RECLAMES_LOCAUX),
              pathListingArchives = path.join(this._path_listings, DIR_RECLAMATIONS, FICHIER_FUUIDS_RECLAMES_ARCHIVES)



        // Cleanup fichiers precedents
        const pathLogsFuuids = path.join(this._path_listings, DIR_RECLAMATIONS)
        try {
            await fsPromises.rm(pathLogsFuuids, {recursive: true})
        } catch(err) {
            console.error(new Date() + " Erreur suppression %s : %O", pathLogsFuuids, err)
        }
        await fsPromises.mkdir(pathLogsFuuids, {recursive: true})

        // Cleanup listings precedents - permet de recevoir fuuidsNouveaux.txt durant sync
        const dirListingsExposes = path.join(this._path_listings, DIR_LISTINGS_EXPOSES)
        try {
            await fsPromises.rm(dirListingsExposes, {recursive: true})
        } catch(err) {
            console.error(new Date() + " Erreur suppression %s : %O", dirListingsExposes, err)
        }
        await fsPromises.mkdir(dirListingsExposes, {recursive: true})

        // Initialiser fichiers vides
        await fsPromises.writeFile(pathListingLocal + '.work', '')
        await fsPromises.writeFile(pathListingArchives + '.work', '')

        // Reclamation des fuuids de chaque domaine
        for await (const domaine of domaines) {
            try {
                reclamationComplete &= await this.reclamerFichiersDomaine(domaine)
            } catch(err) {
                console.error(`${new Date()} Erreur reclamations fichiers domaine ${domaine}, skip : ${''+err}`)
                reclamationComplete = false
            }
        }

        // Trier les fichiers
        const logs = [ pathListingLocal, pathListingArchives ]
        for await(const log of logs) {
            try {
                await fileutils.sortFile(log + '.work', log, {gzip: true})
                await fsPromises.unlink(log + '.work')
            } catch(err) {
                console.error(new Date() + " reclamerFichiers Erreur sort/compression fichier %s, on continue : %O", log, err)
            }
        }

        // Retirer d'archives tous les fichiers fuuids aussi presents dans local (integrite, eviter duplication de fichiers)
        await fileutils.trouverUniques(pathListingArchives, pathListingLocal, pathListingArchives + '.unique')
        // Recreer le liste d'archives
        await fileutils.sortFile(pathListingArchives + '.unique', pathListingArchives, {gzip: true})
        await fsPromises.unlink(pathListingArchives + '.unique')

        debug("reclamerFichiers Fin reclamation fichiers, complete %O", reclamationComplete)
        return reclamationComplete
    }
    
    async reclamerFichiersDomaine(domaine) {
        debug("reclamerFichiers Reclamer fichiers domaine : ", domaine)

        const action = 'reclamerFuuids'
        const reponse = await this.mq.transmettreCommande(domaine, {}, {action, exchange: '2.prive'})
        if(reponse.ok !== true) {
            debug("reclamerFichiersDomaine Echec demarrage reclamations domaine %s, SKIP", domaine)
            return false
        }

        try {
            const attenteRecevoirFuuidsDomaine = new Promise((resolve, reject)=>{
                // Exposer resolve/reject pour callback via recevoirFuuidsReclames
                this.resolveRecevoirFuuidsDomaine = resolve
                this.rejectRecevoirFuuidsDomaine = reject
            })
            this.timerRecevoirFuuidsReclames = setTimeout(()=>{
                this.rejectRecevoirFuuidsDomaine(new Error('timeout'))
            }, DUREE_ATTENTE_RECLAMATIONS)
            
            // Attendre
            await attenteRecevoirFuuidsDomaine

            debug("reclamerFichiersDomaine Fuuids reclames au complet (OK)")
        } finally {
            // Cleanup
            this.resolveRecevoirFuuidsDomaine = null
            this.rejectRecevoirFuuidsDomaine = null
            clearTimeout(this.timerRecevoirFuuidsReclames)
            this.timerRecevoirFuuidsReclames = null
        }

        return true
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
}

async function getListeDomainesFuuids(mq) {
    const domaine = 'CoreTopologie', action = 'listeDomaines',
          requete = {'supporte_fuuids': true}

    const reponse = await mq.transmettreRequete(domaine, requete, {action})
    debug("Reponse liste domaines : ", reponse)

    if(reponse.ok === false) throw new Error(`Erreur recuperation domaines : ${''+reponse.err}`)

    const domaines = reponse.resultats
        .map(item=>item.domaine)
        .filter(item=>['GrosFichiers', 'Messagerie'].includes(item))  // Note : fix avec filtre cote serveur

    return domaines

}

module.exports = SynchronisationPrimaire
