const debug = require('debug')('sync:syncPrimaire')
const path = require('path')
const fs = require('fs')
const fsPromises = require('fs/promises')

const { SynchronisationConsignation } = require('./synchronisationConsignation')
const fileutils = require('./fileutils')

const FICHIER_FUUIDS_RECLAMES_LOCAUX = 'fuuidsReclamesLocaux.txt',
      FICHIER_FUUIDS_RECLAMES_ARCHIVES = 'fuuidsReclamesArchives.txt',
      FICHIER_FUUIDS_LOCAUX = 'fuuidsLocaux.txt',
      FICHIER_FUUIDS_ARCHIVES = 'fuuidsArchives.txt',
      FICHIER_FUUIDS_ORPHELINS = 'fuuidsOrphelins.txt',
      DIR_RECLAMATIONS = 'reclamations',
      DIR_LISTINGS_EXPOSES = 'listings'

const DUREE_ATTENTE_RECLAMATIONS = 10_000,
      EXPIRATION_ORPHELINS_PRIMAIRES = 86_400_000 * 21

/** Gere les fichiers et catalogues de la consignation primaire */
class SynchronisationPrimaire extends SynchronisationConsignation {

    constructor(mq, consignationManager) {
        super(mq, consignationManager)
        
        this.resolveRecevoirFuuidsDomaine = null
        this.rejectRecevoirFuuidsDomaine = null
    }

    async runSync() {

        this.emettreEvenementActivite()
        const intervalActivite = setInterval(()=>this.emettreEvenementActivite(), 3_000)
        try {
            let [infoConsignation, reclamationComplete] = await Promise.all([
                this.genererListeFichiers(),
                this.reclamerFichiers(),
            ])

            // Combiner listes locales et reclamees
            await this.genererListeCombinees()

            // Deplacer les fichiers entre local, archives et orphelins
            // Ne pas deplacer vers orphelins si reclamationComplete est false (tous les domaines n'ont pas repondus)
            await this.genererListeOperations()
            const nombreOperations = await this.moveFichiers({
                traiterOrphelins: reclamationComplete, expirationOrphelins: EXPIRATION_ORPHELINS_PRIMAIRES})
            if(nombreOperations > 0) {
                debug("runSync Regenerer information de consignation apres %d operations", nombreOperations)
                infoConsignation = await this.genererListeFichiers({emettreBatch: false})
            }
            debug("Information de consignation courante : ", infoConsignation)

            // Exposer les listings pour consignations secondaires (download)
            await this.exposerListings()
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

    emettreEvenementActivite(opts) {
        opts = opts || {}
        
        const termine = opts.termine || false

        const message = {
            termine,
        }
        const domaine = 'fichiers', action = 'syncPrimaire'
        this.mq.emettreEvenement(message, {domaine, action, ajouterCertificat: true})
            .catch(err=>console.error("emettreEvenementActivite Erreur : ", err))
    }

}

async function getListeDomainesFuuids(mq) {
    const domaine = 'CoreTopologie', 
          action = 'listeDomaines',
          requete = {'reclame_fuuids': true}

    const reponse = await mq.transmettreRequete(domaine, requete, {action})
    debug("Reponse liste domaines : ", reponse)

    if(reponse.ok === false) throw new Error(`Erreur recuperation domaines : ${''+reponse.err}`)

    return reponse.resultats.map(item=>item.domaine)
}

module.exports = SynchronisationPrimaire
