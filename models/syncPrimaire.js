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
      DIR_RECLAMATIONS = 'reclamations'

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
        await this.genererListeFichiers()
        const reclamationComplete = await this.reclamerFichiers()
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

        // Generer listing repertoire local
        const repertoireLocal = new SynchronisationConsignationFuuids(this.manager)
        const fichierLocalPath = path.join(pathConsignationListings, FICHIER_FUUIDS_LOCAUX)
        await repertoireLocal.genererOutputListing(fichierLocalPath)

        // Generer listing repertoire archives
        const repertoireArchives = new SynchronisationConsignationFuuids(
            this.manager, {parcourirFichiers: this.manager.parcourirArchives})
        const fichierArchivesPath = path.join(pathConsignationListings, FICHIER_FUUIDS_ARCHIVES)
        await repertoireArchives.genererOutputListing(fichierArchivesPath)

        const repertoireOrphelins = new SynchronisationConsignationFuuids(
            this.manager, {parcourirFichiers: this.manager.parcourirOrphelins})
        const fichierOrphelinsPath = path.join(pathConsignationListings, FICHIER_FUUIDS_ORPHELINS)
        await repertoireOrphelins.genererOutputListing(fichierOrphelinsPath)

        debug("genererListeFichiers Fin")
    }

    /**
     * Demande a tous les domaines avec des fichiers de relamer leurs fichiers
     */
    async reclamerFichiers() {
        debug("reclamerFichiers Debut reclamation fichiers")

        let reclamationComplete = true

        const domaines = await getListeDomainesFuuids(this.mq)

        // Cleanup fichiers precedents
        const pathLogsFuuids = path.join(this._path_listings, DIR_RECLAMATIONS)
        try {
            await fsPromises.rm(pathLogsFuuids, {recursive: true})
        } catch(err) {
            console.error(new Date() + " Erreur suppression %s : %O", pathLogsFuuids, err)
        }
        await fsPromises.mkdir(pathLogsFuuids, {recursive: true})

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
        const logs = [
            path.join(this._path_listings, DIR_RECLAMATIONS, FICHIER_FUUIDS_RECLAMES_ARCHIVES),
            path.join(this._path_listings, DIR_RECLAMATIONS, FICHIER_FUUIDS_RECLAMES_LOCAUX),
        ]
        for await(const log of logs) {
            try {
                await fileutils.sortFile(log + '.work', log)
                await fsPromises.unlink(log + '.work')
            } catch(err) {
                console.error(new Date() + " reclamerFichiers Erreur sort/compression fichier %s, on continue : %O", log, err)
            }
        }

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

    /** Genere une liste locale de tous les fuuids */
    async OLD__genererListeLocale() {
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
            let listeFuuidsVisites = []

            const streamFuuidsActifs = fichierFuuidsActifsHandle.createWriteStream()
            const callbackTraiterFichier = async item => {
                if(!item) {
                    streamFuuidsActifs.close()
                    return  // Dernier fichier
                }

                const fuuid = item.filename.split('.').shift()
                listeFuuidsVisites.push(fuuid)
                streamFuuidsActifs.write(fuuid + '\n')
                nombreFichiersActifs++
                tailleActifs += item.size

                if(listeFuuidsVisites.length >= BATCH_PRESENCE_NOMBRE_MAX) {
                    debug("Emettre batch fuuids reconnus")
                    this.emettreBatchFuuidsVisites(listeFuuidsVisites)
                        .catch(err=>console.warn(new Date() + " storeConsignationManager.genererListeLocale (actifs loop) Erreur emission batch fuuids visite : ", err))
                    listeFuuidsVisites = []
                }
            }
            await _storeConsignationHandler.parcourirFichiers(callbackTraiterFichier)
            if(listeFuuidsVisites.length > 0) {
                this.emettreBatchFuuidsVisites(listeFuuidsVisites)
                    .catch(err=>console.warn(new Date() + " storeConsignationManager.genererListeLocale (actifs) Erreur emission batch fuuids visite : ", err))
            }
        } catch(err) {
            console.error(new Date() + " storeConsignation.genererListeLocale ERROR Actifs : %O", err)
            throw err
        } finally {
            await fichierFuuidsActifsHandle.close()
        }

        // Calculer archives
        try {
            let listeFuuidsVisites = []

            const streamFuuidsArchives = fichierFuuidsArchivesHandle.createWriteStream()
            const callbackTraiterFichier = async item => {
                if(!item) {
                    streamFuuidsArchives.close()
                    return  // Dernier fichier
                }
                const fuuid = item.filename.split('.').shift()
                listeFuuidsVisites.push(fuuid)
                streamFuuidsArchives.write(fuuid + '\n')
                nombreFichiersArchives++
                tailleArchives += item.size

                if(listeFuuidsVisites.length >= BATCH_PRESENCE_NOMBRE_MAX) {
                    debug("Emettre batch fuuids reconnus")
                    this.emettreBatchFuuidsVisites(listeFuuidsVisites)
                        .catch(err=>console.warn(new Date() + " storeConsignationManager.genererListeLocale (archives loop) Erreur emission batch fuuids visite : ", err))
                    listeFuuidsVisites = []
                }
            }
            await _storeConsignationHandler.parcourirArchives(callbackTraiterFichier)
            if(listeFuuidsVisites.length > 0) {
                this.emettreBatchFuuidsVisites(listeFuuidsVisites)
                    .catch(err=>console.warn(new Date() + " storeConsignationManager.genererListeLocale (archives) Erreur emission batch fuuids visite : ", err))
            }
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
        const messageFormatte = await _mq.pki.formatterMessage(
            MESSAGE_KINDS.KIND_COMMANDE, info,  
            {domaine: 'fichiers', action: 'liste', ajouterCertificat: true}
        )
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
            await _mq.emettreEvenement(messageFormatte, {domaine: 'fichiers', action: 'syncPret', ajouterCertificat: true})
        }

        debug("genererListeLocale Fin")
    }

    async emettreBatchFuuidsVisites(listeFuuidsVisites) {
        const message = {
            fuuids: listeFuuidsVisites
        }
        const domaine = 'fichiers', action = 'visiterFuuids'
        await _mq.emettreEvenement(message, {domaine, action})
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
