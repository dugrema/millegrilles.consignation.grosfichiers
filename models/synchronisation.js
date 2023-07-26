const debug = require('debug')('sync:synchronisation')
const path = require('path')
const fsPromises = require('fs/promises')

const SynchronisationPrimaire = require('./syncPrimaire')
const SynchronisationSecondaire = require('./syncSecondaire')

const INTERVALLE_DEMARRER_THREAD = 300_000,
      BATCH_PRESENCE_NOMBRE_MAX = 1_000

class SynchronisationManager {

    constructor(mq, consignationManager, opts) {
        opts = opts || {}

        if(!mq) throw new Error("mq null")
        this.mq = mq
        if(!consignationManager) throw new Error("consignationManager null")
        this.manager = consignationManager

        this._path_listings = path.join(this.manager.getPathStaging(), 'liste')

        // Information sur consignation primaire
        this.instanceId = mq.pki.cert.subject.getField('CN').value
        this.urlConsignationTransfert = null
        this.instanceIdPrimaire = null
        this.estPrimaire = null

        // Threads
        this.timerThread = false  // null => actif, true => init en cours, int => en attente timeout, false => arrete
        this.timerRecevoirFuuidsReclames = null
        this.syncPrimaireHandler = new SynchronisationPrimaire(mq, consignationManager)
        this.syncSecondaireHandler = new SynchronisationSecondaire(mq, consignationManager)
        this.syncHandler = null  // Prend la valeur synPrimaireHandler ou syncSecondaireHandler

        // Parametres optionnels
        this.intervalleSync = opts.intervalleSync || INTERVALLE_DEMARRER_THREAD

        debug("SynchronisationManager path staging %s, intervalle sync %d", this.manager.getPathStaging(), this.intervalleSync)
        
        this.init()
    }

    /** Set nombre de millsecs entre sync automatiques */
    setIntervalleSync(intervalleSync) {
        this.intervalleSync = intervalleSync
    }

    init() {
        const pathLogsReclamations = path.join(this._path_listings, 'reclamations')
        fsPromises.mkdir(pathLogsReclamations, {recursive: true})
            .catch(err=>console.error(new Date() + " Erreur creation path %s : %O", pathLogsReclamations, err))
    
        // const repertoireDownloadSync = path.join(this.manager.getPathDataFolder(), 'syncDownload')
        // await fsPromises.mkdir(repertoireDownloadSync, {recursive: true})
        setTimeout(()=>{
            this.reloadUrlTransfert()
                .catch(err=>{
                    console.warn("Erreur initialisation SynchronisationManager, ressayer plus tard : ", err)
                    setTimeout(()=>{
                        this.reloadUrlTransfert()
                            .catch(err=>{
                                console.error("Erreur initialisation SynchronisationManager, sync inactive : ", err)
                            })
                    }, 25_000)
                })
        }, 5_000)
    }

    async reloadUrlTransfert(opts) {
        opts = opts || {}
        const requete = { primaire: true }
    
        if (!this.mq) throw new Error("mq absent")
    
        const reponse = await this.mq.transmettreRequete(
            'CoreTopologie',
            requete,
            { action: 'getConsignationFichiers', exchange: '2.prive', attacherCertificat: true }
        )
    
        if (!reponse.ok) {
            debug("Erreur reception reponse url transfert : ", reponse)
            throw new Error("Erreur configuration URL transfert (reponse MQ): ok = false")
        }
    
        const { instance_id, consignation_url } = reponse

        let consignationUrl = new URL('https://fichiers:443')  // Default
        try {
            consignationUrl = new URL(consignation_url)
        } catch(err) {
            if(instance_id === this.instanceId) {
                // On est le primaire, OK
                console.info(new Date() + " transfertPrimaire.reloadUrlTransfert Erreur chargement consignation url, utilisation default %s : %O", consignationUrl, err)
            } else {
                throw err  // On est secondaire, il faut recuperer un URL de consignation primaire
            }
        }
        consignationUrl.pathname = '/fichiers_transfert'
    
        debug("Consignation URL : %s sur instance_id : %s", consignationUrl.href, instance_id)
        this.urlConsignationTransfert = consignationUrl
        this.instanceIdPrimaire = instance_id
        this.setModePrimaire(instance_id === this.instanceId)
    
        return { url: consignationUrl.href, instance_id }
    }

    /** Demarrer la thread d'entretien. */
    demarrer() {
        if(this.timerThread === false) this.timerThread = true
        this.manager.setEstConsignationPrimaire(this.estPrimaire)
            .then(()=>this._thread())
            .catch(err=>console.error("SynhronisationManager.demarrer Erreur : %O", err))
    }

    /** Arreter la thread d'entretien. */
    arreter() {
        debug("Arreter SynchronisationManager")
        if(this.timerThread) clearTimeout(this.timerThread)
        this.timerThread = false
        if(this.syncHandler) {
            this.syncHandler.arreter()
            this.syncHandler = null
        }
    }

    /** Change le mode d'operation a consignation primaire */
    demarrerModePrimaire() {
        debug("Demarrage/switch sync en mode primaire")
        if(this.syncHandler) this.syncHandler.arreter()
        this.estPrimaire = true
        this.syncHandler = this.syncPrimaireHandler
        this.demarrer()
    }

    /** Change le mode d'operation a consignation secondaire */
    demarrerModeSecondaire() {
        debug("Demarrage/switch sync en mode secondaire")
        if(this.syncHandler) this.syncHandler.arreter()
        this.estPrimaire = false
        this.syncHandler = this.syncSecondaireHandler
        this.demarrer()
    }

    /**
     * Methode qui permet de switch de mode d'operation
     * @param {*} estPrimaire 
     */
    setModePrimaire(estPrimaire) {
        if(estPrimaire && this.syncHandler !== this.syncPrimaireHandler) {
            this.demarrerModePrimaire()
        } else if(!estPrimaire && this.syncHandler !== this.syncSecondaireHandler) {
            this.demarrerModeSecondaire()
        }
    }

    async _thread() {

        if(!this.timerThread) {
            debug("SynhronisationManager._thread - deja en cours, SKIP")
            return
        }

        try {
            debug(new Date() + " Run SynhronisationManager._thread")

            // Clear timer si present
            if(this.timerThread) clearTimeout(this.timerThread)
            this.timerThread = null
    
            if(this.syncHandler) {
                // Rafraichir configuration consignation
                try {
                    await this.reloadUrlTransfert()
                } catch(err) {
                    console.warn(new Date() + " SynhronisationManager._thread Erreur rafraichissement consignation (tenter de continuer) : ", err)
                }

                // Executer la synchronisation
                await this.syncHandler.runSync()
            } else {
                console.info("SynhronisationManager SyncHandler null, skip execution")
            }
    
        } catch(err) {
            console.error(new Date() + ' SynhronisationManager._thread Erreur execution cycle : %O', err)
        } finally {
            if(this.timerThread !== false) {
                // Redemarrer apres intervalle
                debug("SynhronisationManager._thread Fin execution cycle, attente %s ms", this.intervalleSync)
                this.timerThread = setTimeout(()=>{
                    this.timerThread = true
                    this._thread()
                        .catch(err=>console.error("SynhronisationManager Erreur run _thread: %O", err))
                }, this.intervalleSync)
            } else {
                debug("SynhronisationManager._thread Fin execution cycle et arret _thread")
            }
        }
    }

    /**
     * Methode passthrough pour messages de consignation primaire.
     * @param {*} fuuids 
     * @param {*} opts 
     * @returns 
     */
    async recevoirFuuidsReclames(fuuids, opts) {
        if(!this.estPrimaire) throw new Error("SynchronisationManager recevoirFuuidsReclames Consignation secondaire/null, fuuids ignores")
        return this.syncPrimaireHandler.recevoirFuuidsReclames(fuuids, opts)
    }
}


module.exports = SynchronisationManager
