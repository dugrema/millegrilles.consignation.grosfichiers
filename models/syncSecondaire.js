const debug = require('debug')('sync:syncSecondaire')
const { SynchronisationConsignation } = require('./synchronisationConsignation')

/** Gere les fichiers, catalogues et la synchronisation avec la consignation primaire pour un serveur secondaire */
class SynchronisationSecondaire extends SynchronisationConsignation {

    constructor(mq, consignationManager) {
        super(mq, consignationManager)
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

    /** Thread upload de fichiers vers consignation primaire */
    async _threadUpload() {

    }

    /** Thread upload de fichiers de la consignation primaire */
    async _threadDownload() {

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

}

module.exports = SynchronisationSecondaire
