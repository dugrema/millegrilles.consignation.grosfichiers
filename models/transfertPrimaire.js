const debug = require('debug')('transfertPrimaire')

const INTERVALLE_PUT_CONSIGNATION = 900_000,
      CONST_TAILLE_SPLIT_MAX_DEFAULT = 20 * 1024 * 1024

function TransfertPrimaire(mq, storeConsignation) {
    this.amqpdao = mq
    this.storeConsignation = storeConsignation

    this.queueItems = []
    this.timerPutFichiers = null
    this.traitementPutFichiersEnCours = false
}

TransfertPrimaire.prototype.ajouterItem = function(item) {
    this.queueItems.push(item)
    debug('ajouterItem %O, Q: %O', item, this.queueItems)

    // Todo declencher traitement
    if(this.timerPutFichiers) {
        this.threadPutFichiersConsignation()
            .catch(err=>console.error("Erreur run threadPutFichiersConsignation: %O", err))
    }
}

TransfertPrimaire.prototype.threadPutFichiersConsignation = async function() {
    try {
        debug("Run threadPutFichiersConsignation")
        if(this.timerPutFichiers) clearTimeout(this.timerPutFichiers)
        this.timerPutFichiers = null

        // Process les items de la Q
        debug("threadPutFichiersConsignation Queue avec %d items", this.queueItems.length)
        while(this.queueItems.length > 0) {
            const item = this.queueItems.shift()  // FIFO
            debug("Traiter PUT pour item %s", item)
            // await _consignerFichier(_amqpdao, pathReady, item)
        }

    } catch(err) {
        console.error(new Date() + ' TransfertPrimaire.threadPutFichiersConsignation Erreur execution cycle : %O', err)
    } finally {
        this.traitementPutFichiersEnCours = false
        debug("threadPutFichiersConsignation Fin execution cycle, attente %s ms", INTERVALLE_PUT_CONSIGNATION)
        // Redemarrer apres intervalle
        this.timerPutFichiers = setTimeout(()=>{
            this.timerPutFichiers = null
            this.threadPutFichiersConsignation()
                .catch(err=>console.error("TransfertPrimaire Erreur run threadPutFichiersConsignation: %O", err))
        }, INTERVALLE_PUT_CONSIGNATION)

    }

}

// const fichierActifsPrimaireDest = path.join(getPathDataFolder(), 'actifsPrimaire.txt')

// const httpsAgent = FichiersTransfertBackingStore.getHttpsAgent()
// const urlTransfert = new URL(FichiersTransfertBackingStore.getUrlTransfert())

module.exports = TransfertPrimaire
