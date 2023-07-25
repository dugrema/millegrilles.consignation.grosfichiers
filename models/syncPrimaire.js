const debug = require('debug')('sync:syncPrimaire')
const path = require('path')

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
        this.timerRecevoirFuuidsReclames = null
    }

    /** Demande aux domaines de fournir une liste de fuuids a conserver. 
     *  Permet de generer la liste d'orphelins. */
    async recupererListeFuuids() {

    }

    /** Reception de listes de fuuids a partir de chaque domaine. */
    async recevoirFuuidsReclames(fuuids, opts) {
        opts = opts || {}
        const archive = opts.archive || false
        debug("recevoirFuuidsReclames %d fuuids (archive %s)", fuuids.length, archive)

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

        if(this.timerRecevoirFuuidsReclames) clearTimeout(this.timerRecevoirFuuidsReclames)
        debug("recevoirFuuidsReclames dans %d secs", DUREE_ATTENTE_RECLAMATIONS / 1000)
        this.timerRecevoirFuuidsReclames = setTimeout(()=>{
            traiterFichiersConfirmes()
                .catch(err=>console.error("recevoirFuuidsReclames ERREUR ", err))
        }, DUREE_ATTENTE_RECLAMATIONS)
    }

    async runSync() {
        throw new Error('todo')
    }

    arreter() {
        throw new Error('todo')
    }

}

module.exports = SynchronisationPrimaire