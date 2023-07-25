const fs = require('fs')
const fsPromises = require('fs/promises')

const fileutils = require('./fileutils')

class SynchronisationConsignation {

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

        return { nombreFichiers, tailleFichiers }
    }

    async parcourirFichiers(callback, opts) {
        throw new Error('must override')
    }

    async genererOutputListing(fichierPath, opts) {
        opts = opts || {}
        const fichierWriteStream = fs.createWriteStream(fichierPath + '.work')
        
        const resultat = await this.genererListing({...opts, outputStream: fichierWriteStream})
        
        fichierWriteStream.close()
        await fileutils.sortFile(fichierPath + '.work', fichierPath, opts)  // Trier
        await fsPromises.unlink(fichierPath + '.work')

        return resultat
    }

}

class SynchronisationConsignationFuuids extends SynchronisationConsignation {

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

class SynchronisationBackup extends SynchronisationConsignation {

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

module.exports = { SynchronisationConsignationFuuids, SynchronisationBackup }
