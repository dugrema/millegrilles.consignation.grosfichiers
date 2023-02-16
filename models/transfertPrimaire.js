const debug = require('debug')('transfertPrimaire')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const axios = require('axios')

const INTERVALLE_PUT_CONSIGNATION = 900_000,
      CONST_TAILLE_SPLIT_MAX_DEFAULT = 5 * 1024 * 1024

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
            await this.putFichier(item)
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
                .catch(err=>console.error(new Date() + " TransfertPrimaire Erreur run threadPutFichiersConsignation: %O", err))
        }, INTERVALLE_PUT_CONSIGNATION)

    }

}

TransfertPrimaire.prototype.putFichier = async function(fuuid) {
    const statItem = await this.storeConsignation.getInfoFichier(fuuid)
    debug("PUT fichier ", statItem)

    if(!statItem) {
        console.error(new Date() + " transfertPrimaire.putFichier Fuuid %s n'existe pas localement, upload annule", fuuid)
        return
    }

    debug("Traiter PUT pour fuuid %s", fuuid)

    try {
        await this.putAxios(fuuid, statItem)
    } catch(err) {
        const response = err.response || {}
        const status = response.status
        console.error(new Date() + " Erreur PUT fichier (status %d) %O", status, err)
        if(status === 409) {
            positionUpload = response.headers['x-position'] || position
        } else {
            throw err
        }
    }
    
}

TransfertPrimaire.prototype.putAxios = async function(fuuid, statItem) {
    debug("PUT Axios %s info %O", fuuid, statItem)
    const filePath = statItem.filePath
    const statContent = statItem.stat || {}
    const size = statContent.size
    const fileRedirect = statItem.fileRedirect

    // debug("PUT Axios %s size %d", fuuid, size)

    const correlation = this.storeConsignation.getInstanceId() + '_' + fuuid,
          httpsAgent = this.storeConsignation.getHttpsAgent(),
          urlConsignationTransfert = this.storeConsignation.getUrlTransfert()

    const urlPrimaireFuuid = new URL(urlConsignationTransfert.href)
    urlPrimaireFuuid.pathname = path.join(urlPrimaireFuuid.pathname, fuuid)

    // S'assurer que le fichier n'existe pas deja
    try {
        await axios({method: 'HEAD', url: urlPrimaireFuuid.href, httpsAgent})
        console.error(new Date() + "transfertPrimaire.putAxios Fichier %s existe deja sur le primaire, annuler transfert", fuuid)
        return false
    } catch(err) {
        const response = err.response
        if(response && response.status === 404) {
            // OK, le fuuid n'existe pas deja
        } else {
            debug("Erreur axios : ", err)
            throw err
        }
    }

    let srcFilePath = null,
        tmpFile = null

    try {
        if(fileRedirect) {
            debug("putAxios Source transfert via %s", fileRedirect)
            tmpFile = path.join('/tmp/', fuuid + '.primaire')
            const sourceReponse = await axios({method: 'GET', url: fileRedirect, responseType: 'stream' })
            if(sourceReponse.status !== 200) {
                throw new Error("Mauvaise reponse source (code %d) %s", sourceReponse.status, fileRedirect)
            }
            const writeStream = fs.createWriteStream(tmpFile)
            await new Promise((resolve, reject)=>{
                writeStream.on('close', resolve)
                writeStream.on('error', reject)
                sourceReponse.data.pipe(writeStream)
            })
            srcFilePath = tmpFile
        } else {
            srcFilePath = filePath
        }

        debug("putAxios Source transfert %s", srcFilePath)
        for (let position=0; position < size; position += CONST_TAILLE_SPLIT_MAX_DEFAULT) {
            // Creer output stream
            const start = position,
                end = Math.min(position+CONST_TAILLE_SPLIT_MAX_DEFAULT, size) - 1
            debug("PUT fuuid %s positions %d a %d", correlation, start, end)
    
            const urlPosition = new URL(urlConsignationTransfert.href)
            urlPosition.pathname = path.join(urlPosition.pathname, correlation, ''+position)
    
            const readStream = fs.createReadStream(srcFilePath, {start, end})
            try {
                await axios({
                    method: 'PUT',
                    httpsAgent,
                    url: urlPosition.href,
                    headers: {'content-type': 'application/stream', 'X-fuuid': fuuid},
                    data: readStream,
                })
            } finally {
                readStream.close()
            }
        }        

    } finally {
        if(tmpFile) {
            debug("putAxios Cleanup ", tmpFile)
            await fsPromises.unlink(tmpFile)
        }
    }
        
    // Emettre message POST pour indiquer que le fichier est complete
    const transactions = { etat: { hachage: fuuid } }
    const urlCorrelation = new URL(urlConsignationTransfert.href)
    urlCorrelation.pathname = path.join(urlCorrelation.pathname, correlation)
    await axios({
        method: 'POST',
        httpsAgent,
        url: urlCorrelation.href,
        data: transactions,
    })
}

module.exports = TransfertPrimaire
