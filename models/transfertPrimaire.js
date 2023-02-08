const debug = require('debug')('transfertPrimaire')
const fs = require('fs')
const path = require('path')
const axios = require('axios')

const INTERVALLE_PUT_CONSIGNATION = 900_000,
      CONST_TAILLE_SPLIT_MAX_DEFAULT = 64*1024  // 20 * 1024 * 1024

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

    // const pathReadyItem = path.join(pathReady, item)
    // debug("Traiter transfert vers consignation de %s", pathReadyItem)

    // let etat = null
    // {
    //     // Charger etat et maj dates/retry count
    //     const pathEtat = path.join(pathReadyItem, FICHIER_ETAT)
    //     etat = JSON.parse(await fsPromises.readFile(pathEtat))
    //     etat.lastProcessed = new Date().getTime()
    //     etat.retryCount++
    //     await fsPromises.writeFile(pathEtat, JSON.stringify(etat), {mode: 0o600})
    // }
    // const hachage = etat.hachage

    // // Verifier les transactions (signature)
    // const transactions = await traiterTransactions(amqpdao, pathReady, item)

    // // Verifier le fichier (hachage)
    // await verifierFichier(hachage, pathReadyItem)

    // debug("Transactions et fichier OK : %s", pathReadyItem)

    // const promiseReaddirp = readdirp(pathReadyItem, {
    //     type: 'files',
    //     fileFilter: '*.part',
    //     depth: 1,
    // })

    // const listeFichiersTriees = []
    // for await (const entry of promiseReaddirp) {
    //     // debug("Entry path : %O", entry);
    //     const fichierPart = entry.basename
    //     const position = Number(fichierPart.split('.').shift())
    //     listeFichiersTriees.push({...entry, position})
    // }

    // // Trier par position (asc)
    // listeFichiersTriees.sort((a,b)=>a.position-b.position)

    debug("Traiter PUT pour fuuid %s", fuuid)
    // if(positionUpload > position) {
    //     debug("Skip position %d (courante connue est %d)", position, positionUpload)
    //     continue
    // } else {
    //     positionUpload = position
    // }

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

    // // Faire POST pour confirmer upload, acheminer transactions
    // const urlPost = new URL(''+_urlConsignationTransfert)
    // urlPost.pathname = path.join(urlPost.pathname, item)
    // const reponsePost = await axios({
    //     method: 'POST',
    //     httpsAgent: _httpsAgent,
    //     url: urlPost.href,
    //     data: transactions,
    // })
    
}

TransfertPrimaire.prototype.putAxios = async function(fuuid, statItem) {
    const filePath = statItem.filePath,
          size = statItem.stat.size
    debug("PUT Axios %s size %d", fuuid, size)

    const correlation = this.storeConsignation.getInstanceId() + '_' + fuuid,
          httpsAgent = this.storeConsignation.getHttpsAgent(),
          urlConsignationTransfert = this.storeConsignation.getUrlTransfert()

    for (let position=0; position < size; position += CONST_TAILLE_SPLIT_MAX_DEFAULT) {
        // Creer output stream
        const start = position,
              end = Math.min(position+CONST_TAILLE_SPLIT_MAX_DEFAULT, size) - 1
        debug("PUT fuuid %s positions %d a %d", correlation, start, end)

        const urlPosition = new URL(urlConsignationTransfert.href)
        urlPosition.pathname = path.join(urlPosition.pathname, correlation, ''+position)

        const readStream = fs.createReadStream(filePath, {start, end})
        try {
            const reponsePut = await axios({
                method: 'PUT',
                httpsAgent,
                url: urlPosition.href,
                headers: {'content-type': 'application/stream'},
                data: readStream,
            })
                    
        } finally {
            readStream.close()
        }
    }
    

    // if(typeof(inputStream._read === 'function')) {
    //     // Assumer stream
    //     let compteur = 0
    //     const promise = new Promise((resolve, reject)=>{
    //         inputStream.on('data', async chunk => {
    //             inputStream.pause()
                
    //             if(chunk.length + compteur > TAILLE_SPLIT) {
    //                 debug("stagingStream split %s", pathFichierPut)
    //                 const bytesComplete = TAILLE_SPLIT - compteur
    //                 await writer.write(chunk.slice(0, bytesComplete))  // Ecrire partie du chunk
    //                 await writer.close()  // Fermer le fichier

    //                 pathFichierPut = await getPathRecevoir(pathStaging, correlation, position + bytesComplete)
    //                 debug("stagingStream Ouvrir nouveau fichier %s", pathFichierPut)
    //                 writer = await fsPromises.open(pathFichierPut, 'w')

    //                 position += chunk.length       // Aller a la fin du chunk
    //                 compteur = chunk.length - bytesComplete   // Reset compteur
    //                 await writer.write(chunk.slice(bytesComplete))  // Continuer a la suite dans le chunk
    //             } else {
    //                 debug("stagingStream chunk %d dans %s", chunk.length, pathFichierPut)
    //                 compteur += chunk.length
    //                 position += chunk.length
    //                 await writer.write(chunk)
    //             }
                
    //             inputStream.resume()
    //         })
    //         inputStream.on('end', async () => { 
    //             await writer.close()
    //             resolve()
    //         })
    //         inputStream.on('error', err=>{ reject(err) })
    //     })
    //     //inputStream.pipe(writer)
    //     inputStream.read()  // Lancer la lecture
        
    //     return promise
    // } else {
    //     throw new Error("Type input non supporte")
    // }    
}

// const fichierActifsPrimaireDest = path.join(getPathDataFolder(), 'actifsPrimaire.txt')

// const httpsAgent = FichiersTransfertBackingStore.getHttpsAgent()
// const urlTransfert = new URL(FichiersTransfertBackingStore.getUrlTransfert())

module.exports = TransfertPrimaire
