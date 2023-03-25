// Middleware pour reception de fichiers des clients
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const readdirp = require('readdirp')

const { VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')

const debug = require('debug')('fichiers:fichiersTransfertMiddleware')

const PATH_STAGING_DEFAUT = '/var/opt/millegrilles/consignation/staging/commun',
      PATH_STAGING_UPLOAD = 'upload',
      PATH_STAGING_READY = 'ready',
      FICHIER_TRANSACTION_CLES = 'transactionCles.json',
      FICHIER_TRANSACTION_CONTENU = 'transactionContenu.json',
      FICHIER_ETAT = 'etat.json'

const CODE_HACHAGE_MISMATCH = 1,
      CODE_CLES_SIGNATURE_INVALIDE = 2,
      CODE_TRANSACTION_SIGNATURE_INVALIDE = 3

class FichiersMiddleware {

    constructor(mq, opts) {
        opts = opts || {}

        if(!mq || !mq.pki) throw new Error("Parametre mq ou mq.pki pas initialise")

        this._mq = mq
        this._pathStaging = opts.PATH_STAGING || PATH_STAGING_DEFAUT
    }

    /**
     * Recoit une partie de fichier.
     * Configurer params :position et :fuuid dans path expressjs.
     * @param {*} opts 
     * @returns 
     */
    middlewareRecevoirFichier(opts) {
        opts = opts || {}

        // Preparer directories
        const pathUpload = path.join(this._pathStaging, PATH_STAGING_UPLOAD)
        fsPromises.mkdir(pathUpload, {recursive: true, mode: 0o750})
            .catch(err=>console.error("Erreur preparer path staging upload : %O", err))

        // Retourner fonction middleware pour recevoir un fichier (part)
        return (req, res, next) => this.middlewareRecevoirFichierHandler(req, res, next, opts)
    }

    async middlewareRecevoirFichierHandler(req, res, next, opts) {
        opts = opts || {}
        debug("middlewareRecevoirFichierHandler DEBUG THIS ", this)

        const fuuid = req.params.fuuid
        const position = req.params.position || 0
        debug("middlewareRecevoirFichier PUT fuuid %s : position %d", fuuid, position)
        
        if(!fuuid) {
            debug("middlewareRecevoirFichier ERREUR fuuid manquant")
            return res.sendStatus(400)
        }

        try {
            await stagingPut(this._pathStaging, req, fuuid, position, opts)
        } catch(err) {
            console.error("middlewareRecevoirFichier Erreur PUT: %O", err)
            const response = err.response
            if(response) {
                if(response.headers) {
                    for (const name of Object.keys(response.headers)) {
                        res.setHeader(name, response.headers[name])
                    }
                }
                if(response.status) res.status(response.status)
                return res.send(response.data)
            }
            return res.sendStatus(500)
        }

        if(opts.chainOnSuccess === true) {
            // Chainage
            debug("middlewareRecevoirFichier chainage next")
            next()
        } else {
            res.send({ok: true})
        }
    }

    /**
     * Verifie un fichier et le met dans la Q de transfert interne vers consignation.
     * Verifie et conserve opts.cles et opts.transaction si fournis (optionnels).
     * Appelle next() sur succes, status 500 sur erreur.
     * @param {*} opts 
     *            - successStatus : Code de retour si succes, empeche call next()
     *            - cles : JSON de transaction de cles
     *            - transaction : JSON de transaction de contenu
     *            - writeStream : Conserve le fichier reassemble
     *            - clean(err) : Nettoyage (err : si erreur)
     */
    middlewareReadyFichier(opts) {
        opts = opts || {}

        // // Preparer directories
        // const pathStaging = this._pathStaging
        // const pathReadyItem = path.join(pathStaging, PATH_STAGING_READY)
        // fsPromises.mkdir(pathReadyItem, {recursive: true, mode: 0o750})
        //     .catch(err=>console.error("Erreur preparer path staging ready : %O", err))
        
        return (req, res, next) => this.middlewareReadyFichierHandler(req, res, next, opts)
    }

    async middlewareReadyFichierHandler(req, res, next, opts) {
        opts = opts || {}

        const { fuuid } = req.params
        const informationFichier = req.body || {}
        debug("middlewareReadyFichier Traitement post %s upload %O", fuuid, informationFichier)
    
        const commandeMaitreCles = informationFichier.cles
        const transactionContenu = informationFichier.transaction
        
        const optsReady = {...opts, cles: commandeMaitreCles, transaction: transactionContenu}

        try {
            
            await readyStaging(this._mq, this._pathStaging, fuuid, optsReady)

            if(opts.clean) await opts.clean()

            res.hachage = fuuid
            res.transaction = transactionContenu
            res.pathFichier = path.join(this._pathStaging, PATH_STAGING_UPLOAD, fuuid)
            return next()

        } catch(err) {
            console.error("middlewareReadyFichier Erreur traitement fichier %s : %O", fuuid, err)
            
            // Tenter cleanup
            try { 
                const pathFuuid = path.join(this._pathStaging, PATH_STAGING_UPLOAD, fuuid)
                await fsPromises.rm(pathFuuid, {recursive: true})
                if(opts.clean) await opts.clean(err) 
            } 
            catch(err) { console.error("middlewareReadyFichier Erreur clean %s : %O", err) }

            switch(err.code) {
                case CODE_HACHAGE_MISMATCH:
                    return res.status(500).send({ok: false, err: 'HACHAGE MISMATCH', code: err.code})
                case CODE_CLES_SIGNATURE_INVALIDE:
                    return res.status(500).send({ok: false, err: 'CLES SIGNATURE INVALIDE', code: err.code})
                case CODE_TRANSACTION_SIGNATURE_INVALIDE:
                    return res.status(500).send({ok: false, err: 'TRANSACTION SIGNATURE INVALIDE', code: err.code})
            }

            res.status(500).send({ok: false, err: ''+err})
        }
    }

    /**
     * Supprime le repertoire de staging (upload et/ou ready)
     * @param {*} opts 
     * @returns 
     */
    middlewareDeleteStaging(opts) {
        opts = opts || {}
        return (req, res, next) => this.middlewareDeleteStagingHandler(req, res, next, opts)        
    }

    async middlewareDeleteStagingHandler(req, res, next, opts) {
        opts = opts || {}

        const { fuuid } = req.params

        if(!fuuid) return res.status(400).send({ok: false, err: 'fuuid manquant'})

        try {
            await deleteFichierStaging(this._pathStaging, fuuid)
            return res.sendStatus(200)
        } catch(err) {
            console.error("middlewareReadyFichier Erreur traitement fichier %s : %O", fuuid, err)
            return res.sendStatus(500)
        }
    }

    getPathFichier(fuuid) {
        return path.join(this._pathStaging, PATH_STAGING_UPLOAD, fuuid)
    }

}

async function getPathRecevoir(pathStaging, fuuid, position) {
    const pathUpload = path.join(pathStaging, PATH_STAGING_UPLOAD, fuuid)
    const pathUploadItem = path.join(pathUpload, '' + position + '.part')

    debug("getPathRecevoir mkdir path %s", pathUpload)
    await fsPromises.mkdir(pathUpload, {recursive: true, mode: 0o700})

    return pathUploadItem
}

async function majFichierEtatUpload(pathStaging, fuuid, data) {
    const pathFichierEtat = path.join(pathStaging, PATH_STAGING_UPLOAD, fuuid, FICHIER_ETAT)
    
    const contenuFichierStatusString = await fsPromises.readFile(pathFichierEtat)
    const contenuCourant = JSON.parse(contenuFichierStatusString)
    Object.assign(contenuCourant, data)
    await fsPromises.writeFile(pathFichierEtat, JSON.stringify(contenuCourant))

    return contenuCourant
}

/**
 * Conserver une partie de fichier provenant d'un inputStream (e.g. req)
 * @param {*} inputStream 
 * @param {*} fuuid 
 * @param {*} position 
 * @param {*} opts 
 * @returns 
 */
async function stagingPut(pathStaging, inputStream, fuuid, position, opts) {
    opts = opts || {}
    if(typeof(position) === 'string') position = Number.parseInt(position)

    // Verifier si le repertoire existe, le creer au besoin
    const pathFichierPut = await getPathRecevoir(pathStaging, fuuid, position)
    debug("PUT fichier %s", pathFichierPut)

    const contenuStatus = await getFicherEtatUpload(pathStaging, fuuid)
    debug("stagingPut Status upload courant : ", contenuStatus)

    if(contenuStatus.position != position && position !== 0) {
        debug("stagingPut Detecte resume fichier avec mauvaise position, on repond avec position courante")
        const err = new Error("stagingPut Detecte resume fichier, on repond avec position courante")
        err.response = {
            status: 409,
            headers: {'x-position': contenuStatus.position},
            json: {position: contenuStatus.position}
        }
        throw err
    } else if(position === 0) {
        debug("stagingPut Reset upload %s a 0", fuuid)
        await fsPromises.rm(path.join(pathStaging, fuuid, '*.work'), {force: true})
        await fsPromises.rm(path.join(pathStaging, fuuid, '*.part.work'), {force: true})
        contenuStatus.position = 0
    }

    // Creer output stream
    const pathFichierPutWork = pathFichierPut + '.work'
    const writer = fs.createWriteStream(pathFichierPutWork)
    debug("stagingPut Conserver fichier work upload ", pathFichierPutWork)

    if(ArrayBuffer.isView(inputStream)) {
        // Traiter buffer directement
        writer.write(inputStream)

        const nouvellePosition = inputStream.length + contenuStatus.position
        await majFichierEtatUpload(pathStaging, fuuid, {position: nouvellePosition})
        await fsPromises.rename(pathFichierPutWork, pathFichierPut)

    } else if(typeof(inputStream._read === 'function')) {
        // Assumer stream
        let compteurTaille = 0
        const promise = new Promise((resolve, reject)=>{
            inputStream.on('data', chunk=>{ 
                compteurTaille += chunk.length
                return chunk
            })
            inputStream.on('end', ()=>{ 
                // Resultat OK
                const nouvellePosition = compteurTaille + contenuStatus.position
                majFichierEtatUpload(pathStaging, fuuid, {position: nouvellePosition})
                    .then(()=>{
                        debug("stagingPut Rename fichier work vers ", pathFichierPut)
                        return fsPromises.rename(pathFichierPutWork, pathFichierPut)
                    })
                    .then(()=>resolve())
                    .catch(err=>reject(err))
            })
            inputStream.on('error', err=>{ 
                fsPromises.unlink(pathFichierPut).catch(err=>{
                    console.error("Erreur delete part incomplet %s : %O", pathFichierPut, err)
                })
                reject(err)
            })
        })
        inputStream.pipe(writer)
        
        return promise
    } else {
        throw new Error("Type inputstream non supporte")
    }
}

/**
 * Verifie le contenu de l'upload, des transactions (opts) et transfere le repertoire sous /ready
 * @param {*} pathStaging 
 * @param {*} item 
 * @param {*} hachage 
 * @param {*} opts 
 */
async function readyStaging(amqpdao, pathStaging, fuuid, opts) {
    opts = opts || {}
    debug("readyStaging item %s", fuuid)
    const pki = amqpdao.pki
    const pathUploadItem = path.join(pathStaging, PATH_STAGING_UPLOAD, fuuid)

    if(opts.cles) {
        // On a une commande de maitre des cles. Va etre acheminee et geree par le serveur de consignation.
        let contenu = opts.cles
        // contenu.corrompre = true
        try { await validerMessage(pki, contenu) } 
        catch(err) {
            debug("readyStaging ERROR readyStaging Message cles invalide")
            err.code = CODE_CLES_SIGNATURE_INVALIDE
            throw err
        }

        // Sauvegarder la transaction de cles
        const pathCles = path.join(pathUploadItem, FICHIER_TRANSACTION_CLES)
        if(typeof(contenu) !== 'string') contenu = JSON.stringify(contenu)
        await fsPromises.writeFile(pathCles, contenu, {mode: 0o600})
    }

    if(opts.transaction) {
        // On a une commande de transaction. Va etre acheminee et geree par le serveur de consignation.
        let contenu = opts.transaction
        // contenu.corrompre = true
        try { await validerMessage(pki, contenu) } 
        catch(err) {
            debug("readyStaging ERROR readyStaging Message transaction invalide")
            err.code = CODE_TRANSACTION_SIGNATURE_INVALIDE
            throw err
        }

        // Sauvegarder la transaction de contenu
        const pathContenu = path.join(pathUploadItem, FICHIER_TRANSACTION_CONTENU)
        if(typeof(contenu) !== 'string') contenu = JSON.stringify(contenu)
        await fsPromises.writeFile(pathContenu, contenu, {mode: 0o600})
    }

    try {
        const pathOutput = path.join(pathUploadItem, ''+fuuid)
        const writeStream = fs.createWriteStream(pathOutput)
        await verifierFichier(fuuid, pathUploadItem, {...opts, writeStream, deleteParts: true})
    } catch(err) {
        debug("readyStaging ERROR Fichier hachage mismatch")
        err.code = CODE_HACHAGE_MISMATCH
        throw err
    }

    // Conserver information d'etat/work
    const etat = {
        hachage: fuuid,
        created: new Date().getTime(),
        lastProcessed: new Date().getTime(),
        retryCount: 0,
    }
    const pathEtat = path.join(pathUploadItem, FICHIER_ETAT)
    await fsPromises.writeFile(pathEtat, JSON.stringify(etat), {mode: 0o600})

    // const pathReadyItem = path.join(pathStaging, PATH_STAGING_READY, batchId, fuuid)

    // try {
    //     // Tenter un rename de repertoire (rapide)
    //     await fsPromises.rename(pathUploadItem, pathReadyItem)
    // } catch(err) {
    //     // Echec du rename, on copie le contenu (long)
    //     console.warn("WARN : Erreur deplacement fichier, on copie : %O", err)
    //     await fsPromises.cp(pathUploadItem, pathReadyItem, {recursive: true})
    //     await fsPromises.rm(pathUploadItem, {recursive: true})
    // }

    // Fichier pret, on l'ajoute a la liste de transfert
    //ajouterFichierConsignation(item)  // Maintenant gerer via batch
}

function deleteFichierStaging(pathStaging, fuuid) {
    const pathUploadItem = path.join(pathStaging, PATH_STAGING_UPLOAD, fuuid)

    // Ok si une des deux promises reussi
    return Promise.any([
        fsPromises.rm(pathUploadItem, {recursive: true}),
    ])
}

async function getFicherEtatUpload(pathStaging, fuuid) {
    const pathFichierEtat = path.join(pathStaging, PATH_STAGING_UPLOAD, fuuid, FICHIER_ETAT)

    try {
        const contenuFichierStatusString = await fsPromises.readFile(pathFichierEtat)
        return JSON.parse(contenuFichierStatusString)
    } catch(err) {
        // Fichier n'existe pas, on le genere
        const contenu = {
            "creation": Math.floor(new Date().getTime() / 1000),
            "position": 0,
        }
        await fsPromises.writeFile(pathFichierEtat, JSON.stringify(contenu))
        return contenu
    }
}

/**
 * Verifier le hachage. Supporte opts.writeStream pour reassembler le fichier en output.
 * @param {*} hachage 
 * @param {*} pathUploadItem 
 * @param {*} opts 
 *            - writeStream : Output stream, ecrit le resultat durant verification du hachage.
 * @returns 
 */
async function verifierFichier(hachage, pathUploadItem, opts) {
    opts = opts || {}

    const verificateurHachage = new VerificateurHachage(hachage)
    const files = await readdirp.promise(pathUploadItem, {fileFilter: '*.part'})

    // Extraire noms de fichiers, cast en Number (pour trier)
    const filesNumero = files.map(file=>{
        return Number(file.path.split('.')[0])
    })

    // Trier en ordre numerique
    filesNumero.sort((a,b)=>{return a-b})

    let total = 0
    for(let idx in filesNumero) {
        const fileNumero = filesNumero[idx]
        debug("Charger fichier %s position %d", pathUploadItem, fileNumero)
        const pathFichier = path.join(pathUploadItem, fileNumero + '.part')
        const fileReader = fs.createReadStream(pathFichier)

        // verificateurHachage.update(Buffer.from([0x1]))  // Corrompre (test)

        fileReader.on('data', chunk=>{
            // Verifier hachage
            verificateurHachage.update(chunk)
            total += chunk.length

            if(opts.writeStream) {
                opts.writeStream.write(chunk)
            }
        })

        const promise = new Promise((resolve, reject)=>{
            fileReader.on('end', resolve)
            fileReader.on('error', reject)
        })

        await promise
        debug("Taille cumulative fichier %s : %d", pathUploadItem, total)

        if(opts.deleteParts ===  true) await fsPromises.unlink(pathFichier)
    }

    // Verifier hachage - lance une exception si la verification echoue
    await verificateurHachage.verify()
    // Aucune exception, hachage OK

    debug("Fichier fuuid %s OK, hachage %s", pathUploadItem, hachage)
    return true
}

module.exports = FichiersMiddleware
// {
//     middlewareRecevoirFichier, middlewareReadyFichier, middlewareDeleteStaging,
//     preparerTransfertBatch,
// }
