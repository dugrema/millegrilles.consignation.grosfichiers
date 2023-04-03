const debug = require('debug')('transfertPrimaire')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const axios = require('axios')
const tmpPromises = require('tmp-promise')
const { exec } = require('child_process')

const { VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')

const { chargerFuuidsListe, sortFile } = require('./fileutils')

const INTERVALLE_PUT_CONSIGNATION = 900_000,    // millisecs
      INTERVALLE_THREAD_TRANSFERT = 1_200_000,  // millisecs
      CONST_TAILLE_SPLIT_MAX_DEFAULT = 1024 * 1024 * 100,
      TIMEOUT_AXIOS = 30_000

const FICHIER_FUUIDS_ACTIFS = 'fuuidsActifs.txt',
      FICHIER_FUUIDS_ACTIFS_PRIMAIRE = 'fuuidsActifsPrimaire.txt',
      FICHIER_FUUIDS_NOUVEAUX_PRIMAIRE = 'fuuidsNouveauxPrimaire.txt',
      FICHIER_FUUIDS_ARCHIVES = 'fuuidsArchives.txt',
      FICHIER_FUUIDS_ARCHIVES_PRIMAIRE = 'fuuidsArchivesPrimaire.txt',
      FICHIER_FUUIDS_ARCHIVES_RECLAMEES_PRIMAIRE = 'fuuidsArchivesReclameesPrimaire.txt',
      FICHIER_FUUIDS_ORPHELINS = 'fuuidsOrphelins.txt',
      FICHIER_FUUIDS_MANQUANTS_PRIMAIRE = 'fuuidsManquantsPrimaire.txt',
      FICHIER_FUUIDS_PRIMAIRE = 'fuuidsPrimaire.txt',
      FICHIER_FUUIDS_UPLOAD_PRIMAIRE = 'fuuidsUploadPrimaire.txt',
      FICHIER_FUUIDS_DOWNLOAD_PRIMAIRE = 'fuuidsDownloadPrimaire.txt',
      FICHIER_FUUIDS_VERS_ARCHIVES = 'fuuidsVersArchives.txt',
      FICHIER_FUUIDS_ACTIFS_ARCHIVES = 'fuuidsActifsArchives.txt'

class TransfertPrimaire {

    constructor(mq, consignationManager) {
        if(!mq) throw new Error("mq null")
        if(!consignationManager) throw new Error("consignationManager null")

        this.mq = mq
        this.consignationManager = consignationManager

        // Q memoire pour upload et download (fuuid)
        this.queueUploadsFuuids = []
        this.queueDownloadsFuuids = []

        this.timerUploadFichiers = true
        this.timerDownloadFichiers = true

        // Information sur consignation primaire
        this.urlConsignationTransfert = null
        this.instanceIdPrimaire = null

        this.ready = this.init()
            .then(() => {
                this.ready = true
                return true
            })
            .catch(err=>{
                console.error(new Date() + " Erreur initialisation TransfertPrimaire : ", err)
                throw err
            })
    }

    async init() {
        const repertoireDownloadSync = path.join(this.consignationManager.getPathDataFolder(), 'syncDownload')
        await fsPromises.mkdir(repertoireDownloadSync, {recursive: true})
    
        await this.reloadUrlTransfert()
    }

    estPrimare() {
        return this.instanceIdPrimaire === this.instance_id
    }

    setInstanceIdPrimaire(instanceId) {
        if(this.instanceIdPrimaire !== instanceId) {
            this.reloadUrlTransfert()
                .catch(err => console.error(new Date() + " ERROR Reload url transfert ", err))
        }
        // this.instanceIdPrimaire = instanceId
    }

    getInstanceIdPrimaire() {
        return this.instanceIdPrimaire
    }

    getUrlTransfert() {
        return ''+this.urlConsignationTransfert
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
            throw new Error("Erreur configuration URL transfert (reponse MQ): ok = false")
        }
    
        const { instance_id, consignation_url } = reponse
    
        const consignationURL = new URL(consignation_url)
        consignationURL.pathname = '/fichiers_transfert'
    
        debug("Consignation URL : %s sur instance_id : %s", consignationURL.href, instance_id)
        this.urlConsignationTransfert = consignationURL
        this.instanceIdPrimaire = instance_id
    
        return { url: consignationURL.href, instance_id }
    }

    /** Lit tous les fichiers a uploader vers le primaire et les ajoute dans la Q */
    async uploaderFichiersVersPrimaire() {
        debug("uploaderFichiersVersPrimaire")
        const pathDataFolder = this.consignationManager.getPathDataFolder()
        const fichierUploadPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_UPLOAD_PRIMAIRE)
        await chargerFuuidsListe(fichierUploadPrimaire, fuuid=>this.ajouterUpload(fuuid))
    }
    
    /** Lit tous les fichiers a download du primaire et les ajoute dans la Q */
    async downloaderFichiersDuPrimaire() {
        debug("uploaderFichiersVersPrimaire Debut")
        const pathDataFolder = this.consignationManager.getPathDataFolder()
        const fichierDownloadPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_DOWNLOAD_PRIMAIRE)
        await chargerFuuidsListe(fichierDownloadPrimaire, fuuid=>this.ajouterDownload(fuuid))
        debug("uploaderFichiersVersPrimaire Fin")
    }

    ajouterUpload(fuuid) {
        if(this.ready !== true) throw new Error("Non disponible - Erreur init transfertPrimaire")

        if( ! this.queueUploadsFuuids.includes(fuuid) ) {
            this.queueUploadsFuuids.push(fuuid)
            debug('transfertPrimaire.ajouterUpload %O, Q: %O', fuuid, this.queueUploadsFuuids)

            if(this.timerUploadFichiers) {
                this.threadUploadFichiersConsignation()
                    .catch(err=>console.error("Erreur run threadUploadFichiersConsignation: %O", err))
            }
        }
    }

    ajouterDownload(fuuid, opts) {
        opts = opts || {}

        if(this.ready !== true) throw new Error("Non disponible - Erreur init transfertPrimaire")

        if( ! this.queueDownloadsFuuids.includes(fuuid) ) {
            this.queueDownloadsFuuids.push(fuuid)
            debug('transfertPrimaire.ajouterDownload %O, Q: %O', fuuid, this.queueDownloadsFuuids)

            if(this.timerDownloadFichiers) {
                this.threadDownloadFichiersConsignation()
                    .catch(err=>console.error("Erreur run threadDownloadFichiersConsignation: %O", err))
            }
        }
    }

    /** Vide les q de traitement. */
    clearQueues() {
        this.queueDownloadsFuuids.clear()
        this.queueUploadsFuuids.clear()
    }

    async threadUploadFichiersConsignation() {
        try {
            debug("Run threadPutFichiersConsignation")
            if(this.timerUploadFichiers) clearTimeout(this.timerUploadFichiers)
            this.timerUploadFichiers = null

            // Process les items de la Q
            debug("threadPutFichiersConsignation Queue avec %d items", this.queueUploadsFuuids.length)
            while(this.queueUploadsFuuids.length > 0) {
                const fuuid = this.queueUploadsFuuids.shift()  // FIFO
                try {
                    debug("threadUploadFichiersConsignation Traiter PUT pour item %s", fuuid)
                    await this.putFichier(fuuid)
                } catch(err) {
                    console.error(new Date() + " threadUploadFichiersConsignation Erreur upload %s vers primaire %O", fuuid, err)
                    const response = err.response || {}
                    if(response.status >= 400 && response.status <= 599) {
                        console.warn("Abandon threadDownloadFichiersConsignation sur erreur serveur, redemarrage pending")
                        intervalleRedemarrageThread = 60_000  // Reessayer dans 1 minute
                        return
                    }
                }
            }

        } catch(err) {
            console.error(new Date() + ' TransfertPrimaire.threadPutFichiersConsignation Erreur execution cycle : %O', err)
        } finally {
            debug("threadPutFichiersConsignation Fin execution cycle, attente %s ms", INTERVALLE_PUT_CONSIGNATION)
            // Redemarrer apres intervalle
            this.timerUploadFichiers = setTimeout(()=>{
                this.timerUploadFichiers = null
                this.threadUploadFichiersConsignation()
                    .catch(err=>console.error(new Date() + " TransfertPrimaire Erreur run threadPutFichiersConsignation: %O", err))
            }, INTERVALLE_PUT_CONSIGNATION)

        }

    }

    async putFichier(fuuid) {
        const statItem = await this.consignationManager.getInfoFichier(fuuid)
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

    async putAxios(fuuid, statItem) {
        debug("PUT Axios %s info %O", fuuid, statItem)
        const filePath = statItem.filePath
        const statContent = statItem.stat || {}
        const size = statContent.size
        const fileRedirect = statItem.fileRedirect

        // debug("PUT Axios %s size %d", fuuid, size)

        const correlation = fuuid,
            httpsAgent = this.consignationManager.getHttpsAgent()

        const urlPrimaireFuuid = new URL(this.urlConsignationTransfert.href)
        urlPrimaireFuuid.pathname = path.join(urlPrimaireFuuid.pathname, fuuid)

        // S'assurer que le fichier n'existe pas deja
        try {
            await axios({method: 'HEAD', url: urlPrimaireFuuid.href, httpsAgent, timeout: TIMEOUT_AXIOS})
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
                const sourceReponse = await axios({method: 'GET', url: fileRedirect, responseType: 'stream', timeout: TIMEOUT_AXIOS })
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
        
                const urlPosition = new URL(this.urlConsignationTransfert.href)
                urlPosition.pathname = path.join(urlPosition.pathname, correlation, ''+position)
        
                const readStream = fs.createReadStream(srcFilePath, {start, end})
                await axios({
                    method: 'PUT',
                    httpsAgent,
                    url: urlPosition.href,
                    headers: {'content-type': 'application/stream', 'X-fuuid': fuuid},
                    data: readStream,
                    timeout: TIMEOUT_AXIOS,
                    maxRedirects: 0,  // Eviter redirect buffer (max 10mb)
                })
            }        

        } finally {
            if(tmpFile) {
                debug("putAxios Cleanup ", tmpFile)
                await fsPromises.unlink(tmpFile)
            }
        }
            
        // Emettre message POST pour indiquer que le fichier est complete
        const transactions = { etat: { hachage: fuuid } }
        const urlCorrelation = new URL(this.urlConsignationTransfert.href)
        urlCorrelation.pathname = path.join(urlCorrelation.pathname, correlation)
        await axios({
            method: 'POST',
            httpsAgent,
            url: urlCorrelation.href,
            data: transactions,
            timeout: TIMEOUT_AXIOS,
        })
    }

    async downloadFichierListe(fichierDestination, remotePathnameFichier, opts) {
        opts = opts || {}
        const gzipsrc = opts.gzipsrc!==undefined?opts.gzipsrc:true
        const httpsAgent = this.consignationManager.getHttpsAgent()
        const urlData = new URL(this.urlConsignationTransfert.href)
        urlData.pathname = urlData.pathname + remotePathnameFichier
        debug("downloadFichierListe Download %s", urlData.href)
        const fichierTmp = await tmpPromises.file()
        try {
            const writeStream = fs.createWriteStream('', {fd: fichierTmp.fd})
            const reponseActifs = await axios({ 
                method: 'GET', 
                httpsAgent, 
                url: urlData.href, 
                responseType: 'stream',
                timeout: TIMEOUT_AXIOS,
            })
            debug("Reponse GET actifs %s", reponseActifs.status)

            await new Promise((resolve, reject)=>{
                writeStream.on('close', resolve)
                writeStream.on('error', reject)
                reponseActifs.data.pipe(writeStream)
            })

            try {
                debug("Sort fichier %s vers %s", fichierTmp.path, fichierDestination)
                await sortFile(fichierTmp.path, fichierDestination, {gzipsrc})
            } catch(err) {
                console.error("consignationManager.getDataSynchronisation Erreur renaming actifs ", err)
            }
        } catch(err) {
            const response = err.response || {}
            const status = response.status
            debug("Reponse GET status %s", status)
            if(status === 416) {
                // OK, le fichier est vide
            } else {
                throw err
            }
        } finally {
            fichierTmp.cleanup().catch(err=>console.error(new Date() + "ERROR Cleanup fichier tmp : ", err))
        }
    }

    /** Recupere les listes de fuuids a partir du primaire. Genere les liste d'uploads et downloads. */
    async getDataSynchronisation() {
        const httpsAgent = this.consignationManager.getHttpsAgent()
        const urlData = new URL(this.urlConsignationTransfert.href)
        urlData.pathname = urlData.pathname + '/data/data.json'
        debug("Download %s", urlData.href)

        const reponse = await axios({
            method: 'GET',
            httpsAgent,
            url: urlData.href,
            timeout: TIMEOUT_AXIOS,
        })
        debug("Reponse GET data.json %s :\n%O", reponse.status, reponse.data)
        
        // Charger listes
        const pathDataFolder = this.consignationManager.getPathDataFolder()
        const fuuidsActifsPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_ACTIFS_PRIMAIRE)
        const fuuidsActifsPrimaireOriginal = fuuidsActifsPrimaire + '.original'
        const fuuidsNouveauxPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_NOUVEAUX_PRIMAIRE)
        const fuuidsManquantsPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_MANQUANTS_PRIMAIRE)
        const fuuidsArchivesPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_ARCHIVES_PRIMAIRE)
        const fuuidsArchivesReclameesPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_ARCHIVES_RECLAMEES_PRIMAIRE)
        await this.downloadFichierListe(fuuidsActifsPrimaireOriginal, '/data/fuuidsActifs.txt.gz')
        await this.downloadFichierListe(fuuidsManquantsPrimaire, '/data/fuuidsManquants.txt.gz')

        try {
            await fsPromises.rm(fuuidsNouveauxPrimaire)
        } catch(err) {
            if(err.code === 'ENOENT') { }  // Ok
            else throw err
        }
        try {
            await this.downloadFichierListe(fuuidsNouveauxPrimaire, '/data/fuuidsNouveaux.txt', {gzipsrc: false})
        } catch(err) {
            const response = err.response || {}
            if(response.status === 404) { 
                await fsPromises.writeFile(fuuidsNouveauxPrimaire, '')  // Ecrire fichier vide
            } // Ok
            else throw err
        }
        try {
            await this.downloadFichierListe(fuuidsArchivesPrimaire, '/data/fuuidsArchives.txt.gz')
        } catch(err) {
            const response = err.response || {}
            if(response.status === 404) { 
                await fsPromises.writeFile(fuuidsArchivesPrimaire, '')  // Ecrire fichier vide
            } // Ok
            else throw err
        }
        try {
            await this.downloadFichierListe(fuuidsArchivesReclameesPrimaire, '/data/fuuidsReclamesArchives.courant.txt.gz')
        } catch(err) {
            const response = err.response || {}
            if(response.status === 404) { 
                await fsPromises.writeFile(fuuidsArchivesReclameesPrimaire, '')  // Ecrire fichier vide
            } // Ok
            else throw err
        }

        // Combiner actifs avec nouveaux
        await new Promise((resolve, reject)=>{
            exec(`cat ${fuuidsActifsPrimaireOriginal} ${fuuidsNouveauxPrimaire} ${fuuidsArchivesPrimaire} | sort -u -o ${fuuidsActifsPrimaire}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })

        // Combiner listes, dedupe
        const fuuidsPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_PRIMAIRE)
        await new Promise((resolve, reject)=>{
            exec(`cat ${fuuidsActifsPrimaire} ${fuuidsManquantsPrimaire} | sort -u -o ${fuuidsPrimaire}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })


        // Generer listes pour upload et download
        await this.chargerListeFichiersMissing()

        return reponse.data
    }

    async threadDownloadFichiersConsignation() {
        if(this.timerDownloadFichiers) clearTimeout(this.timerDownloadFichiers)
        this.timerDownloadFichiers = null

        debug(new Date() + ' threadDownloadFichiersConsignation Demarrer download fichiers')

        let intervalleRedemarrageThread = INTERVALLE_THREAD_TRANSFERT

        try {
            while(this.queueDownloadsFuuids.length > 0) {
                const fuuid = this.queueDownloadsFuuids.shift()
                try {
                    await this.downloadFichierDuPrimaire(fuuid)
                } catch(err) {
                    console.error(new Date() + " Erreur download %s du primaire %O", fuuid, err)
                    const response = err.response || {}
                    if(response.status === 404) {
                        console.info(new Date() + ' transfertPrimaire.threadDownloadFichiersConsignation Erreur download ', fuuid)
                        continue
                    } else if(response.status >= 400 && response.status <= 599) {
                        console.warn("Abandon threadDownloadFichiersConsignation sur erreur serveur, redemarrage pending")
                        intervalleRedemarrageThread = 60_000  // Reessayer dans 1 minute
                        return
                    }
                }
            }

        } catch(err) {
            console.error(new Date() + ' transfertPrimaire.threadDownloadFichiersConsignation Erreur ', err)
        } finally {
            this.timerDownloadFichiers = setTimeout(()=>{
                this.timerDownloadFichiers = null
                this.threadDownloadFichiersConsignation()
                    .catch(err=>{console.error(new Date() + ' transfertPrimaire.threadDownloadFichiersConsignation Erreur ', err)})
            }, intervalleRedemarrageThread)
            debug(new Date() + ' threadDownloadFichiersConsignation Fin')
        }
    }

    async downloadFichierDuPrimaire(fuuid) {

        debug("storeConsignation.downloadFichiersSync Fuuid %s manquant, debut download", fuuid)
        const urlFuuid = new URL(this.urlConsignationTransfert.href)
        urlFuuid.pathname = urlFuuid.pathname + '/' + fuuid
        debug("Download %s", urlFuuid.href)

        const pathDataFolder = this.consignationManager.getPathDataFolder()

        const dirFuuid = path.join(pathDataFolder, 'syncDownload', fuuid)
        await fsPromises.mkdir(dirFuuid, {recursive: true})
        const fuuidFichier = path.join(dirFuuid, fuuid)  // Fichier avec position initiale - 1 seul fichier

        const fichierActifsWork = path.join(pathDataFolder, path.join(FICHIER_FUUIDS_ACTIFS + '.work'))
        const writeCompletes = fs.createWriteStream(fichierActifsWork, {flags: 'a', encoding: 'utf8'})

        try {
            const verificateurHachage = new VerificateurHachage(fuuid)
            const httpsAgent = this.consignationManager.getHttpsAgent()
            const reponseActifs = await axios({ 
                method: 'GET', 
                httpsAgent, 
                url: urlFuuid.href, 
                responseType: 'stream',
                timeout: TIMEOUT_AXIOS,
            })
            debug("Reponse GET actifs %s", reponseActifs.status)
            const fuuidStream = fs.createWriteStream(fuuidFichier)
            await new Promise((resolve, reject)=>{
                reponseActifs.data.on('data', chunk=>verificateurHachage.update(chunk))
                fuuidStream.on('close', resolve)
                fuuidStream.on('error', err=>{
                    reject(err)
                    // fuuidStream.close()
                    fsPromises.unlink(fuuidFichier)
                        .catch(err=>console.warn("Erreur suppression fichier %s : %O", fuuidFichier, err))
                })
                reponseActifs.data.on('error', err=>{
                    reject(err)
                })
                reponseActifs.data.pipe(fuuidStream)
            })

            // Verifier hachage - lance une exception si la verification echoue
            await verificateurHachage.verify()
            // Aucune exception, hachage OK

            debug("Fichier %s download complete", fuuid)
            await this.consignationManager.consignerFichier(dirFuuid, fuuid)

            // Ajouter a la liste de downloads completes
            writeCompletes.write(fuuid + '\n')

        } finally {
            await fsPromises.rm(dirFuuid, {recursive: true, force: true})
        }
    }

    async chargerListeFichiersMissing() {

        const pathDataFolder = this.consignationManager.getPathDataFolder()

        const fuuidsPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_PRIMAIRE)
        const fuuidsActifsPrimaireOriginal = path.join(pathDataFolder, FICHIER_FUUIDS_ACTIFS_PRIMAIRE + '.original')
        const fuuidsManquantsPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_MANQUANTS_PRIMAIRE)
        const fuuidsActifsLocaux = path.join(pathDataFolder, FICHIER_FUUIDS_ACTIFS)
        const fuuidsOrphelins = path.join(pathDataFolder, FICHIER_FUUIDS_ORPHELINS)
        const fuuidsArchives = path.join(pathDataFolder, FICHIER_FUUIDS_ARCHIVES)
        const fuuidsArchivesReclamesPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_ARCHIVES_RECLAMEES_PRIMAIRE)
        const fuuidsUploadPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_UPLOAD_PRIMAIRE)
        const fuuidsActifsPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_ACTIFS_PRIMAIRE)
        const fuuidsDownloadPrimaire = path.join(pathDataFolder, FICHIER_FUUIDS_DOWNLOAD_PRIMAIRE)
        const fuuidsVersArchives = path.join(pathDataFolder, FICHIER_FUUIDS_VERS_ARCHIVES)
        const fichierActifsArchives = path.join(pathDataFolder, FICHIER_FUUIDS_ACTIFS_ARCHIVES)

        try {
            await fsPromises.stat(fuuidsActifsLocaux)
        } catch(err) {
            debug("chargerListeFichiersMissing Fichier fuuids manquants locaux, pas de sync")
            return
        }
    
        try {
            // Trouver les fichiers sont sur le primaire mais pas localement
            await fsPromises.stat(fichierActifsArchives)
            await new Promise((resolve, reject) => {
                exec(`comm -13 ${fichierActifsArchives} ${fuuidsActifsPrimaire} > ${fuuidsDownloadPrimaire}`, error=>{
                    if(error) return reject(error)
                    else resolve()
                })
            })
        } catch(err) {
            if(err.code === 'ENOENT') {
                debug("chargerListeFichiersMissing Fichier fuuids primaire absent, pas de sync download")            
            } else {
                throw err
            }
        }

        if(this.consignationManager.estSupporteArchives()) {
            debug("Support archives, on prepare les fichiers de catalogues")
            await fsPromises.stat(fuuidsArchives)

            // Ajouter liste d'archives manquantes de l'archive locale au download primaire
            await new Promise((resolve, reject) => {
                exec(`comm -13 ${fichierActifsArchives} ${fuuidsArchivesReclamesPrimaire} >> ${fuuidsDownloadPrimaire}`, error=>{
                    if(error) return reject(error)
                    else resolve()
                })
            })            

            // Prendre fichiers actifs qui doivent etre deplaces vers archives
            await new Promise((resolve, reject) => {
                exec(`comm -12 ${fuuidsActifsLocaux} ${fuuidsArchivesReclamesPrimaire} > ${fuuidsVersArchives}`, error=>{
                    if(error) return reject(error)
                    else resolve()
                })
            })            
        }
    
        // Trouver fichiers qui sont presents localement et manquants sur le primaire
        try {
            // Test de presence des fichiers de fuuids
            await fsPromises.stat(fuuidsActifsPrimaire)
            await new Promise((resolve, reject) => {
                exec(`comm -12 ${fichierActifsArchives} ${fuuidsManquantsPrimaire} > ${fuuidsUploadPrimaire}`, error=>{
                    if(error) return reject(error)
                    else resolve()
                })
            })
        } catch(err) {
            if(err.code === 'ENOENT') {
                debug("chargerListeFichiersMissing Fichier manquants primaire absent, pas de sync download")            
            } else {
                throw err
            }
        }

        // Trouver fichiers qui sont presents localement mais non requis par le primaire (orphelins)
        try {
            // Test de presence des fichiers de fuuids
            await fsPromises.stat(fuuidsPrimaire)
            await new Promise((resolve, reject) => {
                exec(`comm -23 ${fuuidsActifsLocaux} ${fuuidsPrimaire} > ${fuuidsOrphelins}`, error=>{
                    if(error) return reject(error)
                    else resolve()
                })
            })
        } catch(err) {
            if(err.code === 'ENOENT') {
                debug("chargerListeFichiersMissing Fichier manquants primaire absent, pas de sync download")            
            } else {
                throw err
            }
        }
        
    }    
}

module.exports = TransfertPrimaire

