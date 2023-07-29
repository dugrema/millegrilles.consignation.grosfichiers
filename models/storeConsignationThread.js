const debug = require('debug')('consignation:store:thread')
const path = require('path')
const fsPromises = require('fs/promises')
const readdirp = require('readdirp')

const INTERVALLE_DEMARRER_THREAD = 300_000,
      PATH_STAGING_READY = 'ready',
      FICHIER_ETAT = 'etat.json',
      FICHIER_TRANSACTION_CONTENU = 'transactionContenu.json'

const CODE_HACHAGE_MISMATCH = 1,
      CODE_CLES_SIGNATURE_INVALIDE = 2,
      CODE_TRANSACTION_SIGNATURE_INVALIDE = 3

class StoreConsignationThread {
    
    constructor(mq, manager) {
        this.mq = mq
        this.manager = manager

        this.queueFuuids = []
        this.timerThread = true
    }

    demarrer() {
        if(this.timerThread) {
            this._threadConsignerFichiers()
                .catch(err=>console.error("Erreur run _threadPutFichiersConsignation: %O", err))
        }
    }

    ajouterFichierConsignation(fuuid) {
        if( ! this.queueFuuids.includes(fuuid) ) {
            debug("ajouterFichierConsignation Fichier %s", fuuid)
            this.queueFuuids.push(fuuid)
            if(this.timerThread) {
                this._threadConsignerFichiers()
                    .catch(err=>console.error("Erreur run _threadPutFichiersConsignation: %O", err))
            }
        }
    }

    async consigner(pathReady, fuuid) {
        debug("transfererFichierVersConsignation Fichier %s/%s", pathReady, fuuid)
        const transactions = await this.traiterTransactions(pathReady, fuuid)
        debug("transfererFichierVersConsignation Info ", transactions)
        // const {etat, transaction: transactionGrosFichiers, cles: commandeMaitreCles} = transactions
        const {etat} = transactions
        const transactionGrosFichiers = transactions.transaction
        const attachements = (transactionGrosFichiers?transactionGrosFichiers.attachements:null) || {}
        const commandeMaitreCles = attachements.cle

        // Conserver cle
        if(commandeMaitreCles) {
            // Transmettre la cle
            debug("Transmettre commande cle pour le fichier: %O", commandeMaitreCles)
            try {
                await this.mq.transmettreEnveloppeCommande(commandeMaitreCles)
            } catch(err) {
                console.error("%O ERROR Erreur sauvegarde cle fichier %s : %O", new Date(), fuuid, err)
                return
            }
        }

        // Conserver le fichier
        const pathFichierStaging = path.join(pathReady, fuuid)
        try {
            await this.manager.consignerFichier(pathFichierStaging, fuuid)
        } catch(err) {
            if(err.code === 'ENOENT') {
                // Le fichier a deja ete consigne, mais l'etat/transaction encore present. On poursuit
            } else {
                console.error("%O ERROR Erreur consignation fichier : %O", new Date(), err)
                return
            }
        }

        // Conserver transaction contenu (grosfichiers)
        // Note : en cas d'echec, on laisse le fichier en place. Il sera mis dans la corbeille automatiquement au besoin.
        if(transactionGrosFichiers) {
            debug("Transmettre commande fichier nouvelleVersion : %O", transactionGrosFichiers)
            try {
                const reponseGrosfichiers = await this.mq.transmettreEnveloppeCommande(transactionGrosFichiers, {exchange: '2.prive'})
                debug("Reponse message grosFichiers : %O", reponseGrosfichiers)
            } catch(err) {
                console.error("%O ERROR Erreur sauvegarde fichier (commande) %s : %O", new Date(), fuuid, err)
                return
            }
        }

        fsPromises.rm(pathFichierStaging, {recursive: true})
            .catch(err=>console.error("Erreur suppression repertoire %s apres consignation reussie : %O", fuuid, err))

        // Emettre un evenement de consignation, peut etre utilise par domaines connexes (e.g. messagerie)
        try {
            const domaine = 'fichiers', action = 'consigne'
            const contenu = { 'hachage_bytes': etat.hachage }
            this.mq.emettreEvenement(contenu, {domaine, action, exchange: '2.prive'})
                .catch(err=>{
                    console.error("%O ERROR Erreur Emission evenement nouveau fichier %s : %O", new Date(), fuuid, err)
                })
        } catch(err) {
            console.error("%O ERROR Erreur Emission evenement nouveau fichier %s : %O", new Date(), fuuid, err)
        }

        // Le fichier a ete transfere avec succes (aucune exception)
        if(this.manager.estPrimaire() === true) {
            // Emettre un message
            await this.evenementFichierPrimaire(fuuid)
        } else {
            this.manager.ajouterUploadPrimaire(fuuid)
        }

    }

    async _threadConsignerFichiers() {

        try {
            debug(new Date() + " Run _threadConsignerFichiers")

            // Clear timer si present
            if(this.timerThread) clearTimeout(this.timerThread)
            this.timerThread = null
    
            const pathReady = path.join(this.manager.getPathStaging(), PATH_STAGING_READY)
    
            debug("_threadPutFichiersConsignation On parcours le repertoire %s", pathReady)
            // Traiter le contenu du repertoire
            const promiseReaddirp = readdirp(pathReady, {
                type: 'directories',
                depth: 0,
            })

            for await (const entry of promiseReaddirp) {
                const fuuid = entry.basename
                debug("Ajouter repertoire existant fuuid ", fuuid)
                this.ajouterFichierConsignation(fuuid)
            }
    
            // Process les items recus durant le traitement
            debug("_threadPutFichiersConsignation Queue avec %d items", this.queueFuuids.length, pathReady)
            while(this.queueFuuids.length > 0) {
                const fuuid = this.queueFuuids.shift()  // FIFO
                debug("Traiter consigner pour fuuid %s", fuuid)
                await this.consigner(pathReady, fuuid)
            }
    
        } catch(err) {
            console.error(new Date() + ' _threadConsignerFichiers Erreur execution cycle : %O', err)
        } finally {
            debug("_threadConsignerFichiers Fin execution cycle, attente %s ms", INTERVALLE_DEMARRER_THREAD)
            // Redemarrer apres intervalle
            this.timerThread = setTimeout(()=>{
                this.timerThread = null
                this._threadConsignerFichiers()
                    .catch(err=>console.error("Erreur run _threadConsignerFichiers: %O", err))
            }, INTERVALLE_DEMARRER_THREAD)
        }
    }

    async evenementFichierPrimaire(fuuid) {
        // Emettre evenement aux secondaires pour indiquer qu'un nouveau fichier est pret
        debug("Evenement consignation primaire sur", fuuid)
        const evenement = {fuuid}
        try {
            this.mq.emettreEvenement(
                evenement, {domaine: 'fichiers', action: 'consignationPrimaire', exchange: '2.prive', attacherCertificat: true})
        } catch(err) {
            console.error(new Date() + " uploadFichier.evenementFichierPrimaire Erreur ", err)
        }
    }
    
    async traiterTransactions(pathReady, fuuid) {
        let transactionContenu = null, transactionCles = null
        const pki = this.mq.pki
    
        const transactions = {}
    
        try {
            const pathEtat = path.join(pathReady, fuuid, FICHIER_ETAT)
            const etat = JSON.parse(await fsPromises.readFile(pathEtat))
            transactions.etat = etat
        } catch(err) {
            console.warn("ERROR durant chargement de l'etat de consignation de %s : %O", fuuid, err)
        }
    
        try {
            const pathReadyItemTransaction = path.join(pathReady, fuuid, FICHIER_TRANSACTION_CONTENU)
            transactionContenu = JSON.parse(await fsPromises.readFile(pathReadyItemTransaction))
        } catch(err) {
            // Pas de transaction de contenu
        }
    
        if(transactionCles) {
            try { 
                await await pki.verifierMessage(transactionCles) 
                transactions.cles = transactionCles
            } 
            catch(err) {
                err.code = CODE_CLES_SIGNATURE_INVALIDE
                throw err
            }
        }
        
        if(transactionContenu) {
            try { 
                await pki.verifierMessage(transactionContenu) 
                transactions.transaction = transactionContenu
            } 
            catch(err) {
                err.code = CODE_TRANSACTION_SIGNATURE_INVALIDE
                throw err
            }
        }
    
        return transactions
    }
}

module.exports = StoreConsignationThread
