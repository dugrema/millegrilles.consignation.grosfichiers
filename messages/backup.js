const debug = require('debug')('millegrilles:messages:backup')
const fs = require('fs')
const path = require('path')
const tar = require('tar')
const lzma = require('lzma-native')

// const { spawn } = require('child_process');
const { TraitementFichier, PathConsignation, supprimerRepertoiresVides, supprimerFichiers} = require('../util/traitementFichier');
const { TraitementFichierBackup } = require('../util/traitementBackup')
const { RestaurateurBackup } = require('../util/restaurationBackup')
const { calculerHachageFichier, calculerHachageData } = require('../util/utilitairesHachage')

class GestionnaireMessagesBackup {

  constructor(mq) {
    this.mq = mq;
    this.pki = mq.pki;
    this.genererBackupQuotidien.bind(this);
    this.traitementFichier = new TraitementFichier(mq);
    this.traitementFichierBackup = new TraitementFichierBackup(mq);
    this.pathConsignation = this.traitementFichier.pathConsignation;
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message, opts) => {
        // Retourner la promise pour rendre cette operation bloquante (longue duree)
        return this.genererBackupQuotidien(routingKey, message, opts)
      },
      ['commande.backup.genererBackupQuotidien'],
      {operationLongue: true}
    )

    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message, opts) => {
        // Retourner la promise pour rendre cette operation bloquante (longue duree)
        return this.genererBackupAnnuel(routingKey, message, opts)
      },
      ['commande.backup.genererBackupAnnuel'],
      {operationLongue: true}
    );

    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message, opts) => {
        return this.prerarerStagingRestauration(routingKey, message, opts)
      },
      ['commande.backup.preparerStagingRestauration'],
      {operationLongue: true}
    )

    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message, opts) => {
        return this.restaurerGrosFichiers(routingKey, message, opts)
      },
      ['commande.backup.restaurerGrosFichiers'],
      {operationLongue: true}
    )

  }

  async genererBackupQuotidien(routingKey, message, opts) {
    debug("Generer backup quotidien : %O", message);

    try {
      const catalogue = message.catalogue
      const informationArchive = await genererBackupQuotidien(
        this.mq, this.traitementFichierBackup, this.pathConsignation, catalogue)
      const { fichiersInclure, pathRepertoireBackup } = informationArchive
      delete informationArchive.fichiersInclure // Pas necessaire pour la transaction
      delete informationArchive.pathRepertoireBackup // Pas necessaire pour la transaction

      // Finaliser le backup en retransmettant le journal comme transaction
      // de backup quotidien. Met le flag du document quotidien a false
      debug("Transmettre journal backup quotidien comme transaction de backup quotidien")
      await this.mq.transmettreEnveloppeTransaction(catalogue)

      // Generer transaction pour journal mensuel. Inclue SHA512 et nom de l'archive quotidienne
      debug("Transmettre transaction informationArchive :\n%O", informationArchive)
      await this.mq.transmettreTransactionFormattee(informationArchive, 'Backup.archiveQuotidienneInfo')

      // Effacer les fichiers transferes dans l'archive quotidienne
      await supprimerFichiers(fichiersInclure, pathRepertoireBackup)
      await supprimerRepertoiresVides(this.pathConsignation.consignationPathBackupHoraire)

    } catch (err) {
      console.error("genererBackupQuotidien: Erreur creation backup quotidien:\n%O", err)
    }

  }

  genererBackupAnnuel(routingKey, message, opts) {
    debug("Generer backup annuel : %O", message);

    return new Promise( async (resolve, reject) => {

      try {
        const informationArchive = await genererBackupAnnuel(
          this.mq, this.traitementFichierBackup, this.pathConsignation, message.catalogue)

        debug("Journal annuel sauvegarde : %O", informationArchive)

        // Finaliser le backup en retransmettant le journal comme transaction
        // de backup quotidien
        await this.mq.transmettreEnveloppeTransaction(message.catalogue)

        // Generer transaction pour journal annuel. Inclue SHA512 et nom de l'archive mensuelle
        const {fichiersInclure} = informationArchive

        delete informationArchive.fichiersInclure
        delete informationArchive.pathRepertoireBackup

        debug("Transmettre transaction avec information \n%O", informationArchive)
        await this.mq.transmettreTransactionFormattee(informationArchive, 'Backup.archiveAnnuelleInfo')

        const domaine = message.catalogue.domaine
        const pathArchivesQuotidiennes = this.pathConsignation.trouverPathDomaineQuotidien(domaine) // path.join(this.pathConsignation.consignationPathBackupArchives, 'quotidiennes', domaine)

        await supprimerFichiers(fichiersInclure, pathArchivesQuotidiennes)
        await supprimerRepertoiresVides(path.join(this.pathConsignation.consignationPathBackupArchives, 'quotidiennes'))

      } catch (err) {
        console.error("Erreur creation backup annuel")
        console.error(err)
      }

      resolve()
    })

  }

  // Generer le repertoire staging, extrait et verifie tous les fichiers
  // de catalogues, transactions et autres (e.g. grosfichiers)
  // Retourne un rapport avec les erreurs
  async prerarerStagingRestauration(routingKey, message, opts) {
    debug("Preparer staging restauration")

    const {correlationId, replyTo} = opts.properties

    const restaurateur = new RestaurateurBackup(this.mq)
    const rapportRestauration = await restaurateur.restaurationComplete()
    debug("rapportRestauration : %O", rapportRestauration)

    // debug("rapportHardLinks : %O", rapportRestauration.rapportHardLinks)
    // debug("rapportTarExtraction : %O", rapportRestauration.rapports.rapportTarExtraction)
    // debug("rapportVerificationHoraire.erreurs : %O", rapportRestauration.rapports.rapportVerificationHoraire.erreurs)
    //
    // // Transmettre reponse
    // this.mq.transmettreReponse(rapportRestauration, replyTo, correlationId)

    debug("Staging restauration complete")
  }

  async restaurerGrosFichiers(routingKey, message, opts) {
    debug("Restaurer les grosfichiers a partir du repertoire backup")
    const {correlationId, replyTo} = opts.properties

    // Transmettre la reponse immediatement pour indiquer le debut du traitement
    var reponse = {'action': 'debut_restauration', 'domaine': 'fichiers'}
    this.mq.transmettreReponse(reponse, replyTo, correlationId)

    try {
      const restaurateur = new RestaurateurBackup(this.mq)

      // Restaurer les fichiers deja extraits sous horaire
      this.mq.emettreEvenement({action: 'debut_restauration', niveau: 'horaire', domaine: 'fichiers'}, 'evenement.backup.restaurationFichiers')
      await restaurateur.restaurerGrosFichiersHoraire()

      // Restaurer les fichiers dans les archives quotidiennes
      this.mq.emettreEvenement({action: 'restauration_en_cours', niveau: 'quotidien', domaine: 'fichiers'}, 'evenement.backup.restaurationFichiers')
      await restaurateur.restaurerGrosFichiersQuotidien()

      // Restaurer les fichiers dans les archives annuelles
      this.mq.emettreEvenement({action: 'restauration_en_cours', niveau: 'annuel', domaine: 'fichiers'}, 'evenement.backup.restaurationFichiers')
      await restaurateur.restaurerGrosFichiersAnnuel()

      this.mq.emettreEvenement({action: 'fin_restauration', domaine: 'fichiers'}, 'evenement.backup.restaurationFichiers')

    } catch(err) {
      console.error("restaurerGrosFichiers: Erreur\n%O", err)
      const messageErreur = {'err': ''+err, 'domaine': 'fichiers'}
      this.mq.emettreEvenement(messageErreur, 'evenement.backup.restaurationFichiers')
    }

  }

}

// Genere un fichier de backup quotidien qui correspond au journal
async function genererBackupQuotidien(mq, traitementFichierBackup, pathConsignation, journal) {
  debug("genererBackupQuotidien : journal \n%O", journal)

  const {domaine, securite} = journal
  const jourBackup = new Date(journal.jour * 1000)
  var repertoireBackup = pathConsignation.trouverPathBackupQuotidien(jourBackup)

  // Faire liste des fichiers de catalogue et transactions a inclure.
  // var fichiersInclure = [nomJournal]
  var fichiersInclure = []

  for(let heureStr in journal.fichiers_horaire) {
    let infoFichier = journal.fichiers_horaire[heureStr]
    debug("Preparer backup heure %s :\n%O", heureStr, infoFichier)

    if(heureStr.length == 1) heureStr = '0' + heureStr; // Ajouter 0 devant heure < 10

    let fichierCatalogue = path.join(heureStr, infoFichier.catalogue_nomfichier);
    let fichierTransactions = path.join(heureStr, infoFichier.transactions_nomfichier);

    // Verifier SHA512
    const pathFichierCatalogue = path.join(repertoireBackup, fichierCatalogue)
    const sha512Catalogue = await calculerHachageFichier(pathFichierCatalogue)
    if( ! infoFichier.catalogue_hachage ) {
      delete journal['_signature']  // Signale qu'on doit regenerer entete et signature du catalogue (dirty)

      console.warn("genererBackupQuotidien: Hachage manquant pour catalogue horaire %s, on cree l'entree au vol", fichierCatalogue)
      // Il manque de l'information pour l'archive quotidienne, on insere les valeurs maintenant
      await new Promise((resolve, reject)=>{
        fs.readFile(pathFichierCatalogue, (err, data)=>{
          if(err) return reject(err)
          try {
            var compressor = lzma.LZMA().decompress(data, (data, err)=>{
              if(err) return reject(err)
              const catalogueDict = JSON.parse(data)
              infoFichier.catalogue_hachage = sha512Catalogue
              infoFichier.hachage_entete = calculerHachageData(catalogueDict['en-tete']['hachage_contenu'])
              infoFichier['uuid-transaction'] = catalogueDict['en-tete']['uuid-transaction']
              debug("genererBackupQuotidien: Hachage calcule : %O", infoFichier)
              resolve()
            })
          } catch(err) {
            reject(err)
          }
        })
      })
    } else if(sha512Catalogue != infoFichier.catalogue_hachage) {
      throw `Fichier catalogue ${fichierCatalogue} ne correspond pas au hachage`;
    }

    const sha512Transactions = await calculerHachageFichier(path.join(repertoireBackup, fichierTransactions));
    if(sha512Transactions !== infoFichier.transactions_hachage) {
      throw `Fichier transaction ${fichierCatalogue} ne correspond pas au hachage`;
    }

    fichiersInclure.push(fichierCatalogue);
    fichiersInclure.push(fichierTransactions);
  }

  // Faire liste des grosfichiers au besoin
  if(journal.fuuid_grosfichiers) {
    // Aussi inclure le repertoire des grosfichiers
    // fichiersInclureStr = `${fichiersInclureStr} */grosfichiers/*`

    for(let fuuid in journal.fuuid_grosfichiers) {
      let infoFichier = journal.fuuid_grosfichiers[fuuid]
      let heureStr = infoFichier.heure
      if(heureStr.length == 1) heureStr = '0' + heureStr

      let extension = infoFichier.extension
      if(infoFichier.securite == '3.protege' || infoFichier.securite == '4.secure') {
        extension = 'mgs1'
      }
      let nomFichier = path.join(heureStr, 'grosfichiers', `${fuuid}.${extension}`)

      debug("Ajout grosfichier %s", nomFichier)

      // Verifier le hachage des fichiers a inclure
      if(infoFichier.hachage) {
        const fonctionHash = infoFichier.hachage.split(':')[0]
        const hachageCalcule = await calculerHachageFichier(
          path.join(repertoireBackup, nomFichier), {fonctionHash});

        if(hachageCalcule !== infoFichier.hachage) {
          debug("Erreur verification hachage grosfichier\nCatalogue : %s\nCalcule : %s", infoFichier.hachage, hachageCalcule)
          throw `Erreur Hachage sur fichier : ${nomFichier}`
        } else {
          debug("Hachage grosfichier OK : %s => %s ", hachageCalcule, nomFichier)
        }
      } else {
        throw `Erreur Hachage absent sur fichier : ${nomFichier}`;
      }

      fichiersInclure.push(nomFichier);
    }
  }

  // Sauvegarder journal quotidien, sauvegarder en format .json.xz
  if( ! journal['_signature'] ) {
    debug("Regenerer signature du catalogue horaire, entete precedente : %O", journal['en-tete'])
    // Journal est dirty, on doit le re-signer
    const domaine = journal['en-tete'].domaine
    delete journal['en-tete']
    mq.formatterTransaction(domaine, journal)
  }

  var resultat = await traitementFichierBackup.sauvegarderJournalQuotidien(journal)
  debug("Resultat sauvegarder journal quotidien : %O", resultat)
  const pathJournal = resultat.path
  const nomJournal = path.basename(pathJournal)
  // const pathRepertoireBackup = path.dirname(pathJournal)
  fichiersInclure.unshift(nomJournal)  // Inserer comme premier fichier dans le .tar, permet traitement stream

  const pathArchive = pathConsignation.consignationPathBackupArchives
  // Creer nom du fichier d'archive - se baser sur le nom du catalogue quotidien
  const pathArchiveQuotidienneRepertoire = path.join(pathArchive, 'quotidiennes', domaine)
  debug("Path repertoire archive quotidienne : %s", pathArchiveQuotidienneRepertoire)
  await new Promise((resolve, reject)=>{
    fs.mkdir(pathArchiveQuotidienneRepertoire, { recursive: true, mode: 0o770 }, err=>{
      if(err) return reject(err)
      resolve()
    })
  })

  var nomArchive = [domaine, resultat.dateFormattee, securite + '.tar'].join('_')
  const pathArchiveQuotidienne = path.join(pathArchiveQuotidienneRepertoire, nomArchive)
  debug("Path archive quotidienne : %s", pathArchiveQuotidienne)

  var fichiersInclureStr = fichiersInclure.join('\n');
  debug(`Fichiers quotidien inclure relatif a ${pathArchive} : \n${fichiersInclure}`);

  // Creer nouvelle archive quotidienne
  await tar.c(
    {
      file: pathArchiveQuotidienne,
      cwd: repertoireBackup,
    },
    fichiersInclure
  )

  // Calculer le SHA512 du fichier d'archive
  const hachageArchive = await calculerHachageFichier(pathArchiveQuotidienne)

  const informationArchive = {
    archive_hachage: hachageArchive,
    archive_nomfichier: nomArchive,
    jour: journal.jour,
    domaine: journal.domaine,
    securite: journal.securite,

    fichiersInclure,
    pathRepertoireBackup: repertoireBackup,
  }

  return informationArchive

}

// Genere un fichier de backup mensuel qui correspond au journal
async function genererBackupAnnuel(mq, traitementFichierBackup, pathConsignation, journal) {

  const {domaine, securite} = journal
  const anneeBackup = new Date(journal.annee * 1000)

  const pathRepertoireQuotidien = pathConsignation.trouverPathDomaineQuotidien(domaine)

  const fichiersInclure = []

  // Trier la liste des jours pour ajout dans le fichier .tar
  const listeJoursTriee = Object.keys(journal.fichiers_quotidien)
  listeJoursTriee.sort()
  debug("Liste jours tries : %O", listeJoursTriee)

  for(const idx in listeJoursTriee) {
    const jourStr = listeJoursTriee[idx]
    debug("Traitement jour %s", jourStr)
    let infoFichier = journal.fichiers_quotidien[jourStr]
    debug("Verifier fichier backup quotidien %s :\n%O", jourStr, infoFichier)

    let fichierArchive = infoFichier.archive_nomfichier

    // Verifier SHA512
    const pathFichierArchive = path.join(pathRepertoireQuotidien, fichierArchive)
    const hachageArchive = await calculerHachageFichier(pathFichierArchive)
    if(hachageArchive != infoFichier.archive_hachage) {
      throw new Error(`Fichier archive ${fichierArchive} ne correspond pas au hachage`)
    }

    fichiersInclure.push(fichierArchive)
  }

  // Sauvegarder journal annuel, sauvegarder en format .json.xz
  var resultat = await traitementFichierBackup.sauvegarderJournalAnnuel(journal)
  debug("Resultat preparation catalogue annuel : %O", resultat)
  const pathJournal = resultat.path
  const nomJournal = path.basename(pathJournal)
  fichiersInclure.unshift(nomJournal)  // Ajouter le journal comme premiere entree du .tar

  const pathRepertoireAnnuel = pathConsignation.trouverPathDomaineAnnuel(domaine)

  debug("Path repertoire archives \nannuelles : %s", pathRepertoireAnnuel)
  await new Promise((resolve, reject)=>{
    fs.mkdir(pathRepertoireAnnuel, { recursive: true, mode: 0o770 }, err=>{
      if(err) return reject(err);
      resolve();
    })
  })

  // Creer nom du fichier d'archive - se baser sur le nom du catalogue quotidien
  var nomArchive = [domaine, resultat.dateFormattee, securite + '.tar'].join('_')
  const pathArchiveAnnuelle = path.join(pathRepertoireAnnuel, nomArchive)
  debug("Path archive annuelle : %s", pathArchiveAnnuelle)

  debug('Archive annuelle inclure : %O', fichiersInclure)

  // Creer nouvelle archive quotidienne
  await tar.c(
    {
      file: pathArchiveAnnuelle,
      cwd: pathRepertoireQuotidien,
    },
    fichiersInclure
  )

  // Calculer le hachage du fichier d'archive
  const hachageArchive = await calculerHachageFichier(pathArchiveAnnuelle)

  const informationArchive = {
    archive_hachage: hachageArchive,
    archive_nomfichier: nomArchive,
    annee: journal.annee,
    domaine: journal.domaine,
    securite: journal.securite,

    fichiersInclure,
    pathRepertoireBackup: pathRepertoireQuotidien,
  }

  return informationArchive

}

module.exports = {GestionnaireMessagesBackup};
