const debug = require('debug')('millegrilles:messages:backup')
const fs = require('fs')
const path = require('path')
const tar = require('tar')
const S3 = require('aws-sdk/clients/s3')
// const { spawn } = require('child_process');
const { TraitementFichier, PathConsignation, supprimerRepertoiresVides, supprimerFichiers} = require('../util/traitementFichier');
const { TraitementFichierBackup } = require('../util/traitementBackup')
const { RestaurateurBackup } = require('../util/traitementBackup')
const { calculerHachageFichier } = require('../util/utilitairesHachage')

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
    );

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
    );

  }

  genererBackupQuotidien(routingKey, message, opts) {
    return new Promise( async (resolve, reject) => {
      // console.debug("Generer backup quotidien");
      // console.debug(message);

      try {
        const informationArchive = await genererBackupQuotidien(
          this.traitementFichierBackup, this.pathConsignation, message.catalogue)
        const { fichiersInclure, pathRepertoireBackup } = informationArchive
        delete informationArchive.fichiersInclure // Pas necessaire pour la transaction
        delete informationArchive.pathRepertoireBackup // Pas necessaire pour la transaction

        // Finaliser le backup en retransmettant le journal comme transaction
        // de backup quotidien
        debug("Transmettre journal backup quotidien comme transaction de backup quotidien")
        await this.mq.transmettreEnveloppeTransaction(message.catalogue, 'Backup.nouvelle')

        // Generer transaction pour journal mensuel. Inclue SHA512 et nom de l'archive quotidienne
        debug("Transmettre transaction informationArchive :\n%O", informationArchive)

        await this.mq.transmettreTransactionFormattee(informationArchive, 'Backup.archiveQuotidienneInfo')

        // Effacer les fichiers transferes dans l'archive quotidienne
        await supprimerFichiers(fichiersInclure, pathRepertoireBackup)
        await supprimerRepertoiresVides(this.pathConsignation.consignationPathBackupHoraire)

      } catch (err) {
        console.error("Erreur creation backup quotidien:\n%O", err)
        return reject(err)
      }

      // console.debug("Backup quotidien termine");
      resolve();
    });

  }

  genererBackupAnnuel(routingKey, message, opts) {
    return new Promise( async (resolve, reject) => {

      try {
        const informationArchive = await genererBackupAnnuel(
          this.traitementFichierBackup, this.pathConsignation, message.catalogue)

        debug("Journal annuel sauvegarde : %O", informationArchive)

        // Finaliser le backup en retransmettant le journal comme transaction
        // de backup quotidien
        await this.mq.transmettreEnveloppeTransaction(message.catalogue, 'Backup.nouvelle')

        // Generer transaction pour journal annuel. Inclue SHA512 et nom de l'archive mensuelle
        const {nomJournal, fichiersInclure} = informationArchive

        delete informationArchive.fichiersInclure
        delete informationArchive.nomJournal

        await this.mq.transmettreTransactionFormattee(informationArchive, 'Backup.archiveAnnuelleInfo')

        const domaine = message.catalogue.domaine
        const pathArchivesQuotidiennes = path.join(this.pathConsignation.consignationPathBackupArchives, 'quotidiennes', domaine)

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
  prerarerStagingRestauration(routingKey, message, opts) {
    return new Promise( async (resolve, reject) => {
      console.info("Preparer staging restauration")

      const {correlationId, replyTo} = opts.properties;

      try {
        const restaurateur = new RestaurateurBackup(this.mq, this.pki);
        const rapportRestauration = await restaurateur.restaurationComplete();

        console.debug("Rapport restauration");
        console.debug(rapportRestauration);
        console.debug("rapportHardLinks :");
        console.debug(rapportRestauration.rapports.rapportHardLinks);
        console.debug("rapportTarExtraction :");
        console.debug(rapportRestauration.rapports.rapportTarExtraction);
        console.debug("rapportVerificationHoraire.erreurs :");
        console.debug(rapportRestauration.rapports.rapportVerificationHoraire.erreurs);

        // Transmettre reponse
        this.mq.transmettreReponse(rapportRestauration, replyTo, correlationId);

      } catch (err) {
        console.error("Erreur preparation staging restauration");
        console.error(err);
        reject(err);
      }

      console.info("Staging restauration complete");
      resolve();
    });

  }

}

// Genere un fichier de backup quotidien qui correspond au journal
async function genererBackupQuotidien(traitementFichier, pathConsignation, journal) {
  debug("genererBackupQuotidien : journal \n%O", journal)

  const {domaine, securite} = journal;
  const jourBackup = new Date(journal.jour * 1000);

  // Sauvegarder journal quotidien, sauvegarder en format .json.xz
  var resultat = await traitementFichier.sauvegarderJournalQuotidien(journal);
  debug("Resultat sauvegarder journal quotidien : %O", resultat)
  const pathJournal = resultat.path;
  const nomJournal = path.basename(pathJournal);
  const pathRepertoireBackup = path.dirname(pathJournal);

  const pathArchive = pathConsignation.consignationPathBackupArchives
  // Creer nom du fichier d'archive - se baser sur le nom du catalogue quotidien
  const pathArchiveQuotidienneRepertoire = path.join(pathArchive, 'quotidiennes', domaine)
  debug("Path repertoire archive quotidienne : %s", pathArchiveQuotidienneRepertoire)
  await new Promise((resolve, reject)=>{
    fs.mkdir(pathArchiveQuotidienneRepertoire, { recursive: true, mode: 0o770 }, err=>{
      if(err) return reject(err);
      resolve();
    })
  })

  var nomArchive = [domaine, resultat.dateFormattee, securite + '.tar'].join('_')
  const pathArchiveQuotidienne = path.join(pathArchiveQuotidienneRepertoire, nomArchive)
  debug("Path archive quotidienne : %s", pathArchiveQuotidienne)

  // Faire liste des fichiers de catalogue et transactions a inclure.
  var fichiersInclure = [nomJournal];

  for(let heureStr in journal.fichiers_horaire) {
    let infoFichier = journal.fichiers_horaire[heureStr]
    debug("Preparer backup heure %s :\n%O", heureStr, infoFichier)

    if(heureStr.length == 1) heureStr = '0' + heureStr; // Ajouter 0 devant heure < 10

    let fichierCatalogue = path.join(heureStr, infoFichier.catalogue_nomfichier);
    let fichierTransactions = path.join(heureStr, infoFichier.transactions_nomfichier);

    // Verifier SHA512
    const sha512Catalogue = await calculerHachageFichier(path.join(pathRepertoireBackup, fichierCatalogue));
    if(sha512Catalogue != infoFichier.catalogue_hachage) {
      throw `Fichier catalogue ${fichierCatalogue} ne correspond pas au hachage`;
    }

    const sha512Transactions = await calculerHachageFichier(path.join(pathRepertoireBackup, fichierTransactions));
    if(sha512Transactions != infoFichier.transactions_hachage) {
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
      let infoFichier = journal.fuuid_grosfichiers[fuuid];
      let heureStr = infoFichier.heure;
      if(heureStr.length == 1) heureStr = '0' + heureStr;

      let extension = infoFichier.extension;
      if(infoFichier.securite == '3.protege' || infoFichier.securite == '4.secure') {
        extension = 'mgs1';
      }
      let nomFichier = path.join(heureStr, 'grosfichiers', `${fuuid}.${extension}`);

      // Verifier le SHA si disponible
      if(infoFichier.sha256) {
        const sha256Calcule = await utilitaireFichiers.calculerSHAFichier(
          path.join(pathRepertoireBackup, nomFichier), {fonctionHash: 'sha256'});

        if(sha256Calcule != infoFichier.sha256) {
          throw `Erreur SHA256 sur fichier : ${nomFichier}`
        }
      } else if(infoFichier.sha512) {
        const sha512Calcule = await utilitaireFichiers.calculerSHAFichier(
          path.join(pathRepertoireBackup, nomFichier));

        if(sha512Calcule != infoFichier.sha512) {
          throw `Erreur SHA512 sur fichier : ${nomFichier}`;
        }
      }

      fichiersInclure.push(nomFichier);
    }
  }

  var fichiersInclureStr = fichiersInclure.join('\n');
  debug(`Fichiers quotidien inclure relatif a ${pathArchive} : \n${fichiersInclure}`);

  // Creer nouvelle archive quotidienne
  await tar.c(
    {
      file: pathArchiveQuotidienne,
      cwd: pathRepertoireBackup,
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
    pathRepertoireBackup,
  }

  return informationArchive

}

// Genere un fichier de backup mensuel qui correspond au journal
async function genererBackupAnnuel(traitementFichier, pathConsignation, journal) {

  const {domaine, securite} = journal
  const anneeBackup = new Date(journal.annee * 1000)

  // Sauvegarder journal annuel, sauvegarder en format .json.xz
  var resultat = await traitementFichier.sauvegarderJournalAnnuel(journal)
  debug("Resultat preparation catalogue annuel : %O", resultat)
  const pathJournal = resultat.pathJournal
  const nomJournal = path.basename(pathJournal)

  const pathArchive = pathConsignation.consignationPathBackupArchives

  // Creer nom du fichier d'archive - se baser sur le nom du catalogue quotidien
  const pathRepertoireQuotidien = path.join(pathArchive, 'quotidiennes', domaine)
  const pathArchiveAnnuelleRepertoire = path.join(pathArchive, 'annuelles', domaine)
  debug("Path repertoire archives \nquotidiennes : %s\nannuelle : %s", pathRepertoireQuotidien, pathArchiveAnnuelleRepertoire)
  await new Promise((resolve, reject)=>{
    fs.mkdir(pathArchiveAnnuelleRepertoire, { recursive: true, mode: 0o770 }, err=>{
      if(err) return reject(err);
      resolve();
    })
  })

  var nomArchive = [domaine, resultat.dateFormattee, securite + '.tar'].join('_')
  const pathArchiveAnnuelle = path.join(pathArchiveAnnuelleRepertoire, nomArchive)
  debug("Path archive annuelle : %s", pathArchiveAnnuelle)

  var fichiersInclure = [nomJournal]
  for(let jour in journal.fichiers_quotidien) {
    const infoArchive = journal.fichiers_quotidien[jour]

    const hachageConnu = infoArchive.archive_hachage
    const nomFichier = infoArchive.archive_nomfichier

    const pathFichierArchive = path.join(pathRepertoireQuotidien, nomFichier)
    debug("Calculer hachage archive quotidienne : %s", pathFichierArchive)
    const hachageArchive = await calculerHachageFichier(pathFichierArchive)
    if(hachageArchive != hachageConnu) {
      throw `Hachage archive ${nomFichier} est incorrect, backup annule`
    }

    fichiersInclure.push(nomFichier)
  }

  let fichiersInclureStr = fichiersInclure.join(' ')
  debug(`Archive annuelle inclure : ${fichiersInclure}`)

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
