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

    // this.mq.routingKeyManager.addRoutingKeyCallback(
    //   (routingKey, message, opts) => {
    //     // Retourner la promise pour rendre cette operation bloquante (longue duree)
    //     return this.genererBackupMensuel(routingKey, message, opts)
    //   },
    //   ['commande.backup.genererBackupMensuel'],
    //   {operationLongue: true}
    // );

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
          this.traitementFichierBackup, this.pathConsignation, message.catalogue);
        const { fichiersInclure, pathRepertoireBackup } = informationArchive;
        delete informationArchive.fichiersInclure; // Pas necessaire pour la transaction
        delete informationArchive.pathRepertoireBackup; // Pas necessaire pour la transaction

        // Finaliser le backup en retransmettant le journal comme transaction
        // de backup quotidien
        debug("Transmettre journal backup quotidien comme transaction de backup quotidien")
        await this.mq.transmettreEnveloppeTransaction(message.catalogue, 'Backup.nouvelle');

        // Generer transaction pour journal mensuel. Inclue SHA512 et nom de l'archive quotidienne
        debug("Transmettre transaction informationArchive :\n%O", informationArchive);

        await this.mq.transmettreTransactionFormattee(informationArchive, 'Backup.archiveQuotidienneInfo');

        // Effacer les fichiers transferes dans l'archive quotidienne
        await supprimerFichiers(fichiersInclure, pathRepertoireBackup);
        await supprimerRepertoiresVides(this.pathConsignation.consignationPathBackupHoraire);

      } catch (err) {
        console.error("Erreur creation backup quotidien:\n%O", err);
        return reject(err)
      }

      // console.debug("Backup quotidien termine");
      resolve();
    });

  }

  genererBackupMensuel(routingKey, message, opts) {
    return new Promise( async (resolve, reject) => {
      // console.debug("Generer backup mensuel");
      // console.debug(message);

      try {
        const informationArchive = await genererBackupMensuel(this.traitementFichierBackup, message.catalogue);

        // console.debug("Information archive mensuelle:");
        // console.debug(informationArchive);

        // Finaliser le backup en retransmettant le journal comme transaction
        // de backup quotidien
        await this.mq.transmettreEnveloppeTransaction(message.catalogue, 'Backup.nouvelle');

        // Generer transaction pour journal annuel. Inclue SHA512 et nom de l'archive mensuelle
        // console.debug("Transmettre transaction informationArchive : ");
        // console.debug(informationArchive);
        const {nomJournal, fichiersInclure} = informationArchive;

        delete informationArchive.fichiersInclure;
        delete informationArchive.nomJournal;

        await this.mq.transmettreTransactionFormattee(informationArchive, 'Backup.archiveMensuelleInfo');

        // console.debug("Info archive mensuelle transmise, nettoyage des fichiers locaux");
        await utilitaireFichiers.supprimerFichiers(fichiersInclure, this.pathConsignation.consignationPathBackupArchives);

      } catch (err) {
        console.error("Erreur creation backup mensuel");
        console.error(err);
      }

      // console.debug("Backup mensuel termine");
      resolve();
    });

  }

  // Generer le repertoire staging, extrait et verifie tous les fichiers
  // de catalogues, transactions et autres (e.g. grosfichiers)
  // Retourne un rapport avec les erreurs
  prerarerStagingRestauration(routingKey, message, opts) {
    return new Promise( async (resolve, reject) => {
      console.info("Preparer staging restauration");
      // console.debug(message);
      // console.debug(opts);

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
  // console.debug("Domaine " + domaine + ", securite " + securite);
  // console.debug(jourBackup);

  // Sauvegarder journal quotidien, sauvegarder en format .json.xz
  var resultat = await traitementFichier.sauvegarderJournalQuotidien(journal);
  debug("Resultat sauvegarder journal quotidien : %O", resultat)
  const pathJournal = resultat.path;
  const nomJournal = path.basename(pathJournal);
  const pathRepertoireBackup = path.dirname(pathJournal);

  // console.debug(`Path backup : ${pathRepertoireBackup}`);

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
  console.debug(`Fichiers quotidien inclure relatif a ${pathArchive} : \n${fichiersInclure}`);

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
async function genererBackupMensuel(traitementFichier, journal) {

  const {domaine, securite} = journal;
  const moisBackup = new Date(journal.mois * 1000);
  // console.debug("Domaine " + domaine + ", securite " + securite);
  // console.debug(moisBackup);

  // Sauvegarder journal quotidien, sauvegarder en format .json.xz
  var resultat = await traitementFichier.sauvegarderJournalMensuel(journal);
  const pathJournal = resultat.pathJournal;
  const nomJournal = path.basename(pathJournal);

  // console.debug(`Path journal mensuel : ${pathJournal}`);

  const pathArchives = traitementFichier.pathConsignation.consignationPathBackupArchives;

  // Creer nom du fichier d'archive - se baser sur le nom du catalogue quotidien
  const nomArchive = nomJournal.replace('_catalogue', '').replace('.json', '.tar');
  const pathArchiveMensuelle = path.join(pathArchives, nomArchive);

  // Faire liste des fichiers a inclure.
  let year = moisBackup.getUTCFullYear();
  let month = moisBackup.getUTCMonth() + 1; if(month < 10) month = '0'+month;
  const moisStr = '' + year + month;

  // const fichiersInclure = `${nomJournal} ${domaine}_${moisStr}??_${securite}.tar.xz`
  var fichiersInclure = [nomJournal];
  for(let jour in journal.fichiers_quotidien) {
    let infoArchive = journal.fichiers_quotidien[jour];
    let sha512_archive = infoArchive.archive_sha3_512;
    let nomFichier = infoArchive.archive_nomfichier;

    let pathFichierArchive = path.join(pathArchives, nomFichier);
    const sha512Archive = await calculerHachageFichier(pathFichierArchive);
    if(sha512Archive != sha512_archive) {
      throw `SHA3-512 archive ${nomFichier} est incorrect, backup annule`;
    }
    // console.debug(`Reducing fichier ${nomFichier}, SHA512: ${sha512Archive}`);
    // console.debug(fichiersInclure);
    fichiersInclure.push(nomFichier);
  }

  // console.debug("Array fichiersInclure");
  // console.debug(fichiersInclure);

  let fichiersInclureStr = fichiersInclure.join(' ');

  console.debug(`Archive mensuelle inclure : ${fichiersInclure}`);

  const commandeBackup = spawn('/bin/sh', ['-c', `cd ${pathArchives}; tar -Jcf ${pathArchiveMensuelle} ${fichiersInclureStr}`]);
  commandeBackup.stderr.on('data', data=>{
    console.error(`tar backup mensuel: ${data}`);
  })

  const resultatTar = await new Promise(async (resolve, reject) => {
    commandeBackup.on('close', async code =>{
      if(code != 0) {
        return reject(code);
      }

      // Calculer le SHA512 du fichier d'archive
      try {
        const sha512Archive = await calculerHachageFichier(pathArchiveMensuelle);
        const informationArchive = {
          archive_sha3_512: sha512Archive,
          archive_nomfichier: nomArchive,
          catalogue_sha3_512: resultat.sha512,
          mois: journal.mois,
          domaine: journal.domaine,
          securite: journal.securite,

          nomJournal,
          fichiersInclure,
        }

        // Supprimer les archives quotidiennes correspondants au mois, journal

        // console.debug("SHA512 archive mensuelle : " + sha512Archive);

        return resolve(informationArchive);
      } catch(err) {
        reject(err);
      }

    })
  })
  .catch(err=>{
    return({err});
  });

  if(resultatTar.err) {
    throw err;
  }

  return resultatTar;

}

module.exports = {GestionnaireMessagesBackup};
