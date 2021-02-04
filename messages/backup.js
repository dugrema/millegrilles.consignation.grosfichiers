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
const { genererBackupQuotidien, genererBackupAnnuel } = require('../util/processFichiersBackup')
const { genererRapportVerification } = require('../util/verificationBackups')

class GestionnaireMessagesBackup {

  constructor(mq) {
    this.mq = mq;
    this.pki = mq.pki;
    //this.genererBackupQuotidien.bind(this);
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
        // debug("ERROR : commande.backup.genererBackupQuotidien pas implemente, message %O", message)
        // Retourner la promise pour rendre cette operation bloquante (longue duree)
        return genererBackupQuotidien(this.mq, this.pathConsignation, message.catalogue)
        // throw new Error("commande.backup.genererBackupQuotidien pas implemente")
      },
      ['commande.backup.genererBackupQuotidien'],
      {operationLongue: true}
    )

    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message, opts) => {
        // Retourner la promise pour rendre cette operation bloquante (longue duree)
        throw new Error("commande.backup.genererBackupAnnuel pas implemente")
        // return this.genererBackupAnnuel(this.mq, routingKey, message, opts)
      },
      ['commande.backup.genererBackupAnnuel'],
      {operationLongue: true}
    )

    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message, opts) => {
        return genererRapportVerification(
          this.mq, this.pathConsignation, message.domaine, message.uuid_rapport,
          {...opts, verification_hachage: true, verification_enchainement: true}
        )
      },
      ['commande.backup.verifierDomaine'],
      {operationLongue: true}
    )

    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message, opts) => {
        throw new Error("commande.backup.prerarerStagingRestauration pas implemente")
        // return this.prerarerStagingRestauration(routingKey, message, opts)
      },
      ['commande.backup.preparerStagingRestauration'],
      {operationLongue: true}
    )

    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message, opts) => {
        throw new Error("commande.backup.restaurerGrosFichiers pas implemente")
        // return this.restaurerGrosFichiers(routingKey, message, opts)
      },
      ['commande.backup.restaurerGrosFichiers'],
      {operationLongue: true}
    )

    // Ajouter messages suivants :
    // - retourner la liste des domaines connus (repertoires de backup)

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

      // Restaurer les fichiers dans les archives annuelles
      this.mq.emettreEvenement({action: 'debut_restauration', niveau: 'archives', domaine: 'fichiers'}, 'evenement.backup.restaurationFichiers')
      await restaurateur.restaurerGrosFichiersArchives()

      // Restaurer les fichiers deja extraits sous horaire
      this.mq.emettreEvenement({action: 'restauration_en_cours', niveau: 'horaire', domaine: 'fichiers'}, 'evenement.backup.restaurationFichiers')
      await restaurateur.restaurerGrosFichiersHoraire()

      this.mq.emettreEvenement({action: 'fin_restauration', domaine: 'fichiers'}, 'evenement.backup.restaurationFichiers')

    } catch(err) {
      console.error("restaurerGrosFichiers: Erreur\n%O", err)
      const messageErreur = {'err': ''+err, 'domaine': 'fichiers'}
      this.mq.emettreEvenement(messageErreur, 'evenement.backup.restaurationFichiers')
    }

  }

}

module.exports = {GestionnaireMessagesBackup};
