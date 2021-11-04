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
const { genererBackupQuotidien } = require('../util/processFichiersBackup')
const { genererRapportVerification } = require('../util/verificationBackups')

const EXPIRATION_MESSAGE_DEFAUT = 15 * 60 * 1000  // 15 minutes en millisec

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
        // Verifier si la commande est expiree
        if(this.mq.estExpire(message, {expiration: EXPIRATION_MESSAGE_DEFAUT})) {
          console.warn("WARN backup.genererBackupQuotidien Commande expiree, on l'ignore : %O", message)
          return
        }
        return genererBackupQuotidien(this.mq, this.pathConsignation, message.catalogue, message.uuid_rapport)
      },
      ['commande.backup.genererBackupQuotidien'],
      {qCustom: 'backup'}
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
