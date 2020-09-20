const debug = require('debug')('millegrilles:fichiers:media')
const {PathConsignation} = require('../util/traitementFichier');
const traitementMedia = require('../util/traitementMedia.js')

// const path = require('path');
// const fs = require('fs');
// const tmp = require('tmp-promise');
// const crypto = require('crypto');
// const im = require('imagemagick');
// const uuidv1 = require('uuid/v1');
// const { DecrypterFichier, decrypterCleSecrete, getDecipherPipe4fuuid } = require('./crypto.js')
// const { Decrypteur } = require('../util/cryptoUtils.js');
// const transformationImages = require('../util/transformationImages');
// const decrypteur = new Decrypteur();

// Traitement d'images pour creer des thumbnails et preview
class GenerateurMedia {

  constructor(mq) {
    this.mq = mq;
    this.idmg = this.mq.pki.idmg;

    this.pathConsignation = new PathConsignation({idmg: this.idmg});

    debug("Path RabbitMQ %s : %s", this.idmg, this.pathConsignation.consignationPath);
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      return genererPreviewImage(this.mq, this.pathConsignation, message)}, ['commande.fichiers.genererPreviewImage'], {operationLongue: true});
    // this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
    //   return this.genererPreviewVideo(routingKey, message)}, ['commande.fichiers.genererPreviewVideo'], {operationLongue: true});
    // this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
    //   return this.transcoderVideoDecrypte(routingKey, message)}, ['commande.grosfichiers.transcoderVideo'], {operationLongue: true});
  }


}

async function genererPreviewImage(mq, pathConsignation, message) {
  debug("Commande genererPreviewImage recue : %O", message)

  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  const permission = message.permission
  var opts = {}
  if(permission) {
    // Transmettre demande cle et attendre retour sur l'autre Q (on bloque Q operations longues)
    debug("Recu permission de dechiffrage, on transmet vers le maitre des cles")

    // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
    const chainePem = mq.pki.getChainePems()
    permission['_certificat_tiers'] = chainePem
    debug("Nouvelle requete permission a transmettre : %O", permission)
    const domaineAction = permission['en-tete'].domaine
    const reponseCle = await mq.transmettreRequete(domaineAction, permission, {noformat: true})
    debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)

    // Dechiffrer cle recue
    const cleChiffree = reponseCle.cle
    const cleDechiffree = await mq.pki.decrypterAsymetrique(cleChiffree)

    // Demander cles publiques pour chiffrer preview
    const domaineActionClesPubliques = 'MaitreDesCles.certMaitreDesCles'
    const reponseClesPubliques = await mq.transmettreRequete(domaineActionClesPubliques, {})
    const clesPubliques = [reponseClesPubliques.certificat, [reponseClesPubliques.certificat_millegrille]]

    opts = {cleSymmetrique: cleDechiffree, iv: reponseCle.iv, clesPubliques}

  } else {
    debug("Fichier non chiffre, on traite immediatement")
  }

  debug("Debut generation preview image")
  const resultatPreview = await traitementMedia.genererPreviewImage(mq, pathConsignation, message, opts)
  debug("Fin traitement preview image, resultat : %O", resultatPreview)

  if(permission) {
    // Transmettre transaction info chiffrage
    const domaineActionCles = 'MaitreDesCles.cleGrosFichier'
    const transactionCles = {
      domaine: 'GrosFichiers',
      identificateurs_document: { fuuid: resultatPreview.fuuid },
      cles: resultatPreview.clesChiffrees,
      iv: resultatPreview.iv,
      sujet: 'cles.grosFichiers',
    }
    mq.transmettreTransactionFormattee(transactionCles, domaineActionCles)
  }

  // Transmettre transaction preview
  const domaineActionAssocierPreview = 'GrosFichiers.associerPreview'
  const transactionAssocierPreview = {
    uuid: message.uuid,
    fuuid: message.fuuid,
    mimetype_preview: resultatPreview.mimetype,
    fuuid_preview: resultatPreview.fuuid,
    extension_preview: resultatPreview.extension,
  }
  mq.transmettreTransactionFormattee(transactionAssocierPreview, domaineActionAssocierPreview)

}

module.exports = {GenerateurMedia}
