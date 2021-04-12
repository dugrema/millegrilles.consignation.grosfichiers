const debug = require('debug')('millegrilles:messages:media')
const {PathConsignation} = require('../util/traitementFichier');
const traitementMedia = require('../util/traitementMedia.js')
const { traiterCommandeTranscodage } = require('../util/transformationsVideo')

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
    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return genererPreviewImage(this.mq, this.pathConsignation, message)},
      ['commande.fichiers.genererPreviewImage'],
      {operationLongue: true}
    )
    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return genererPreviewVideo(this.mq, this.pathConsignation, message)},
      ['commande.fichiers.genererPreviewVideo'],
      {operationLongue: true}
    )
    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return _traiterCommandeTranscodage(this.mq, this.pathConsignation, message)},
      ['commande.fichiers.transcoderVideo'],
      {operationLongue: true}
    )
  }

}

function genererPreviewImage(mq, pathConsignation, message) {
  const fctConversion = traitementMedia.genererPreviewImage
  return _genererPreview(mq, pathConsignation, message, fctConversion)
    .catch(err=>{
      console.error("media.genererPreviewImage ERROR fuuid %s: %O", message.fuuid, err)
    })
}

function genererPreviewVideo(mq, pathConsignation, message) {
  const fctConversion = traitementMedia.genererPreviewVideo
  return _genererPreview(mq, pathConsignation, message, fctConversion)
    .catch(err=>{
      console.error("media.genererPreviewVideo ERROR fuuid %s: %O", message.fuuid, err)
    })

}

function _traiterCommandeTranscodage(mq, pathConsignation, message) {
  return traiterCommandeTranscodage(mq, pathConsignation, message)
    .catch(err=>{
      console.error("media._traiterCommandeTranscodage ERROR %s: %O", message.fuuid, err)
    })
}

async function _genererPreview(mq, pathConsignation, message, fctConversion) {
  debug("Commande genererPreviewImage recue : %O", message)

  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  var opts = {}
  // Transmettre demande cle et attendre retour sur l'autre Q (on bloque Q operations longues)
  var hachageFichier = message.hachage
  if(message.version_courante) {
    // C'est une retransmission
    hachageFichier = message.version_courante.hachage
  }
  const liste_hachage_bytes = [hachageFichier]

  // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  // const chainePem = mq.pki.getChainePems()
  const domaineAction = 'MaitreDesCles.dechiffrage'
  const requete = {liste_hachage_bytes}
  debug("Nouvelle requete dechiffrage cle a transmettre : %O", requete)
  const reponseCle = await mq.transmettreRequete(domaineAction, requete)
  if(reponseCle.acces !== '1.permis') {
    return {err: reponseCle.acces, msg: `Erreur dechiffrage cle pour generer preview de ${message.fuuid}`}
  }
  debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)

  // Dechiffrer cle recue
  const informationCle = reponseCle.cles[hachageFichier]
  const cleChiffree = informationCle.cle
  const cleDechiffree = await mq.pki.decrypterAsymetrique(cleChiffree)

  // Demander cles publiques pour chiffrer preview
  const domaineActionClesPubliques = 'MaitreDesCles.certMaitreDesCles'
  const reponseClesPubliques = await mq.transmettreRequete(domaineActionClesPubliques, {})
  const clesPubliques = [reponseClesPubliques.certificat, [reponseClesPubliques.certificat_millegrille]]

  opts = {cleSymmetrique: cleDechiffree, metaCle: informationCle, clesPubliques}

  debug("Debut generation preview")
  const resultatPreview = await fctConversion(mq, pathConsignation, message, opts)
  debug("Fin traitement preview, resultat : %O", resultatPreview)

  // Transmettre transaction info chiffrage
  const domaineActionCles = 'MaitreDesCles.sauvegarderCle'
  const commandeMaitreCles = resultatPreview.commandeMaitreCles
  commandeMaitreCles.identificateurs_document = {
    attachement_fuuid: message.fuuid,
    type: 'preview',
  }

  await mq.transmettreCommande(domaineActionCles, commandeMaitreCles)

  // Transmettre transaction preview
  const domaineActionAssocierPreview = 'GrosFichiers.associerPreview'
  const transactionAssocierPreview = {
    uuid: message.uuid,
    fuuid: message.fuuid,
    mimetype_preview: resultatPreview.mimetype,
    fuuid_preview: resultatPreview.hachage_preview,
    extension_preview: resultatPreview.extension,
    hachage_preview: resultatPreview.hachage_preview,
  }

  if(resultatPreview.dataVideo) {
    transactionAssocierPreview.data_video = resultatPreview.dataVideo['data_video']
  }
  debug("Transaction associer preview : %O", transactionAssocierPreview)

  mq.transmettreTransactionFormattee(transactionAssocierPreview, domaineActionAssocierPreview)

}

module.exports = {GenerateurMedia}
