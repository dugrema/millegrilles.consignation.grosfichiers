const debug = require('debug')('millegrilles:fichiers:media')
const {PathConsignation} = require('../util/traitementFichier');
const traitementMedia = require('../util/traitementMedia.js')

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
      return genererPreviewImage(this.mq, this.pathConsignation, message)}, ['commande.fichiers.genererPreviewImage'], {operationLongue: true})
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      return genererPreviewVideo(this.mq, this.pathConsignation, message)}, ['commande.fichiers.genererPreviewVideo'], {operationLongue: true})
    this.mq.routingKeyManager.addRoutingKeyCallback((routingKey, message)=>{
      return transcoderVideo(this.mq, this.pathConsignation, message)}, ['commande.fichiers.transcoderVideo'], {operationLongue: true})
  }

}

async function transcoderVideo(mq, pathConsignation, message) {
  debug("Commande genererPreviewImage recue : %O", message)

  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  const permission = message.permission
  var opts = {}
  var securite = '3.protege'
  // Transmettre evenement debut de transcodage
  mq.emettreEvenement({fuuid: message.fuuid}, 'evenement.fichiers.transcodageDebut')

  // securite = permission.securite || '3.protege'
  //
  // // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  // const chainePem = mq.pki.getChainePems()
  // permission['_certificat_tiers'] = chainePem
  // debug("Nouvelle requete permission a transmettre : %O", permission)
  // const domaineAction = permission['en-tete'].domaine
  // const reponseCle = await mq.transmettreRequete(domaineAction, permission, {noformat: true})
  // debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)

  var hachageFichier = message.hachage
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

  // Demander cles publiques pour chiffrer video transcode
  const domaineActionClesPubliques = 'MaitreDesCles.certMaitreDesCles'
  const reponseClesPubliques = await mq.transmettreRequete(domaineActionClesPubliques, {})
  const clesPubliques = [reponseClesPubliques.certificat, [reponseClesPubliques.certificat_millegrille]]

  // opts = {cleSymmetrique: cleDechiffree, iv: informationCle.iv, clesPubliques}
  opts = {cleSymmetrique: cleDechiffree, metaCle: informationCle, clesPubliques}

  debug("Debut dechiffrage fichier video")
  const resultatTranscodage = await traitementMedia.transcoderVideo(
    mq, pathConsignation, message, opts)

  debug("Resultat transcodage : %O", resultatTranscodage)

  // Transmettre transaction info chiffrage
  const domaineActionCles = 'MaitreDesCles.sauvegarderCle'
  // const transactionCles = {
  //   domaine: 'GrosFichiers',
  //   identificateurs_document: {
  //     fuuid: resultatTranscodage.fuuidVideo,
  //     attachement_fuuid: message.fuuid,
  //     type: 'video',
  //   },
  //   cles: resultatTranscodage.clesChiffrees,
  //   iv: resultatTranscodage.iv,
  //   hachage_bytes: resultatTranscodage.hachage,
  // }
  const commandeMaitreCles = resultatTranscodage.commandeMaitreCles
  commandeMaitreCles.identificateurs_document = {
      attachement_fuuid: message.fuuid,
      type: 'video',
    }
  await mq.transmettreCommande(domaineActionCles, commandeMaitreCles)

  // Transmettre transaction associer video transcode
  const transactionAssocierPreview = {
    uuid: resultatTranscodage.uuid,
    fuuid: message.fuuid,

    height: resultatTranscodage.height,
    fuuidVideo: resultatTranscodage.fuuidVideo,
    mimetypeVideo: resultatTranscodage.mimetypeVideo,
    hachage: resultatTranscodage.hachage,
    tailleFichier: resultatTranscodage.tailleFichier,
  }

  debug("Transaction transcoder video : %O", transactionAssocierPreview)

  mq.emettreEvenement({fuuid: message.fuuid}, 'evenement.fichiers.transcodageTermine')

  const domaineActionAssocierPreview = 'GrosFichiers.associerVideo'
  await mq.transmettreTransactionFormattee(transactionAssocierPreview, domaineActionAssocierPreview)
}

async function genererPreviewImage(mq, pathConsignation, message) {
  const fctConversion = traitementMedia.genererPreviewImage
  await _genererPreview(mq, pathConsignation, message, fctConversion)
}

async function genererPreviewVideo(mq, pathConsignation, message) {
  const fctConversion = traitementMedia.genererPreviewVideo
  await _genererPreview(mq, pathConsignation, message, fctConversion)
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
  // const transactionCles = {
  //   domaine: 'GrosFichiers',
  //   identificateurs_document: {
  //     fuuid: resultatPreview.fuuid,
  //     attachement_fuuid: message.fuuid,
  //     type: 'preview',
  //   },
  //   cles: resultatPreview.clesChiffrees,
  //   iv: resultatPreview.iv,
  //   tag: resultatPreview.tag,
  //   format: resultatPreview.format,
  //   hachage_bytes: resultatPreview.hachage_preview,
  // }
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
