const debug = require('debug')('millegrilles:messages:media')
const {PathConsignation} = require('../util/traitementFichier')
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
      {
        // operationLongue: true,
        qCustom: 'image',
      }
    )
    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return genererPreviewVideo(this.mq, this.pathConsignation, message)},
      ['commande.fichiers.genererPreviewVideo'],
      {
        // operationLongue: true,
        qCustom: 'image',
      }
    )
    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return _traiterCommandeTranscodage(this.mq, this.pathConsignation, message)},
      ['commande.fichiers.transcoderVideo'],
      {
        // operationLongue: true,
        qCustom: 'video',
      }
    )
  }

}

async function genererPreviewImage(mq, pathConsignation, message) {
  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  var opts = {}
  // Transmettre demande cle et attendre retour sur l'autre Q (on bloque Q operations longues)
  var hachageFichier = message.hachage
  if(message.version_courante) {
    // C'est une retransmission
    hachageFichier = message.version_courante.hachage || message.version_courante.fuuid
  }
  const {cleDechiffree, informationCle, clesPubliques} = await recupererCle(mq, hachageFichier)

  const optsConversion = {cleSymmetrique: cleDechiffree, metaCle: informationCle, clesPubliques}

  debug("Debut generation preview %O", message)
  const resultatConversion = await traitementMedia.genererPreviewImage(mq, pathConsignation, message, optsConversion)
  debug("Fin traitement preview, resultat : %O", resultatConversion)

  const {metadataImage, nbFrames, conversions} = resultatConversion

  // Extraire information d'images converties sous un dict
  let resultatPreview = null  // Utiliser poster (legacy)
  const images = {}
  for(let idx in conversions) {
    const conversion = conversions[idx]
    const resultat = {...conversion.informationImage}
    const cle = resultat.cle
    delete resultat.cle
    images[cle] = resultat
  }

  // Transmettre transaction preview
  // const domaineActionAssocierPreview = 'GrosFichiers.associerPreview'
  const domaineActionAssocier = 'GrosFichiers.associerConversions'
  const transactionAssocier = {
    uuid: message.uuid,
    fuuid: message.fuuid,
    images,
    width: metadataImage.width,
    height: metadataImage.height,
    mimetype: metadataImage['mime type'],
  }
  // Determiner si on a une image animee (fichier avec plusieurs frames, sauf PDF (plusieurs pages))
  const estPdf = transactionAssocier.mimetype === 'application/pdf'
  if(!estPdf && nbFrames > 1) transactionAssocier.anime = true

  debug("Transaction associer images converties : %O", transactionAssocier)

  mq.transmettreTransactionFormattee(transactionAssocier, domaineActionAssocier)
    .catch(err=>{
      console.error("ERROR media.genererPreviewImage Erreur association conversions d'image : %O", err)
    })
}

async function genererPreviewVideo(mq, pathConsignation, message) {
  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  var opts = {}
  // Transmettre demande cle et attendre retour sur l'autre Q (on bloque Q operations longues)
  var hachageFichier = message.hachage
  if(message.version_courante) {
    // C'est une retransmission
    hachageFichier = message.version_courante.hachage
  }
  const {cleDechiffree, informationCle, clesPubliques} = await recupererCle(mq, hachageFichier)

  const optsConversion = {cleSymmetrique: cleDechiffree, metaCle: informationCle, clesPubliques}

  debug("Debut generation preview")
  const resultatConversion = await traitementMedia.genererPreviewVideo(mq, pathConsignation, message, optsConversion)
  debug("Fin traitement preview, resultat : %O", resultatConversion)

  const {metadataImage, metadataVideo, nbFrames, conversions} = resultatConversion

  // Extraire information d'images converties sous un dict
  let resultatPreview = null  // Utiliser poster (legacy)
  const images = {}
  for(let idx in conversions) {
    const conversion = conversions[idx]
    const resultat = {...conversion.informationImage}
    const cle = resultat.cle
    delete resultat.cle
    images[cle] = resultat
  }

  // Transmettre transaction preview
  // const domaineActionAssocierPreview = 'GrosFichiers.associerPreview'
  const domaineActionAssocier = 'GrosFichiers.associerConversions'
  const transactionAssocier = {
    uuid: message.uuid,
    fuuid: message.fuuid,
    images,
    width: metadataImage.width,
    height: metadataImage.height,
    // mimetype: metadataImage['mime type'],
    metadata: metadataVideo,
  }
  transactionAssocier.anime = true

  debug("Transaction associer images converties : %O", transactionAssocier)

  mq.transmettreTransactionFormattee(transactionAssocier, domaineActionAssocier)
    .catch(err=>{
      console.error("ERROR media.genererPreviewImage Erreur association conversions d'image : %O", err)
    })
}

function _traiterCommandeTranscodage(mq, pathConsignation, message) {
  return traiterCommandeTranscodage(mq, pathConsignation, message)
    .catch(err=>{
      console.error("media._traiterCommandeTranscodage ERROR %s: %O", message.fuuid, err)
    })
}

// async function _genererPreview(mq, pathConsignation, message, fctConversion) {
//   debug("Commande genererPreviewImage recue : %O", message)
//
//   // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
//   var opts = {}
//   // Transmettre demande cle et attendre retour sur l'autre Q (on bloque Q operations longues)
//   var hachageFichier = message.hachage
//   if(message.version_courante) {
//     // C'est une retransmission
//     hachageFichier = message.version_courante.hachage
//   }
//   const {cleDechiffree, informationCle, clesPubliques} = await recupererCle(mq, hachageFichier)
//
//   const optsConversion = {cleSymmetrique: cleDechiffree, metaCle: informationCle, clesPubliques}
//
//   debug("Debut generation preview")
//   const resultatConversion = await fctConversion(mq, pathConsignation, message, optsConversion)
//   debug("Fin traitement preview, resultat : %O", resultatConversion)
//
//   // Extraire information d'images converties sous un dict
//   let resultatPreview = null  // Utiliser poster (legacy)
//   const images = {}
//   for(let idx in resultatConversion) {
//     const resultat = {...resultatConversion[idx]}
//     const cle = resultat.cle
//     delete resultat.cle
//     images[cle] = resultat
//
//     if(cle === 'poster') {
//       resultatPreview = {
//         hachage_bytes: resultat.hachage,
//         mimetype: resultat.mimetype,
//         extension: 'jpg',
//       }
//     }
//   }
//
//   // Transmettre transaction preview
//   // const domaineActionAssocierPreview = 'GrosFichiers.associerPreview'
//   const domaineActionAssocier = 'GrosFichiers.associerConversions'
//   const transactionAssocierPreview = {
//     uuid: message.uuid,
//     fuuid: message.fuuid,
//
//     images,
//
//     mimetype_preview: resultatPreview.mimetype,
//     fuuid_preview: resultatPreview.hachage_preview,
//     extension_preview: resultatPreview.extension,
//     hachage_preview: resultatPreview.hachage_preview,
//   }
//
//   if(resultatPreview.dataVideo) {
//     transactionAssocierPreview.data_video = resultatPreview.dataVideo['data_video']
//   }
//   debug("Transaction associer preview : %O", transactionAssocierPreview)
//
//   mq.transmettreTransactionFormattee(transactionAssocierPreview, domaineActionAssocierPreview)
//
// }

async function recupererCle(mq, hachageFichier) {
  const liste_hachage_bytes = [hachageFichier]

  // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  // const chainePem = mq.pki.getChainePems()
  const domaineAction = 'MaitreDesCles.dechiffrage'
  const requete = {liste_hachage_bytes}
  debug("Nouvelle requete dechiffrage cle a transmettre : %O", requete)
  const reponseCle = await mq.transmettreRequete(domaineAction, requete)
  if(reponseCle.acces !== '1.permis') {
    return {err: reponseCle.acces, msg: `Erreur dechiffrage cle pour generer preview de ${hachageFichier}`}
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

  return {cleDechiffree, informationCle, clesPubliques}
}

module.exports = {GenerateurMedia}
