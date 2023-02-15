const debug = require('debug')('routes:uploadFichier')
const express = require('express')
const bodyParser = require('body-parser')

function init(mq, storeConsignation, opts) {
  opts = opts || {}
  const route = express.Router()

  // Reception fichiers (PUT)
  const middlewareRecevoirFichier = storeConsignation.middlewareRecevoirFichier(opts)
  route.put('/:correlation/:position', middlewareRecevoirFichier)

  // Verification fichiers (POST)
  const middlewareReadyFichier = storeConsignation.middlewareReadyFichier(mq, opts)
  route.post('/:correlation', bodyParser.json(), middlewareReadyFichier)

  // Cleanup
  const middlewareDeleteStaging = storeConsignation.middlewareDeleteStaging(opts)
  route.delete('/:correlation', middlewareDeleteStaging)

  debug("Route /fichiers_transfert initialisee")

  return route
}

// async function evenementFichierPrimaire(mq, storeConsignation, req, res) {
//   if(storeConsignation.estPrimaire()) {
//     // Emettre evenement aux secondaires pour indiquer qu'un nouveau fichier est pret
//     const fuuid = res.hachage
//     debug("Evenement consignation primaire sur", fuuid)
//     const evenement = {fuuid}
//     mq.emettreEvenement(evenement, 'fichiers', {action: 'consignationPrimaire', exchange: '2.prive', attacherCertificat: true})
//       .catch(err=>console.error(new Date() + " uploadFichier.evenementFichierPrimaire Erreur ", err))
//   }

//   return res.status(202).send({ok: true})
// }

module.exports = {init}
