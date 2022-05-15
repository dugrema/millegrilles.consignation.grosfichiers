const debug = require('debug')('routes:uploadFichier')
const express = require('express')
const bodyParser = require('body-parser')

function init(mq, storeConsignation, opts) {
  opts = opts || {}
  const route = express()

  // Reception fichiers (PUT)
  const middlewareRecevoirFichier = storeConsignation.middlewareRecevoirFichier(opts)
  route.put('/fichiers_transfert/:correlation/:position', middlewareRecevoirFichier)

  // Verification fichiers (POST)
  const middlewareReadyFichier = storeConsignation.middlewareReadyFichier(mq, opts)
  route.post('/fichiers_transfert/:correlation', bodyParser.json(), middlewareReadyFichier)

  // Cleanup
  const middlewareDeleteStaging = storeConsignation.middlewareDeleteStaging(opts)
  route.delete('/fichiers_transfert/:correlation', middlewareDeleteStaging)

  debug("Route /fichiers_transfert initialisee")

  return route
}

module.exports = {init}
