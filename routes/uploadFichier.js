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

module.exports = {init}
