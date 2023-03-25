const debug = require('debug')('routes:uploadFichier')
const express = require('express')
const FichiersTransfertMiddleware = require('../models/fichiersTransfertMiddleware')

function init(mq, consigner, opts) {
  opts = opts || {}

  const fichiersMiddleware = new FichiersTransfertMiddleware(mq, opts)

  const route = express.Router()

  route.use((req, res, next)=>{
    req.fichiersMiddleware = fichiersMiddleware
    next()
  })

  // Reception fichiers (PUT)
  route.put('/:fuuid/:position', fichiersMiddleware.middlewareRecevoirFichier(opts))

  // Verification fichiers (POST)
  route.post('/:fuuid', express.json(), fichiersMiddleware.middlewareReadyFichier(opts), consigner)

  // Cleanup
  route.delete('/:fuuid', fichiersMiddleware.middlewareDeleteStaging(opts))

  debug("Route /fichiers_transfert initialisee")

  return route
}

module.exports = {init}
