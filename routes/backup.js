const debug = require('debug')('routes:backup')
const path = require('path')
const express = require('express')
const fs = require('fs')
const fsPromises = require('fs/promises')

const PATH_BACKUP = '/var/opt/millegrilles/consignation/backup'

function init(mq, consignationManager, opts) {
  opts = opts || {}

  const route = express.Router()

  route.use((req, res, next)=>{
    req.consignationManager = consignationManager
    next()
  })

  // Fichiers sync primaire
  route.use(headersNoCache)
  route.post('/verifierFichiers', express.json(), verifierBackup)

  debug("Route /fichiers_transfert/backup initialisee")

  return route
}

function headersNoCache(req, res, next) {
    res.setHeader('Cache-Control', 'no-store')
    next()
}

async function verifierBackup(req, res) {
    const body = req.body
    debug("verifierBackup verifier %s", body)

    return res.sendStatus(500)
}

module.exports = init
