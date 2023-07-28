const debug = require('debug')('routes:sync')
const path = require('path')
const express = require('express')
const fs = require('fs')
const fsPromises = require('fs/promises')

const PATH_STAGING = '/var/opt/millegrilles/consignation/staging/fichiers/liste/listings'
const PATH_FUUIDS_LOCAUX = path.join(PATH_STAGING, 'fuuidsReclamesLocaux.txt.gz')
const PATH_FUUIDS_ARCHIVES = path.join(PATH_STAGING, 'fuuidsReclamesArchives.txt.gz')
const PATH_FUUIDS_MANQUANTS = path.join(PATH_STAGING, 'fuuidsManquants.txt.gz')
const PATH_FUUIDS_NOUVEAUX = path.join(PATH_STAGING, 'fuuidsNouveaux.txt')

function init(mq, opts) {
  opts = opts || {}

  const route = express.Router()

  // Fichiers sync primaire
  route.get('/fuuidsLocaux.txt.gz', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_LOCAUX))
  route.get('/fuuidsArchives.txt.gz', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_ARCHIVES))
  route.get('/fuuidsManquants.txt.gz', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_MANQUANTS))
  route.get('/fuuidsNouveaux.txt', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_NOUVEAUX, {gzip: false}))

  debug("Route /fichiers_transfert/sync initialisee")

  return route
}

async function getFichier(req, res, next, pathFichier, opts) {
    opts = opts || {}
    const gzipFlag = opts.gzip===false?false:true
    debug("getFichier ", pathFichier)

    try {
        const statInfo = await fsPromises.stat(pathFichier)
        const readStream = fs.createReadStream(pathFichier)
        await new Promise((resolve, reject) => {
            readStream.on('open', () => {
                if(gzipFlag) {
                    res.setHeader('Content-Type', 'application/gzip')
                } else {
                    res.setHeader('Content-Type', 'text/plain')
                }
                res.setHeader('Content-Length', statInfo.size)
                res.status(200)
                readStream.pipe(res)
            })
            readStream.on('close', resolve)
            readStream.on('error', reject)
        })
    } catch(err) {
        if(err.code === 'ENOENT') return res.sendStatus(404)

        debug("getFichier Erreur chargement fichier %s : %O", pathFichier, err)
        res.sendStatus(500)
    }
}

module.exports = init
