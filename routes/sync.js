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

function init(mq, consignationManager, opts) {
  opts = opts || {}

  const route = express.Router()

  route.use((req, res, next)=>{
    req.consignationManager = consignationManager
    next()
  })

  // Fichiers sync primaire
  route.get('/fuuidsLocaux.txt.gz', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_LOCAUX))
  route.get('/fuuidsArchives.txt.gz', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_ARCHIVES))
  route.get('/fuuidsManquants.txt.gz', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_MANQUANTS))
  route.get('/fuuidsNouveaux.txt', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_NOUVEAUX, {gzip: false}))

  route.post('/fuuidsInfo', express.json(), getFuuidsInfo)

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

async function getFuuidsInfo(req, res, next) {
    const body = req.body
    debug("getFuuidsInfo body ", body)

    const fuuids = body.fuuids || []
    if(fuuids.length === 0) {
        debug("getFuuidsInfo Aucuns fuuids a charger")
        return res.sendStatus(400)
    }

    const fichiers = []
    for await(const fuuid of fuuids) {
        try {
            const infoFichier = await req.consignationManager.getInfoFichier(fuuid)
            const statFichier = infoFichier.stat || {}
            debug("getFuuids infoFichier : ", statFichier)
            fichiers.push({fuuid, status: 200, size: statFichier.size})
        } catch(err) {
            if(err.code === 'ENOENT') {
                fichiers.push({fuuid, status: 404})
            } else {
                console.error(new Date() + " getFuuidsInfo Erreur fichier : ", err)
                fichiers.push({fuuid, status: 500})
            }
        }
    }

    return res.status(200).json(fichiers)
}

module.exports = init
