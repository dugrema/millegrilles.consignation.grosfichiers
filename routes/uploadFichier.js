const debug = require('debug')('millegrilles:fichiers:uploadFichier')
const express = require('express')
const bodyParser = require('body-parser')
const fs = require('fs')
const path = require('path')
const readdirp = require('readdirp')

const {VerificateurHachage} = require('@dugrema/millegrilles.common/lib/hachage')

function init(pathConsignation) {
  const route = express()

  // Operations d'upload
  route.put('/fichiers/:correlation/:position', traiterUpload)
  route.post('/fichiers/:correlation', bodyParser.json(), traiterPostUpload)
  route.delete('/fichiers/:correlation', traiterDeleteUpload)

  return route
}

async function traiterUpload(req, res) {
  const {position, correlation} = req.params
  debug("/upload PUT %s position %d", correlation, position)

  const pathStaging = req.pathConsignation.consignationPathUploadStaging

  // Verifier si le repertoire existe, le creer au besoin
  const pathCorrelation = path.join(pathStaging, correlation)
  await new Promise((resolve, reject)=>{
    fs.mkdir(pathCorrelation, {recursive: true}, err=>{
      if(err && err.code !== 'EEXIST') {
        debug("Erreur creation repertoire multer : %O", err)
        return reject(err)
      }
      return resolve()
    })
  })

  // Creer output stream
  const pathFichier = path.join(pathCorrelation, position + '.part')
  const writer = fs.createWriteStream(pathFichier)

  try {
    const promise = new Promise((resolve, reject)=>{
      req.on('end', _=>{
        resolve()
      })
      req.on('error', err=>{
        reject(err)
      })

    })
    req.pipe(writer)
    await promise
  } catch(err) {
    debug("Erreur PUT: %O", err)
    return res.sendStatus(500)
  }

  res.sendStatus(200)
}

async function traiterPostUpload(req, res, next) {
  const correlation = req.params.correlation
  const pathStaging = req.pathConsignation.consignationPathUploadStaging
  const pathCorrelation = path.join(pathStaging, correlation)

  const informationFichier = req.body
  debug("Traitement post %s upload %O", correlation, informationFichier)

  var files = await readdirp.promise(pathCorrelation, {fileFilter: '*.part'})

  // Convertir les noms de fichier en integer
  files = files.map(file=>{
    return Number(file.path.split('.')[0])
  })

  // Trier en ordre numerique
  files.sort((a,b)=>{return a-b})

  const commandeMaitreCles = informationFichier.commandeMaitrecles
  const hachage = commandeMaitreCles.hachage_bytes
  const pathOutput = path.join(pathCorrelation, hachage + '.mgs2')
  const writer = fs.createWriteStream(pathOutput)
  const verificateurHachage = new VerificateurHachage(hachage)

  for(let idx in files) {
    const file = files[idx]
    debug("Charger fichier %s position %d", correlation, file)
    const pathFichier = path.join(pathCorrelation, file + '.part')
    const fileReader = fs.createReadStream(pathFichier)
    fileReader.on('data', chunk=>{
      // Verifier hachage
      verificateurHachage.update(chunk)
      writer.write(chunk)
    })

    const promise = new Promise((resolve, reject)=>{
      fileReader.on('end', _=>resolve())
      fileReader.on('error', err=>reject(err))
    })

    await promise
  }

  // Verifier le hachage
  try {
    await verificateurHachage.verify()
    debug("Fichier correlation %s OK\nhachage %s", correlation, hachage)

    const pathStorage = req.pathConsignation.trouverPathLocal(hachage)
    const status = await new Promise((resolve, reject) => {
      fs.mkdir(path.dirname(pathStorage), {recursive: true}, err=>{
        if(err) {
          debug("Erreur creation repertoire pour deplacement fichier : %O", err)
          return resolve(500)
        }

        fs.rename(pathOutput, pathStorage, err=>{
          if(err) {
            debug("Erreur deplacement fichier : %O", err)
            return resolve(500)
          }
          return resolve(201)
        })

      })
    })
    res.sendStatus(status)

  } catch(err) {
    debug("Erreur de verification du hachage : %O", err)
    res.sendStatus(500)
  } finally {
    // Nettoyer le repertoire
    fs.rmdir(pathCorrelation, {recursive: true}, err=>{
      if(err) {
        debug("Erreur suppression repertoire tmp %s : %O", pathCorrelation, err)
      }
    })
  }
}

async function downloadFichier(req, res) {
  const hachage = req.params.hachage
  debug("Downloader fichier %s", hachage)
  const pathFichier = pathStorageFichier(hachage)
  res.download(pathFichier)
}

async function traiterDeleteUpload(req, res) {
  const correlation = req.params.correlation
  const pathStaging = req.pathConsignation.consignationPathUploadStaging
  const pathCorrelation = path.join(pathStaging, correlation)

  // Nettoyer le repertoire
  fs.rmdir(pathCorrelation, {recursive: true}, err=>{
    if(err) {
      return debug("Erreur suppression repertoire tmp %s : %O", pathCorrelation, err)
    }
    debug("Repertoire DELETE OK, correlation %s", correlation)
  })

  res.sendStatus(200)
}

module.exports = {init}
