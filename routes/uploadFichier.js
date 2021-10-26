const debug = require('debug')('millegrilles:fichiers:uploadFichier')
const express = require('express')
const bodyParser = require('body-parser')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const readdirp = require('readdirp')

const {VerificateurHachage} = require('@dugrema/millegrilles.common/lib/hachage')

function init(pathConsignation) {
  const route = express()

  // Operations d'upload
  // TODO : pour 1.43, downgrade verif a public (upload via web grosfichiers)
  //        remettre verification privee quand ce sera approprie, avec nouvelle
  //        methode de verification
  route.put('/fichiers/:correlation/:position', verifierNiveauPublic, traiterUpload)
  route.post('/fichiers/:correlation', verifierNiveauPublic, bodyParser.json(), traiterPostUpload)
  route.delete('/fichiers/:correlation', verifierNiveauPublic, traiterDeleteUpload)

  // Path utilise pour communication entre systemes. Validation ssl client (en amont, deja faite)
  route.put('/fichiers_transfert/:correlation/:position', verifierNiveauPublic, traiterUpload)
  route.post('/fichiers_transfert/:correlation', verifierNiveauPublic, bodyParser.json(), traiterPostUpload)
  route.delete('/fichiers_transfert/:correlation', verifierNiveauPublic, traiterDeleteUpload)

  return route
}

function verifierNiveauPrive(req, res, next) {
  // S'assurer que l'usager est au moins logge avec le niveau prive
  debug('!!! REQ verifierNiveauPrive : %O', req.headers)
  if(req.autorisationMillegrille.prive) {
    // OK
    return next()
  } else if(req.headers.verified === 'SUCCESS') {
    const dns = req.headers.dn.split(',').reduce((acc, item)=>{
      const kv = item.split('=')
      acc[kv[0]] = kv[1]
    }, {})
    const ou = dns['OU']
    if( ['web_protege', 'prive'].includes(ou) ) return next()
  }

  return res.sendStatus(403)
}

function verifierNiveauPublic(req, res, next) {
  // S'assurer que l'usager est au moins logge avec le niveau prive
  if(req.autorisationMillegrille.public) {
    return next()
  }
  return res.sendStatus(403)
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

  debug("Information fichier a uploader : %O", informationFichier)

  const commandeMaitreCles = informationFichier.commandeMaitrecles
  const transactionGrosFichiers = informationFichier.transactionGrosFichiers
  const hachage = commandeMaitreCles.hachage_bytes
  const pathOutput = path.join(pathCorrelation, hachage + '.mgs2')
  const writer = fs.createWriteStream(pathOutput)
  debug("Upload Fichier, recu hachage: %s", hachage)
  const verificateurHachage = new VerificateurHachage(hachage)

  for(let idx in files) {
    const file = files[idx]
    debug("Charger fichier %s position %d", correlation, file)
    const pathFichier = path.join(pathCorrelation, file + '.part')
    const fileReader = fs.createReadStream(pathFichier)

    let total = 0
    fileReader.on('data', chunk=>{
      // Verifier hachage
      verificateurHachage.update(chunk)
      writer.write(chunk)
      total += chunk.length
    })

    const promise = new Promise((resolve, reject)=>{
      fileReader.on('end', _=>resolve())
      fileReader.on('error', err=>reject(err))
    })

    await promise
    debug("Taille fichier %s : %d", pathOutput, total)
  }

  // Verifier le hachage
  try {
    await verificateurHachage.verify()
    debug("Fichier correlation %s OK\nhachage %s", correlation, hachage)

    // Transmettre la cle
    debug("Transmettre commande cle pour le fichier: %O", commandeMaitreCles)
    await req.amqpdao.transmettreEnveloppeCommande(commandeMaitreCles)

    debug("Deplacer le fichier vers le storage permanent")
    const pathStorage = req.pathConsignation.trouverPathLocal(hachage)
    await fsPromises.mkdir(path.dirname(pathStorage), {recursive: true})
    try {
      await fsPromises.rename(pathOutput, pathStorage)
    } catch(err) {
      console.warn("WARN : Erreur deplacement fichier, on copie : %O", err)
      await fsPromises.copyFile(pathOutput, pathStorage)
      await fsPromises.unlink(pathOutput)
    }

    if(transactionGrosFichiers) {
      debug("Transmettre commande fichier nouvelleVersion : %O", transactionGrosFichiers)
      const reponseGrosfichiers = await req.amqpdao.transmettreEnveloppeCommande(transactionGrosFichiers)
      debug("Reponse message grosFichiers : %O", reponseGrosfichiers)

      res.status(201).send(reponseGrosfichiers)
    } else {
      // Transaction non inclue, retourner OK
      res.sendStatus(200)
    }

  } catch(err) {
    console.error("ERROR uploadFichier.traiterPostUpload: Erreur de verification du hachage : %O", err)
    res.sendStatus(500)
  } finally {
    // Nettoyer le repertoire
    fs.rmdir(pathCorrelation, {recursive: true}, err=>{
      if(err) {
        console.warn("WARN uploadFichier.traiterPostUpload: Erreur suppression repertoire tmp %s : %O", pathCorrelation, err)
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
