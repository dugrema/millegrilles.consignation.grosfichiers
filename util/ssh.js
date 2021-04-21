const debug = require('debug')('millegrilles:fichiers:ssh')
const express = require('express')
const {Client} = require('ssh2')
const { ssh, pki } = require('node-forge')
const ssh2_streams = require('ssh2-streams')
const multibase = require('multibase')
const fs = require('fs')
const path = require('path')

// Charger la cle privee utilisee pour se connecter par sftp
const _privateKeyPath = process.env.SFTP_KEY || '/run/secrets/pki.fichiers.key'
const _privateKey = fs.readFileSync(_privateKeyPath)

function routeSSH() {

  const route = express.Router()

  route.get('/', (req,res)=>{res.send("SSH module OK")})

  route.get('/getPublicKey', getPublicKey)

  // Routes avec connexion a SSH
  route.use(connecterSSH)

  // Routes avec SFTP (requiert connexion SSH existante)
  route.use(preparerSftp)
  route.get('/connecter', (req,res)=>{res.sendStatus(200)})
  route.get('/getFichier', getFichier)
  route.get('/putFichier', putFichier)

  return route
}

async function connecterSSH(host, port, username) {
  debug("Connecter SSH sur %s:%d avec username %s", host, port, username)
  const conn = new Client()
  await new Promise((resolve, reject)=>{
    conn.on('ready', resolve)
    conn.on('error', reject)
    conn.connect({host, port, username, privateKey: _privateKey})
  })
  return conn
}

function preparerSftp(conn) {
  return new Promise((resolve, reject)=>{
    conn.sftp((err, sftp)=>{
      if(err) return reject(err)
      return resolve(sftp)
    })
  })
}

async function getFichier(req, res, next) {
  const conn = req.sshConnection
  try {
    // const sftp = await new Promise((resolve, reject)=>{
    //   conn.sftp((err, sftp)=>{
    //     if(err) return reject(err)
    //     return resolve(sftp)
    //   })
    // })
    //
    // debug("SFTP ok")
    const sftp = req.sftp
    // Lire fichier
    await new Promise((resolve, reject)=>{
      sftp.fastGet('test.txt', './test.txt', err=>{
        if(err) return reject(err)
        resolve()
      })
    })

    return res.sendStatus(200)
  } catch(err) {
    debug("ERR sftp : %O", err)
    return res.sendStatus(500)
  }

}

function mkdir(sftp, resolve, reject, reps) {
  // Condition de fin de recursion
  if(reps.length === 0) return resolve()

  var rep = reps.shift()
  debug("Creer repertoire %s", rep)
  sftp.mkdir(rep, err=>{
    // Code 4: folder exists, c'est OK
    if(err && err.code !== 4) return reject(err)
    mkdir(sftp, resolve, reject, reps)
  })
}

async function putFichier(sftp, localPath, remotePath) {
  // S'assurer que le repertoire existe
  const repertoire = path.dirname(remotePath)
  await new Promise((resolve, reject)=>{
    debug("Creer repertoire remote : %s", repertoire)
    var last = ''
    var reps = repertoire.split('/').reduce((acc, item)=>{
      if(item === '.') return acc
      last = path.join(last, item)
      acc.push(last)
      return acc
    }, [])
    debug("Ajouter reps : %O", reps)
    mkdir(sftp, resolve, reject, reps)
  })
  debug("Repertoire %s cree", repertoire)

  const stepFunc = (current, _, total) => {
    debug("Upload : %s/%s", current, total)
  }

  // Lire fichier
  return new Promise((resolve, reject)=>{
    sftp.fastPut(
      localPath,
      remotePath,
      {step: stepFunc, mode: 0o644},
      err => {
        if(err) return reject(err)
        resolve()
      }
    )
  })

}

function getPublicKey() {
  const parseKey = ssh2_streams.utils.parseKey
  const privateKeyParsed = parseKey(_privateKey)[0]

  const publicKeyBytes = privateKeyParsed.getPublicSSH()
  const publicKeyBuffer = Buffer.from(publicKeyBytes, 'binary')
  var publicKeyb64 = multibase.encode('base64', publicKeyBuffer)
  publicKeyb64 = String.fromCharCode.apply(null, publicKeyb64).slice(1)
  debug("Public Key\n%s", publicKeyb64)

  const reponse = [privateKeyParsed.type, publicKeyb64, 'fichiers'].join(' ')

  return reponse
}

module.exports = {getPublicKey, connecterSSH, preparerSftp, putFichier}
