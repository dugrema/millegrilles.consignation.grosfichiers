const debug = require('debug')('millegrilles:fichiers:ssh')
// const express = require('express')
const {Client} = require('ssh2')
const { ssh, pki } = require('node-forge')
const ssh2_streams = require('ssh2-streams')
const multibase = require('multibase')
const fs = require('fs')
const path = require('path')
const { preparerPublicationRepertoire } = require('./publierUtils')

// Charger les cles privees utilisees pour se connecter par sftp
// Ed25519 est prefere, RSA comme fallback
const _privateEd25519KeyPath = process.env.SFTP_ED25519_KEY || '/run/secrets/sftp.ed25519.key.pem'
const _privateEd25519Key = fs.readFileSync(_privateEd25519KeyPath)
const _privateRsaKeyPath = process.env.SFTP_RSA_KEY || '/run/secrets/sftp.rsa.key.pem'
const _privateRsaKey = fs.readFileSync(_privateRsaKeyPath)
// const _privateKeyCombinees = [_privateEd25519Key, _privateRsaKey].join('\n')

// Creer un pool de connexions a reutiliser
const _poolConnexions = {},
      CONNEXION_TIMEOUT = 10 * 60 * 1000  // 10 minutes

const _intervalEntretienConnexions = setInterval(entretienConnexions, CONNEXION_TIMEOUT/2)

async function connecterSSH(host, port, username, opts) {
  opts = opts || {}
  debug("Connecter SSH sur %s:%d avec username %s (opts: %O)", host, port, username, opts)

  const connexionName = username + '@' + host + ':' + port
  const connectionPool = _poolConnexions[connexionName]
  if(connectionPool) {
    debug("Utilisation connexion SSH du pool : %O", connectionPool)
    connectionPool.dernierAcces = new Date().getTime()
    return connectionPool.connexion
  }

  var privateKey = _privateEd25519Key
  if(opts.keyType === 'rsa') {
    privateKey = _privateRsaKey
  }

  const conn = new Client()
  await new Promise((resolve, reject)=>{
    conn.on('ready', _=>{
      const objetPool = {
        dernierAcces: new Date().getTime(),
        connexion: conn,
      }
      _poolConnexions[connexionName] = objetPool
      conn.pool = objetPool
      resolve()
    })
    conn.on('error', reject)
    conn.on('end', _=>{
      debug("Connexion %s fermee, nettoyage pool", connexionName)
      delete _poolConnexions[connexionName]
    })
    conn.connect({host, port, username, privateKey})
  })
  return conn
}

function preparerSftp(conn) {
  const objetPool = conn.pool
  if(objetPool && objetPool.sftp) return objetPool.sftp

  return new Promise((resolve, reject)=>{
    conn.sftp((err, sftp)=>{
      if(err) return reject(err)

      if(objetPool) {
        objetPool.sftp = sftp
      }

      return resolve(sftp)
    })
  })
}

function mkdir(sftp, resolve, reject, reps) {
  // Condition de fin de recursion
  if(reps.length === 0) return resolve()

  var rep = reps.shift()
  // debug("Creer repertoire %s", rep)
  sftp.mkdir(rep, err=>{
    // Code 4: folder exists, c'est OK
    if(err && err.code !== 4) return reject(err)
    mkdir(sftp, resolve, reject, reps)
  })
}

async function putFichier(sftp, localPath, remotePath, opts) {
  opts = opts || {}

  // S'assurer que le repertoire existe
  const repertoire = path.dirname(remotePath)
  await new Promise((resolve, reject)=>{
    debug("Creer repertoire remote : %s", repertoire)

    // Creer liste de repertoires a creer a partir du repertoire courant
    var last = ''
    var reps = repertoire.split('/').reduce((acc, item, index)=>{
      if(item === '.') return acc
      if(item === '' && index === 0) {
        // On a un path absolu
        last = '/'
        return acc
      }
      last = path.join(last, item)
      acc.push(last)
      return acc
    }, [])
    // debug("Ajouter reps : %O", reps)
    mkdir(sftp, resolve, reject, reps)
  })
  debug("Repertoire %s cree", repertoire)

  // const stepFunc = (current, _, total) => {
  //   debug("Upload : %s/%s", current, total)
  // }

  const stepFunc = opts.progressCb

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

async function addRepertoire(sftp, repertoire, opts) {
  opts = opts || {}
  const repertoireRemote = opts.repertoireRemote || ''

  const listeFichiers = []
  const cb = entry => cbPreparerSsh(entry, listeFichiers, repertoire, repertoireRemote)
  const info = await preparerPublicationRepertoire(repertoire, cb)
  debug("Info publication repertoire avec SSH : %O, liste fichiers: %O", info, listeFichiers)

  for await (const fichier of listeFichiers) {
    debug("Traiter fichier : %O", fichier)
    await putFichier(sftp, fichier.localPath, fichier.remotePath)
  }
}

function cbPreparerSsh(entry, listeFichiers, pathStaging, repertoireRemote) {
  /* Sert a preparer l'upload d'un repertoire vers IPFS. Append a FormData. */
  const pathRelatif = entry.fullPath.replace(pathStaging + '/', '')

  debug("Ajout path relatif : %s", pathRelatif)
  if(entry.stats.isFile()) {
    debug("Creer readStream fichier %s", entry.fullPath)
    listeFichiers.push({
      localPath: entry.fullPath,
      remotePath: path.join(repertoireRemote, pathRelatif),
    })
  }
}

function getPublicKey(opts) {
  opts = opts || {}

  var privateKey = _privateEd25519Key
  if(opts.keyType === 'rsa') privateKey = _privateRsaKey

  const parseKey = ssh2_streams.utils.parseKey
  const privateKeyParsed = parseKey(privateKey)[0]

  const publicKeyBytes = privateKeyParsed.getPublicSSH()
  const publicKeyBuffer = Buffer.from(publicKeyBytes, 'binary')
  var publicKeyb64 = multibase.encode('base64', publicKeyBuffer)
  publicKeyb64 = String.fromCharCode.apply(null, publicKeyb64).slice(1)
  debug("Public Key\n%s", publicKeyb64)

  const reponse = [privateKeyParsed.type, publicKeyb64, 'fichiers@millegrilles'].join(' ')

  return reponse
}

async function listerConsignation(sftp, repertoire, opts) {
  opts = opts || {}
  debug("ssh.listerConsignation %s", repertoire)

  const list = await new Promise((resolve, reject)=>{
    sftp.readdir(repertoire, (err, list)=>{
      if(err) return reject(err)
      resolve(list)
    })
  })

  // Mapper la liste, conserver uniquement les fuuids (fichiers, enlever extension)
  var infoFichiers = list.filter(item=>item.attrs.isFile())
  // .map(item=>{
  //   return path.parse(item.filename).name
  // })
  if(opts.res && infoFichiers && infoFichiers.length > 0) {
    debug("Info Fichiers : %O", infoFichiers)
    infoFichiers.forEach(item=>{
      const infoFichier = {
        fuuid: path.parse(item.filename).name,
        ...item,
      }
      opts.res.write(JSON.stringify(infoFichier) + '\n')
    })
  }

  // Parcourir recursivement tous les repertoires
  const directories = list.filter(item=>item.attrs.isDirectory())
  for await (const directory of directories) {
    const subDirectory = path.join(repertoire, directory.filename)
    const list = await listerConsignation(sftp, subDirectory, opts)
    if(list) {
      fuuids = [...fuuids, ...list]
    }
  }

  if(!opts.res) {
    return fuuids
  }
}

async function entretienConnexions() {
  debug("Entretien connexions SFTP : %O", _poolConnexions)
  // Fermer les connexion inactives depuis plus longtemps que le timeout
  const poolIds = [...Object.keys(_poolConnexions)]
  const dateExpiration = new Date().getTime() - CONNEXION_TIMEOUT
  poolIds.forEach(poolId=>{
    const objetPool = _poolConnexions[poolId]
    if(objetPool.dernierAcces < dateExpiration) {
      debug("Fermer connexion SFTP %s", poolId)
      delete _poolConnexions[poolId]
      try {
        objetPool.connexion.end()
      } catch(err) {
        console.error("ERROR ssh.entretienConnexions %O", err)
      }
    }
  })
}

module.exports = {
  getPublicKey, connecterSSH, preparerSftp, putFichier, addRepertoire,
  listerConsignation,
}
