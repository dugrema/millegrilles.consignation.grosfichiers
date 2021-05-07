const debug = require('debug')('millegrilles:fichiers:ipfs')
// const express = require('express')
const fs = require('fs')
const axios = require('axios')
const FormData = require('form-data')
const PeerId = require('peer-id')
const multibase = require('multibase')
const streamBuffers = require('stream-buffers')
const got = require('got')
const path = require('path')
const { preparerPublicationRepertoire } = require('./publierUtils')
const { chiffrerDocument } = require('@dugrema/millegrilles.common/lib/chiffrage')
const { getCertificatsChiffrage } = require('./cryptoUtils')

const CRLF = '\r\n',
      MAX_CONTENT_LENGTH = 1 * 1024 * 1024 * 1024  // 1 GB max (upload axios vers IPFS)
// const URL_HOST = 'http://192.168.2.131:5001/api/v0'

var _urlHost = 'http://ipfs:5001'

function init(urlHost) {
  urlHost = urlHost || _urlHost
  _urlHost = urlHost + '/api/v0'
}

async function addFichier(pathFichierLocal) {
  const readStream = fs.createReadStream(pathFichierLocal)

  // Faire un post vers l'API
  const data = new FormData()
  data.append('file', readStream, 'fichier.dat')

  const url = _urlHost + '/add'
  debug("ipfs.addFichier %s", url)

  //

  const response = await axios({
    method: 'POST',
    headers: {
      ...data.getHeaders(),
    },
    // maxContentLength: MAX_CONTENT_LENGTH,
    maxBodyLength: MAX_CONTENT_LENGTH,
    url,
    data,
  })

  debug("addFichier reponse ipfs : %O", response.data)
  var responseData = response.data
  try {
    var responseText = '[' + response.data.trim().split('\n') + ']'
    responseData = JSON.parse(responseData)
  } catch(err) {
    debug("reponse deja json")
  }

  return responseData
}

async function addRepertoire(repertoire, opts) {
  opts = opts || {}

  const formData = new FormData()
  const cb = entry => cbPreparerIpfs(entry, formData, repertoire)
  const info = await preparerPublicationRepertoire(repertoire, cb)
  debug("Info publication repertoire avec IPFS : %O, FormData: %O", info, formData)

  // Lancer publication - TODO: mettre message sur Q operations longues
  // const reponse = await ipfsPublish(formData)
  const url = _urlHost + '/add'
  debug("POST vers ipfs : %s", url)

  const response = await axios({
    method: 'POST',
    headers: {
      ...formData.getHeaders(),
    },
    url,
    data: formData,
  })

  var responseData = response.data
  try {
    if(typeof(responseData) === 'string') {
      responseData = JSON.parse('[' + response.data.trim().split('\n') + ']')
    }
  } catch(err) {
    console.error("ERROR ipfs.addRepertoire: Reponse data string non parseable : %O", err)
  }

  debug("Response : %O", responseData)

  // Trouver le CID - le repertoire temp est le seul sans / (path racine)
  const cidRepertoire = responseData.filter(item=>item.Name.indexOf('/')===-1)[0].Hash
  debug("CID repertoire : %s", cidRepertoire)

  // Si on a un cle, la mettre a jour
  if(opts.ipnsKeyName) {
    await publishName(cidRepertoire, opts.ipnsKeyName)
  }

  return {fichiers: responseData, cid: cidRepertoire}
}

// async function creerKey(req, res, next) {
//   try {
//     // Creer une cle privee, chiffrer et sauvegarder (e.g. maitre des cles)
//
//     // Generates a new Peer ID, complete with public/private keypair
//     // See https://github.com/libp2p/js-peer-id
//     const peerId = await PeerId.create({ keyType: 'ed25519' })
//     debug("Peer ID : %O", peerId)
//     debug("Pub key : %s", peerId.toB58String())
//
//     // var pubKey = String.fromCharCode.apply(null, multibase.encode('base64', peerId.marshalPubKey()))
//
//     const jsonContent = peerId.toJSON()
//
//     // return res.send({jsonContent})
//
//     // const readStream = fs.createReadStream('/var/opt/millegrilles/consignation/ipfs/staging/ipfs_test.key')
//
//     const cleRechargee = await PeerId.createFromJSON(jsonContent)
//     debug("Cle rechargee : %O", cleRechargee)
//
//     // const clePrivee = multibase.decode('m' + jsonContent.privKey)
//     const cleBytes = Buffer.from(jsonContent.privKey, 'base64')  // cleRechargee.marshal()  // marshal()
//     const cleStream = creerStreamFromBytes(cleBytes)
//     // const cleStream = creerStreamFromString(clePrivee)
//
//     debug("Cle Bytes : %O", cleBytes)
//     const data = new FormData()
//     data.append('key', cleStream, 'test3')
//     const reponseCle = await axios({
//       method: 'POST',
//       url: 'http://192.168.2.131:5001/api/v0/key/import?arg=test4',
//       headers: {
//         ...data.getHeaders()
//       },
//       data
//     })
//     debug("Reponse axios : %O", reponseCle)
//     // const reponse = await axios({
//     //   method: 'POST',
//     //   url: 'http://192.168.2.131:5001/api/v0/key/gen?arg=cle1',
//     //   // data: {name: 'cle1'},
//     // })
//     // debug("Reponse : %O", reponse)
//     // const reponseData = reponse.data
//
//     // const reponseCle = await axios({
//     //   method: 'POST',
//     //   url: 'http://192.168.2.131:5001/api/v0/key/export?arg=cle1&output=%2Fexport',
//     // })
//     // debug("Reponse : %O", reponseCle)
//
//     res.send(reponseCle.data)
//   } catch(err) {
//     debug("Erreur : %O", err)
//     res.sendStatus(500)
//   }
// }

async function publishName(cid, keyName) {
  // const cidPublier = 'QmPbUVmHccqr1cTB99XV2K1spqiU9iugQbeTAVKQewxU3V'
  // const cidPublier = 'QmTsJMQmMS9yamQFhbZ79k4aA6dfQpShePSEyx1WZHbwKA'
  var pathPublier = encodeURIComponent('/ipfs/' + cid)

  // Publier avec lifetime de 3 jours - cles actives vont etre republiees a tous les jours
  try {
    const reponse = await axios({
      method: 'POST',
      url: _urlHost + '/name/publish?arg=' + pathPublier + '&lifetime=25h&ttl=60s&key=' + keyName,
      timeout: 240000,  // 4 minutes max
    })
    debug("Reponse : %O", reponse)
    return reponse
  } catch(err) {
    const reponse = err.response
    if(reponse) {
      if(reponse.status === 500) {
        debug("ipfs.publishName Reponse status %d\n%O", reponse.status, reponse.data)
        // Verifier si on a un cle manquante
        const codeReponse = reponse.data.Code,
              messageReponse = reponse.data.Message
        if(codeReponse === 0 && messageReponse.indexOf('no key')>-1) {
          debug("Cle %s manquante", keyName)
          const errManquante = new Error(`Cle ${keyName} manquante`)
          errManquante.code = 404
          errManquante.keyName = keyName
          throw errManquante
        }
      }
      throw new Error(reponse.data.Message)
    }
    throw err
  }

}

function creerStreamFromBytes(sourceBytes) {
  var myReadableStreamBuffer = new streamBuffers.ReadableStreamBuffer({
    frequency: 10,      // in milliseconds.
    chunkSize: 2048     // in bytes.
  })

  // With a buffer
  myReadableStreamBuffer.put(sourceBytes)
  myReadableStreamBuffer.stop()

  // Or with a string
  // myReadableStreamBuffer.put(sourceBytes, "binary")

  return myReadableStreamBuffer
}

function creerStreamFromString(sourceBytes) {
  var myReadableStreamBuffer = new streamBuffers.ReadableStreamBuffer({
    frequency: 10,      // in milliseconds.
    chunkSize: 2048     // in bytes.
  })

  // Or with a string
  myReadableStreamBuffer.put(sourceBytes, "utf8")
  myReadableStreamBuffer.stop()

  return myReadableStreamBuffer
}

function cbPreparerIpfs(entry, formData, pathStaging) {
  /* Sert a preparer l'upload d'un repertoire vers IPFS. Append a FormData. */
  const pathRelatifBase = path.dirname(pathStaging)
  const pathRelatif = encodeURIComponent(entry.fullPath.replace(pathRelatifBase, ''))
  debug("Ajout path relatif : %s", pathRelatif)
  if(entry.stats.isFile()) {
    debug("Creer readStream fichier %s", entry.fullPath)
    formData.append('file', fs.createReadStream(entry.fullPath), pathRelatif)
  } else if(entry.stats.isDirectory()) {
    var dirOptions = {
      filename: pathRelatif,
      contentType: 'application/x-directory',
      knownLength: 0
    }
    formData.append('file', '', dirOptions)
  }
}

function getPins(res) {
  const url = _urlHost + '/pin/ls?stream=true'
  const streamIpfs = got.stream.post(url).end()
  streamIpfs.pipe(res)
}

async function creerCleIpns(mq, nomCle) {
  // Creer une cle privee, chiffrer et sauvegarder (e.g. maitre des cles)

  // Generates a new Peer ID, complete with public/private keypair
  // See https://github.com/libp2p/js-peer-id
  const peerId = await PeerId.create({ keyType: 'ed25519' })
  // debug("Peer ID : %O", peerId)
  debug("Nouvelle cle IPNS, pub key : %s", peerId.toB58String())

  const jsonContent = peerId.toJSON()
  // debug("JSON Content cle ipns : %O", jsonContent)

  // Chiffrer la cle, transmettre au maitre des cles
  const certificatsChiffrage = await getCertificatsChiffrage(mq)
  const identificateurs_document = {type: 'ipns', nom: nomCle}
  const {ciphertext, commandeMaitrecles} = await chiffrerDocument(
    jsonContent, 'Publication', certificatsChiffrage, identificateurs_document)
  debug("Cle IPNS, commande maitre des cles : %O", commandeMaitrecles)

  const cleRechargee = await PeerId.createFromJSON(jsonContent)
  // debug("Cle rechargee : %O", cleRechargee)
  const cleBytes = Buffer.from(jsonContent.privKey, 'base64')
  const cleStream = creerStreamFromBytes(cleBytes)

  const nomCleEncoded = encodeURIComponent(nomCle)

  // debug("Cle Bytes : %O", cleBytes)
  const data = new FormData()
  data.append('key', cleStream, nomCleEncoded)
  try {
    const reponseCle = await axios({
      method: 'POST',
      url: _urlHost + '/key/import?arg=' + nomCleEncoded,
      headers: {
        ...data.getHeaders()
      },
      data
    })
    debug("Reponse axios : %O", reponseCle.data)

    // Transmettre commande maitre des cles
    const domaineActionCles = 'MaitreDesCles.sauvegarderCle'
    await mq.transmettreCommande(domaineActionCles, commandeMaitrecles)
    const cleId = reponseCle.data.Id

    const reponse = { cleId, cle_chiffree: ciphertext}
    return reponse

  } catch(err) {
    const response = err.response
    if(response && response.status === 500 && response.data.Code === 0) {
      // OK, la cle existe deja
      return {err: response.data, message: 'La cle existe deja'}
    }

    // Autre type d'erreur
    throw err
  }
}

async function importerCleIpns(nomCle, cleJson) {
  const cleBytes = Buffer.from(cleJson.privKey, 'base64')
  const cleStream = creerStreamFromBytes(cleBytes)
  const nomCleEncoded = encodeURIComponent(nomCle)

  const data = new FormData()
  data.append('key', cleStream, nomCleEncoded)
  try {
    const reponseCle = await axios({
      method: 'POST',
      url: _urlHost + '/key/import?arg=' + nomCleEncoded,
      headers: {
        ...data.getHeaders()
      },
      data
    })
    debug("Reponse axios : %O", reponseCle.data)
    return reponseCle.data

  } catch(err) {
    const response = err.response
    if(response && response.status === 500 && response.data.Code === 0) {
      // OK, la cle existe deja
      return {err: response.data, message: 'La cle existe deja'}
    }

    // Autre type d'erreur
    throw err
  }
}

module.exports = {
  init, addFichier, addRepertoire,
  cbPreparerIpfs, getPins, publishName,
  creerCleIpns, importerCleIpns,
}
