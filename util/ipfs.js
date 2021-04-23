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

const CRLF = '\r\n'
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

  const response = await axios({
    method: 'POST',
    headers: {
      ...data.getHeaders(),
    },
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

async function addRepertoire(repertoire) {

  // const fichierTexte = fs.createReadStream('/home/mathieu/test.json')
  //
  // // Faire un post vers l'API
  // const data = new FormData()
  //
  // var dirOptions = {
  //   filename: 'rep1',
  //   contentType: 'application/x-directory',
  //   knownLength: 0
  // };
  //
  // data.append('file', '', dirOptions)
  //
  // const nomFichierTexte = ['rep1', 'test.json'].join('%2F')
  //
  // data.append('file', fichierTexte, nomFichierTexte)

  const formData = new FormData()
  const cb = entry => cbPreparerIpfs(entry, formData, repertoire)
  const info = await preparerPublicationRepertoire(repertoire, cb)
  debug("Info publication repertoire avec IPFS : %O, FormData: %O", info, formData)
  //const resultat = await ipfsPublish(formData)
  //debug("Resultat publication IPFS : %O", resultat)

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
//
// async function publishName(req, res, next) {
//
//   // const cidPublier = 'QmPbUVmHccqr1cTB99XV2K1spqiU9iugQbeTAVKQewxU3V'
//   const cidPublier = 'QmTsJMQmMS9yamQFhbZ79k4aA6dfQpShePSEyx1WZHbwKA'
//   try {
//
//     var pathPublier = '%2Fipfs%2F' + cidPublier
//
//     const reponse = await axios({
//       method: 'POST',
//       url: URL_HOST + '/name/publish?arg=' + pathPublier + '&lifetime=5m&key=test4'
//     })
//     debug("Reponse : %O", reponse)
//
//     return res.status(200).send(reponse.data)
//   } catch(err) {
//     debug("Erreur : %O", err)
//     return res.sendStatus(500)
//   }
//
// }

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

module.exports = {init, addFichier, addRepertoire, cbPreparerIpfs, getPins}
