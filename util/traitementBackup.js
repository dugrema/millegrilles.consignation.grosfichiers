const debug = require('debug')('millegrilles:util:backup')
const fs = require('fs')
const readdirp = require('readdirp')
const path = require('path')
const uuidv1 = require('uuid/v1')
const crypto = require('crypto')
const lzma = require('lzma-native')
const { spawn } = require('child_process')
const readline = require('readline')
const tar = require('tar')
const moment = require('moment')
const tmp = require('tmp-promise')

const { formatterDateString } = require('@dugrema/millegrilles.common/lib/js_formatters')
const {uuidToDate} = require('./UUIDUtils')
const transformationImages = require('./transformationImages')
const {pki, ValidateurSignature} = require('./pki')
const { calculerHachageFichier } = require('./utilitairesHachage')
const {PathConsignation, extraireTarFile, supprimerFichiers, getFichiersDomaine} = require('./traitementFichier')
const {traiterFichiersBackup, traiterGrosfichiers, traiterFichiersApplication} = require('./processFichiersBackup')
const { parcourirDomaine } = require('./verificationBackups')

const MAP_MIMETYPE_EXTENSION = require('./mimetype_ext.json')
const MAP_EXTENSION_MIMETYPE = require('./ext_mimetype.json')

class TraitementFichierBackup {

  constructor(rabbitMQ) {
    this.rabbitMQ = rabbitMQ
    const idmg = rabbitMQ.pki.idmg
    this.pathConsignation = new PathConsignation({idmg})
  }

  // PUT pour un fichier de backup
  async traiterPutBackup(req) {

    debug("Body PUT traiterPutBackup : %O", req.body)

    const pathConsignation = new PathConsignation({idmg: req.autorisationMillegrille.idmg})

    //const timestampBackup = new Date(req.body.timestamp_backup * 1000);
    //if(!timestampBackup) {return reject("Il manque le timestamp du backup");}

    //let pathRepertoire = pathConsignation.trouverPathBackupHoraire(timestampBackup)
    let fichiersTransactions = req.files.transactions[0]
    let fichierCatalogue = req.files.catalogue[0]

    // Valider transaction maitre des cles
    // Transmettre cles du fichier de transactions
    let fichierMaitrecles = null
    if(req.files.cles) {
      fichierMaitrecles = req.files.cles[0]
    }

    const amqpdao = req.amqpdao

    // Deplacer les fichiers de backup vers le bon repertoire /backup
    const fichiersDomaines = await traiterFichiersBackup(
      amqpdao, pathConsignation, fichiersTransactions, fichierCatalogue, fichierMaitrecles)

    return fichiersDomaines
  }

  async traiterPutApplication(req) {
    debug("Body PUT traiterPutApplication : %O", req.body)

    const pathConsignation = new PathConsignation({idmg: req.autorisationMillegrille.idmg})
    const nomApplication = req.params.nomApplication

    const pathRepertoire = pathConsignation.trouverPathBackupApplication(nomApplication)

    const transactionsCatalogue = JSON.parse(req.body.catalogue)
    const transactionsMaitredescles = JSON.parse(req.body.transaction_maitredescles)
    const fichierApplication = req.files.application[0]

    // Deplacer les fichiers de backup vers le bon repertoire /backup
    const amqpdao = req.rabbitMQ
    await traiterFichiersApplication(
      amqpdao, transactionsCatalogue, transactionsMaitredescles, fichierApplication, pathRepertoire)
  }

}

async function getListeDomaines(req, res, next) {
  debug("Retourner la liste des domaines avec au moins un fichier de backup disponible")

  try {
    debug("Path consignation : %O", req.pathConsignation)
    const domaines = await identifierDomaines(req.pathConsignation.getPathBackupDomaines())
    res.status(200).send({domaines})
  } catch(err) {
    console.error("getListeDomaines: Erreur\n%O", err)
    res.sendStatus(500)
  }

}

async function getCataloguesDomaine(req, res) {
  debug("getCataloguesDomaine params: %O", req.params)

  const domaine = req.params.domaine

  // Retourne la liste de tous les catalogues de backup horaire d'un domaine
  const cbCatalogues = catalogue => {
    debug("getCataloguesDomaine Catalogue a transmettre: %O", catalogue)
    res.write(JSON.stringify(catalogue) + '\n')
  }

  res.set('Content-Type', 'text/plain')
  res.status(200)  // Header, commencer transfert
  await parcourirDomaine(req.pathConsignation, domaine, cbCatalogues)
  res.end('\n\n')

  debug("getCataloguesDomaine termine pour %s", req.path)
}

async function identifierDomaines(pathRepertoireBackup) {
  // Retourne la liste de tous les domaines avec un backup

  const settingsReaddirp = {
    type: 'directories',
    depth: 0,
  }

  return new Promise((resolve, reject)=>{
    const listeDomaines = [];
    readdirp(
      pathRepertoireBackup,
      settingsReaddirp,
    )
    .on('data', entry=>{
      listeDomaines.push(entry.path)
    })
    .on('error', err=>{
      reject(err);
    })
    .on('end', ()=>{
      resolve(listeDomaines);
    })
  })

  return domaines
}

async function getListeFichiers(req, res) {
  // Retourne la liste de tous les fichiers de backup pour un domaine

  debug("listerFichiers params: %O", req.params)

  const domaine = req.params.domaine
  const pathRepertoireDomaine = req.pathConsignation.trouverPathBackupDomaine(domaine)

  const settingsReaddirp = {
    type: 'files',
    depth: 1,
  }

  try {
    const fichiers = await new Promise((resolve, reject)=>{
      const fichiers = []
      readdirp(
        pathRepertoireDomaine,
        settingsReaddirp,
      )
      .on('data', entry=>{
        fichiers.push(entry.path)
      })
      .on('error', err=>{
        reject(err);
      })
      .on('end', ()=>{
        resolve(fichiers);
      })
    })

    // res.set('Content-Type', 'text/plain')
    res.status(200).send({domaine, fichiers})
  } catch(err) {
    console.error("Erreur listerFichiers: %O", err)
    res.sendStatus(500)
  }

}

async function getFichier(req, res) {
  debug("getFichier %s params: %O", req.path, req.params)

  const domaine = req.params.domaine,
        nomFichier = req.params.nomFichier,
        sousrep = req.params.sousrep
  const pathRepertoireDomaine = req.pathConsignation.trouverPathBackupDomaine(domaine)

  if(sousrep && ! ['snapshot', 'horaire'].includes(sousrep)) {
    return res.status(400).send({err: 'Sous repertoire non supporte : ' + sousrep})
  }

  var subPathFichier = path.join(sousrep||'', nomFichier)

  const pathFichier = path.join(pathRepertoireDomaine, subPathFichier)
  try {
    const infoFichier = new Promise((resolve, reject)=>{
      fs.stat(pathFichier, (err, stat)=>{
        if(err) return reject(err)
      })
    })
    res.status(200).sendFile(pathFichier)
  } catch(err) {
    debug("Code erreur : %O", err)
    return res.sendStatus(404)
  }
}

module.exports = {
  TraitementFichierBackup, formatterDateString, getListeDomaines,
  getCataloguesDomaine, getListeFichiers, getFichier,

  identifierDomaines
};
