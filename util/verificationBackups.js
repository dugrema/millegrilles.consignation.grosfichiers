const debug = require('debug')('millegrilles:util:verificationBackups')
const fs = require('fs')
const path = require('path')
const readdirp = require('readdirp')
const parse = require('tar-parse')
const lzma = require('lzma-native')

const { PathConsignation } = require('../util/traitementFichier');
const { genererListeCatalogues } = require('./processFichiersBackup')
const { pki, ValidateurSignature } = require('./pki')
//const { formatterDateString } = require('@dugrema/millegrilles.common/lib/js_formatters')

async function chargerCatalogue(pathCatalogue) {
  return new Promise((resolve, reject)=>{
    fs.readFile(pathCatalogue, (err, data)=>{
      if(err) return reject(err)
      try {
        lzma.LZMA().decompress(data, (data, err)=>{
          if(err) return reject(err)
          const catalogue = JSON.parse(data)
          resolve(catalogue)
        })
      } catch(err) {
        reject(err)
      }
    })
  })
}

async function genererListeArchives(repertoire) {
  // Faire la liste des archives .tar du repertoire
  const settingsReaddirp = {
    type: 'files',
    fileFilter: [
       '*.tar',
    ],
    depth: 0,
  }

  return new Promise((resolve, reject)=>{
    const listeArchives = [];
    readdirp(
      repertoire,
      settingsReaddirp,
    )
    .on('data', entry=>{
      listeArchives.push(entry.fullPath)
    })
    .on('error', err=>{
      reject({err});
    })
    .on('end', ()=>{
      resolve(listeArchives);
    })
  })
}

async function parcourirBackupsHoraire(pathConsignation, domaine, cb, opts) {
  // Parcourt tous les catalogues de backup horaire d'un domaine
  // Invoque cb(catalogue: dict, pathComplet: str) pour chaque catalogue trouve

  opts = opts || {}

  const repertoireHoraire = pathConsignation.trouverPathBackupHoraire(domaine)
  const catalogues = await genererListeCatalogues(repertoireHoraire)
  debug("Catalogues horaire sous %s: %O", repertoireHoraire, catalogues)

  for(let idx in catalogues) {
    const pathCatalogue = path.join(repertoireHoraire, catalogues[idx])
    const catalogue = await chargerCatalogue(pathCatalogue)
    debug("Catalogue trouve: %s\n%O", pathCatalogue, catalogue)

    // Verifier si on fait un callback
    await cb(catalogue, pathCatalogue)
  }
}

async function parcourirBackupsQuotidiens(pathConsignation, domaine, cb, opts) {
  const repertoireBackup = pathConsignation.trouverPathBackupDomaine(domaine)
  const archives = await genererListeArchives(repertoireBackup)
  debug("Archives sous %s: %O", repertoireBackup, archives)

  // Parcourire toutes les archives - detecter si l'archive est quotidienne ou annuelle
  for(let idx in archives) {
    const pathArchive = archives[idx]
    debug("Ouvrir .tar %s", pathArchive)

    fs.createReadStream(pathArchive)
      .pipe(parse())
      .on('data', async function(entry) {
        entry.pause()
        debug("Fichier tar, entry : %s", entry.path)
        if(entry.path.endsWith('.json.xz')) {
          var decoder = lzma.createStream('autoDecoder')
          var data = ''
          decoder.on("data", buffer=>{
            data += buffer
          })
          decoder.on("end", ()=>{
            debug("Data fichier : %s", data)
            const catalogue = JSON.parse(data)
            debug("Catalogue charge : %O", catalogue)
          })

          const outStream = fs.createWriteStream('/tmp/' + entry.path)
          entry.pipe(decoder)
        }
        entry.resume()
      })
  }
}


module.exports = { parcourirBackupsHoraire, parcourirBackupsQuotidiens };
