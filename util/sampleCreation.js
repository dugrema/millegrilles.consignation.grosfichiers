const fs = require('fs')
const path = require('path')
const lzma = require('lzma-native')
const tar = require('tar')

const { calculerHachageFichier, calculerHachageData } = require('./utilitairesHachage')
const { formatterDateString } = require('@dugrema/millegrilles.common/lib/js_formatters')

const PATH_BACKUP = '/tmp/mg-verificationbackups'

async function lireCatalogue(pathCatalogue) {
  const catalogue = await new Promise((resolve, reject)=>{
    fs.readFile(pathCatalogue, (err, data)=>{
      if(err) return reject(err)
      try {
        lzma.LZMA().decompress(data, (data, err)=>{
          if(err) return reject(err)
          const catalogue = JSON.parse(data)
          // console.info("Catalogue horaire %s charge : %O", pathCatalogue, catalogue)
          resolve(catalogue)
        })
      } catch(err) {
        reject(err)
      }
    })
  })

  return catalogue
}


async function sauvegarderFichier(pathFichier, contenu, opts) {
  opts = opts || {}
  const pathComplet = path.join(PATH_BACKUP, pathFichier)
  try {
    fs.mkdirSync(path.dirname(pathComplet), { recursive: true })
  } catch(err) {
    // EEXIST est OK
    if(err.errno !== -17) {
      console.debug("Erreur mkdir: %O", err)
    }
  }

  // console.debug(typeof(contenu))

  if(typeof(contenu) === 'object') {
    contenu = JSON.stringify(contenu)
  }

  if(opts.lzma) {
    await sauvegarderLzma(pathComplet, contenu)
  } else {
    fs.writeFileSync(pathComplet, contenu)
  }

  return pathComplet
}

async function sauvegarderLzma(fichier, contenu) {
  var compressor = lzma.createCompressor()
  var output = fs.createWriteStream(fichier)
  compressor.pipe(output)

  const promiseSauvegarde = new Promise((resolve, reject)=>{
    output.on('close', ()=>{resolve()})
    output.on('error', err=>{reject(err)})
  })

  compressor.write(contenu)
  compressor.end()
  return promiseSauvegarde
}

async function creerBackupHoraire(dateHeure, opts) {
  opts = opts || {}

  const dateFormattee = formatterDateString(dateHeure).slice(0, 10)

  var rep = path.join(opts.rep||'', 'horaire')

  const transactions = 'contenu dummy sans importance. Date: ' + dateFormattee
  const pathTransaction = await sauvegarderFichier(`${rep}/domaine.test_${dateFormattee}.jsonl.xz.mgs1`, transactions)

  // console.debug("Path fichier transactions : %s", pathTransaction)

  const hachageTransaction = await calculerHachageFichier(pathTransaction)

  const catalogue = {
    transactions_hachage: hachageTransaction,
    transactions_nomfichier: path.basename(pathTransaction),
    catalogue_nomfichier: `domaine.test_${dateFormattee}.json.xz`,
    heure: dateHeure.getTime()/1000,
  }

  const pathCatalogue = `${rep}/domaine.test_${dateFormattee}.json.xz`
  sauvegarderFichier(pathCatalogue, catalogue, {lzma: true})

  return pathCatalogue
}

async function creerBackupQuotidien(dateJour, opts) {
  opts = opts || {}
  var rep = opts.rep || ''

  const dateFormattee = formatterDateString(dateJour).slice(0, 8)
  const dateJourFormatte = `${dateFormattee.slice(0,4)}-${dateFormattee.slice(4,6)}-${dateFormattee.slice(6,8)}`

  const fichiersArchiveQuotidienne = []

  // Preparer des backup horaires a mettre dans le .tar
  var promises = []
  promises.push(creerBackupHoraire(new Date(`${dateJourFormatte} 03:00`), {rep}))
  promises.push(creerBackupHoraire(new Date(`${dateJourFormatte} 06:00`), {rep}))
  promises.push(creerBackupHoraire(new Date(`${dateJourFormatte} 07:00`), {rep}))
  const cataloguesHoraire = await Promise.all(promises)

  console.debug("Promises! : %O", cataloguesHoraire)

  const fichiers = {}
  for(idx in cataloguesHoraire) {
    const pathCatalogue = path.join(PATH_BACKUP, cataloguesHoraire[idx])
    const catalogue = await lireCatalogue(pathCatalogue)

    fichiersArchiveQuotidienne.push(path.basename(pathCatalogue))
    fichiersArchiveQuotidienne.push(catalogue.transactions_nomfichier)

    const hachageCatalogue = await calculerHachageFichier(pathCatalogue)
    var heure = ''+new Date(catalogue.heure*1000).getUTCHours()
    if(heure.length===1) heure = '0'+heure
    fichiers[heure] = {
      catalogue_nomfichier: path.basename(pathCatalogue),
      transactions_nomfichier: catalogue.transactions_nomfichier,
      transactions_hachage: catalogue.transactions_hachage,
      catalogue_hachage: hachageCatalogue,
    }
  }

  const catalogue = {
    fichiers_horaire: fichiers,
    jour: dateJour.getTime()/1000,
  }

  const pathCatalogue = `${rep}/horaire/domaine.test_${dateFormattee}.json.xz`
  await sauvegarderFichier(pathCatalogue, catalogue, {lzma: true})
  fichiersArchiveQuotidienne.unshift(path.basename(pathCatalogue))

  const pathBase = path.join(PATH_BACKUP, rep, 'horaire')
  const nomArchive = `domaine.test_${dateFormattee}.tar`
  const pathArchive = path.join(PATH_BACKUP, nomArchive)

  // Creer archive tar quotidienne
  // Creer nouvelle archive annuelle
  await tar.c(
    {
      file: pathArchive,
      cwd: pathBase,
    },
    fichiersArchiveQuotidienne
  )

  // Supprimer tous les fichiers dans l'archive
  for(let idx in fichiersArchiveQuotidienne) {
    const fichier = path.join(PATH_BACKUP, rep, 'horaire', fichiersArchiveQuotidienne[idx])
    fs.unlinkSync(fichier)
  }

}

async function creerSamples() {
  fs.mkdirSync(PATH_BACKUP)

  var promises = []
  promises.push(creerBackupHoraire(new Date("2020-01-01 00:00"), {rep: 'sample1'}))
  promises.push(creerBackupHoraire(new Date("2020-01-01 02:00"), {rep: 'sample1'}))
  promises.push(creerBackupHoraire(new Date("2020-01-01 03:00"), {rep: 'sample1'}))
  promises.push(creerBackupHoraire(new Date("2020-01-01 04:00"), {rep: 'sample1'}))

  promises.push(creerBackupQuotidien(new Date("2020-02-01"), {rep: 'sample2'}))

  await Promise.all(promises)
}

module.exports = creerSamples
