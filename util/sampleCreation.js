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
  var cataloguesHoraire = []
  cataloguesHoraire.push(await creerBackupHoraire(new Date(`${dateJourFormatte} 03:00`), {rep}))
  cataloguesHoraire.push(await creerBackupHoraire(new Date(`${dateJourFormatte} 06:00`), {rep}))
  cataloguesHoraire.push(await creerBackupHoraire(new Date(`${dateJourFormatte} 07:00`), {rep}))

  console.debug("Promises catalogue quotidien - creation horaire : %O", cataloguesHoraire)

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
  const pathArchive = path.join(PATH_BACKUP, rep, nomArchive)

  // Creer archive tar quotidienne
  // Creer nouvelle archive annuelle
  console.debug("Creer archive quotidienne: %s", pathArchive)
  await tar.c(
    {
      file: pathArchive,
      cwd: pathBase,
    },
    fichiersArchiveQuotidienne
  ).then(()=>{
    console.debug("TAR cree : %s", pathArchive)
  })

  // Supprimer tous les fichiers dans l'archive
  for(let idx in fichiersArchiveQuotidienne) {
    const fichier = path.join(PATH_BACKUP, rep, 'horaire', fichiersArchiveQuotidienne[idx])
    fs.unlinkSync(fichier)
  }
  fs.rmdirSync(path.join(PATH_BACKUP, rep, 'horaire'))

  return {pathArchive, catalogue}
}

async function creerBackupAnnuel(dateAnnee, opts) {
  opts = opts || {}
  var rep = opts.rep || ''

  const anneeFormattee = formatterDateString(dateAnnee).slice(0, 4)

  const archivesQuotidiennes = []
  archivesQuotidiennes.push(await creerBackupQuotidien(new Date(`${anneeFormattee}-01-01`), {rep}))
  archivesQuotidiennes.push(await creerBackupQuotidien(new Date(`${anneeFormattee}-01-02`), {rep}))
  archivesQuotidiennes.push(await creerBackupQuotidien(new Date(`${anneeFormattee}-01-04`), {rep}))
  archivesQuotidiennes.push(await creerBackupQuotidien(new Date(`${anneeFormattee}-02-05`), {rep}))
  archivesQuotidiennes.push(await creerBackupQuotidien(new Date(`${anneeFormattee}-03-06`), {rep}))

  console.debug("Backup annuel, archives quotidiennes crees: %O", archivesQuotidiennes)

  const fichiers = {}, fichiersArchiveAnnuelle = []
  for(idx in archivesQuotidiennes) {
    const {pathArchive, catalogue} = archivesQuotidiennes[idx]

    // const pathArchiveComplet = path.join(PATH_BACKUP, rep, pathArchive)

    fichiersArchiveAnnuelle.push(path.basename(pathArchive))

    const hachageArchive = await calculerHachageFichier(pathArchive)
    var jour = formatterDateString(new Date(catalogue.jour*1000)).slice(0,8)
    fichiers[jour] = {
      archive_nomfichier: path.basename(pathArchive),
      archive_hachage: hachageArchive,
    }
  }

  const catalogue = {
    fichiers_quotidiens: fichiers,
    annee: dateAnnee.getTime()/1000,
  }

  const pathCatalogue = `${rep}/domaine.test_${anneeFormattee}.json.xz`
  await sauvegarderFichier(pathCatalogue, catalogue, {lzma: true})
  fichiersArchiveAnnuelle.unshift(path.basename(pathCatalogue))

  const pathBase = path.join(PATH_BACKUP, rep)
  const nomArchive = `domaine.test_${anneeFormattee}.tar`
  const pathArchive = path.join(PATH_BACKUP, rep, nomArchive)

  // Creer archive tar quotidienne
  // Creer nouvelle archive annuelle
  console.debug("Creer archive annuelle: %s\n%O", pathArchive, fichiersArchiveAnnuelle)
  await tar.c(
    {
      file: pathArchive,
      cwd: pathBase,
    },
    fichiersArchiveAnnuelle
  ).then(()=>{
    console.debug("TAR cree : %s", pathArchive)
  })

  // Supprimer tous les fichiers dans l'archive
  for(let idx in fichiersArchiveAnnuelle) {
    const fichier = path.join(PATH_BACKUP, rep, fichiersArchiveAnnuelle[idx])
    fs.unlinkSync(fichier)
  }

  return pathArchive
}

async function creerSamples() {
  fs.mkdirSync(PATH_BACKUP)

  // Fichiers horaires
  await creerBackupHoraire(new Date("2020-01-01 00:00"), {rep: 'sample1'})
  await creerBackupHoraire(new Date("2020-01-01 02:00"), {rep: 'sample1'})
  await creerBackupHoraire(new Date("2020-01-01 03:00"), {rep: 'sample1'})
  await creerBackupHoraire(new Date("2020-01-01 04:00"), {rep: 'sample1'})

  // Archive quotidienne
  await creerBackupQuotidien(new Date("2020-02-01"), {rep: 'sample2'})

  // Archives quotidiennes
  await creerBackupQuotidien(new Date("2020-03-01"), {rep: 'sample3'})
  await creerBackupQuotidien(new Date("2020-04-02"), {rep: 'sample3'})
  await creerBackupQuotidien(new Date("2020-03-03"), {rep: 'sample3'})

  // Archive annuelle
  await creerBackupAnnuel(new Date("2020-01-01"), {rep: 'sample4'})

  // Sample 5 - tous les types d'archive pour tester de "walking"
  await creerBackupAnnuel(new Date("2018-01-01"), {rep: 'sample5'})
  await creerBackupAnnuel(new Date("2019-01-01"), {rep: 'sample5'})
  await creerBackupQuotidien(new Date("2020-01-01"), {rep: 'sample5'})
  await creerBackupQuotidien(new Date("2020-01-02"), {rep: 'sample5'})
  await creerBackupQuotidien(new Date("2020-01-03"), {rep: 'sample5'})
  await creerBackupQuotidien(new Date("2020-01-04"), {rep: 'sample5'})
  await creerBackupHoraire(new Date("2020-01-05 00:00"), {rep: 'sample5'})
  await creerBackupHoraire(new Date("2020-01-05 01:00"), {rep: 'sample5'})
  await creerBackupHoraire(new Date("2020-01-05 02:00"), {rep: 'sample5'})
  await creerBackupHoraire(new Date("2020-01-05 03:00"), {rep: 'sample5'})

  //await Promise.all(promises)
}

module.exports = creerSamples
