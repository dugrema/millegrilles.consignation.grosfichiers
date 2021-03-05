const fs = require('fs')
const path = require('path')
const lzma = require('lzma-native')
const tar = require('tar')

// const { hacherDictionnaire } = require('@dugrema/millegrilles.common/lib/forgecommon')
const { calculerHachageFichier, calculerHachageData } = require('./utilitairesHachage')
const { hacherMessage } = require('@dugrema/millegrilles.common/lib/formatteurMessage')
const { verifierMessage } = require('@dugrema/millegrilles.common/lib/validateurMessage')
const { formatterDateString } = require('@dugrema/millegrilles.common/lib/js_formatters')

const PATH_BACKUP = '/tmp/mg-verificationbackups'

var compteur_catalogues = 0
var backup_precedent = null

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
  var pathComplet = pathFichier
  if(!pathFichier.startsWith('/')) {
    pathComplet = path.join(PATH_BACKUP, pathFichier)
  }
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

  // console.debug("REP backup horaire : %s", rep)

  const transactions = 'contenu dummy sans importance. Date: ' + dateFormattee
  const pathTransaction = await sauvegarderFichier(`${rep}/domaine.test_${dateFormattee}.jsonl.xz.mgs1`, transactions)

  // console.debug("Path fichier transactions : %s", pathTransaction)

  const hachageTransaction = await calculerHachageFichier(pathTransaction)

  const catalogue = {
    transactions_hachage: hachageTransaction,
    transactions_nomfichier: path.basename(pathTransaction),
    catalogue_nomfichier: `domaine.test_${dateFormattee}.json.xz`,
    heure: dateHeure.getTime()/1000,
    'en-tete': {
      uuid_transaction: 'uuid-' + compteur_catalogues,
      hachage_contenu: 'hachage-' + compteur_catalogues,
    }
  }

  // Enchaine les catalogues horaires
  compteur_catalogues++
  if(backup_precedent) {
    catalogue.backup_precedent = backup_precedent
  }
  backup_precedent = {
    uuid_transaction: catalogue['en-tete'].uuid_transaction,
    hachage_entete: await hacherMessage(catalogue['en-tete']),
  }


  const pathCatalogue = `${rep}/domaine.test_${dateFormattee}.json.xz`
  sauvegarderFichier(pathCatalogue, catalogue, {lzma: true})

  return pathCatalogue
}

async function creerBackupQuotidien(dateJour, opts) {
  opts = opts || {}
  var rep = opts.rep || ''
  var repQuotidien = rep.startsWith('/')?rep:path.join(PATH_BACKUP, rep)

  console.debug("creerBackupQuotidien : REP Quotidien : %s", repQuotidien)

  const dateFormattee = formatterDateString(dateJour).slice(0, 8)
  const dateJourFormatte = `${dateFormattee.slice(0,4)}-${dateFormattee.slice(4,6)}-${dateFormattee.slice(6,8)}`

  const fichiersArchiveQuotidienne = []

  // Preparer des backup horaires a mettre dans le .tar
  var cataloguesHoraire = []
  if(opts.jourComplet) {
    for(let i=0; i<24; i++) {
      var dateArchive = new Date(dateJour.getTime())
      dateArchive.setUTCHours(i)
      const dateHeureStr = formatterDateString(dateArchive)
      const dateHeureFormattee = `${dateHeureStr.slice(0,4)}-${dateHeureStr.slice(4,6)}-${dateHeureStr.slice(6,8)} ${dateHeureStr.slice(8,10)}:00`
      cataloguesHoraire.push(await creerBackupHoraire(new Date(dateHeureFormattee), {rep: repQuotidien}))
    }
  } else {
    cataloguesHoraire.push(await creerBackupHoraire(new Date(`${dateJourFormatte} 03:00`), {rep: repQuotidien}))
    cataloguesHoraire.push(await creerBackupHoraire(new Date(`${dateJourFormatte} 06:00`), {rep: repQuotidien}))
    cataloguesHoraire.push(await creerBackupHoraire(new Date(`${dateJourFormatte} 07:00`), {rep: repQuotidien}))
  }

  // console.debug("Promises catalogue quotidien - creation horaire : %O", cataloguesHoraire)

  const fichiers = {}
  for(idx in cataloguesHoraire) {
    // console.debug("Catalogue horaire : %s", cataloguesHoraire[idx])
    let pathCatalogue = cataloguesHoraire[idx]
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

  const pathCatalogue = `${repQuotidien}/horaire/domaine.test_${dateFormattee}.json.xz`
  await sauvegarderFichier(pathCatalogue, catalogue, {lzma: true})
  fichiersArchiveQuotidienne.unshift(path.basename(pathCatalogue))

  const pathBase = path.join(repQuotidien, 'horaire')
  const nomArchive = `domaine.test_${dateFormattee}.tar`
  const pathArchive = path.join(repQuotidien, nomArchive)

  // Creer archive tar quotidienne
  // Creer nouvelle archive annuelle
  // console.debug("Creer archive quotidienne: %s", pathArchive)
  await tar.c(
    {
      file: pathArchive,
      cwd: pathBase,
    },
    fichiersArchiveQuotidienne
  ).then(()=>{
    // console.debug("TAR cree : %s", pathArchive)
  })

  // Supprimer tous les fichiers dans l'archive
  for(let idx in fichiersArchiveQuotidienne) {
    const fichier = path.join(repQuotidien, 'horaire', fichiersArchiveQuotidienne[idx])
    fs.unlinkSync(fichier)
  }
  fs.rmdirSync(path.join(repQuotidien, 'horaire'))

  return {pathArchive, catalogue}
}

async function creerBackupAnnuel(dateAnnee, opts) {
  opts = opts || {}
  var rep = opts.rep || ''

  const anneeFormattee = formatterDateString(dateAnnee).slice(0, 4)

  const archivesQuotidiennes = []
  if(opts.anneeComplete) {
    for(let i=1; i<=365; i++) {
      var dateArchive = new Date(dateAnnee.getTime())
      dateArchive.setUTCDate(i)
      const dateJourStr = formatterDateString(dateArchive).slice(0, 8)
      const dateJourFormatte = `${dateJourStr.slice(0,4)}-${dateJourStr.slice(4,6)}-${dateJourStr.slice(6,8)}`
      archivesQuotidiennes.push(await creerBackupQuotidien(new Date(dateJourFormatte), opts))
    }
  } else {
    archivesQuotidiennes.push(await creerBackupQuotidien(new Date(`${anneeFormattee}-01-01`), opts))
    archivesQuotidiennes.push(await creerBackupQuotidien(new Date(`${anneeFormattee}-01-02`), opts))
    archivesQuotidiennes.push(await creerBackupQuotidien(new Date(`${anneeFormattee}-01-04`), opts))
    archivesQuotidiennes.push(await creerBackupQuotidien(new Date(`${anneeFormattee}-02-05`), opts))
    archivesQuotidiennes.push(await creerBackupQuotidien(new Date(`${anneeFormattee}-03-06`), opts))
  }

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

async function creerSamplesHoraire(opts) {
  opts = opts || {}
  const pathSamples = opts.rep || (PATH_BACKUP + '/horaire/sample1')
  fs.mkdirSync(pathSamples, {recursive: true})
  console.debug("Samples horaires folder : %s, opts: %O", pathSamples, opts)

  // Fichiers horaires
  backup_precedent = null
  await creerBackupHoraire(new Date("2020-01-01 00:00"), {rep: pathSamples})
  await creerBackupHoraire(new Date("2020-01-01 02:00"), {rep: pathSamples})
  await creerBackupHoraire(new Date("2020-01-01 03:00"), {rep: pathSamples})
  await creerBackupHoraire(new Date("2020-01-01 04:00"), {rep: pathSamples})
}

async function creerSamplesQuotidien() {
  fs.mkdirSync(PATH_BACKUP + '/quotidien')

  // Archive quotidienne
  backup_precedent = null
  await creerBackupQuotidien(new Date("2020-02-01"), {rep: 'quotidien/sample2'})

  // Archives quotidiennes
  backup_precedent = null
  await creerBackupQuotidien(new Date("2020-03-01"), {rep: 'quotidien/sample3'})
  await creerBackupQuotidien(new Date("2020-03-02"), {rep: 'quotidien/sample3'})
  await creerBackupQuotidien(new Date("2020-03-03"), {rep: 'quotidien/sample3'})
}

async function creerSamplesAnnuel() {
  fs.mkdirSync(PATH_BACKUP + '/annuel', {recursive: true})

  // Archive annuelle
  backup_precedent = null
  await creerBackupAnnuel(new Date("2020-01-01"), {rep: 'annuel/sample4'})

  // Sample 5 - tous les types d'archive pour tester un cas complet
  backup_precedent = null
  await creerBackupAnnuel(new Date("2017-01-01"), {rep: 'annuel/sample5'})
  await creerBackupAnnuel(new Date("2018-01-01"), {rep: 'annuel/sample5'})
  await creerBackupAnnuel(new Date("2019-01-01"), {rep: 'annuel/sample5'})
  await creerBackupQuotidien(new Date("2020-01-01"), {rep: 'annuel/sample5'})
  await creerBackupQuotidien(new Date("2020-01-02"), {rep: 'annuel/sample5'})
  await creerBackupQuotidien(new Date("2020-01-03"), {rep: 'annuel/sample5'})
  await creerBackupQuotidien(new Date("2020-01-04"), {rep: 'annuel/sample5'})
  await creerBackupHoraire(new Date("2020-01-05 00:00"), {rep: 'annuel/sample5'})
  await creerBackupHoraire(new Date("2020-01-05 01:00"), {rep: 'annuel/sample5'})
  await creerBackupHoraire(new Date("2020-01-05 02:00"), {rep: 'annuel/sample5'})
  await creerBackupHoraire(new Date("2020-01-05 03:00"), {rep: 'annuel/sample5'})
}

async function creerSamplesLoad() {
  fs.mkdirSync(PATH_BACKUP + '/load')

  // Sample 6 - generer backup annuel pour 365 jours
  backup_precedent = null
  await creerBackupAnnuel(new Date("2018-01-01"), {rep: 'load/sample6', anneeComplete: true, jourComplet: true})

  //await Promise.all(promises)
}

module.exports = {creerSamplesLoad, creerSamplesAnnuel, creerSamplesQuotidien, creerSamplesHoraire, creerBackupQuotidien}
