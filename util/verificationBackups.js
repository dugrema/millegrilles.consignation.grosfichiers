const debug = require('debug')('millegrilles:util:verificationBackups')
const fs = require('fs')
const path = require('path')
const readdirp = require('readdirp')
const parse = require('tar-parse')
const lzma = require('lzma-native')

const { PathConsignation } = require('../util/traitementFichier');
const { genererListeCatalogues } = require('./processFichiersBackup')
const { pki, ValidateurSignature } = require('./pki')
const { calculerHachageFichier, calculerHachageStream } = require('./utilitairesHachage')
const { formatterDateString } = require('@dugrema/millegrilles.common/lib/js_formatters')

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

async function genererListeHoraire(repertoire, opts) {
  opts = opts || {}

  // Faire la liste des archives .tar du repertoire
  const fileFilter = ['*.json.xz']
  if(opts.verification_hachage) {
    fileFilter.push('*.jsonl.xz')
    fileFilter.push('*.jsonl.xz.mgs1')
  }

  const settingsReaddirp = {
    type: 'files',
    fileFilter,
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
  const fichiers = await genererListeHoraire(repertoireHoraire, opts)
  debug("Fichiers horaire sous %s: %O", repertoireHoraire, fichiers)

  var dateHachageEntetes = {}  // Conserver liste de hachage d'entete par date (epoch secs)
  var hachagesTransactions = {}  // Conserver hachage de fichiers de transaction

  for(let idx in fichiers) {
    const pathFichier = fichiers[idx]
    if(pathFichier.endsWith('.json.xz')) {
      const catalogue = await chargerCatalogue(pathFichier)
      debug("Catalogue trouve: %s\n%O", pathFichier, catalogue)

      if(opts.verification_enchainement) {
        // Verification de l'enchainement entre catalogues horaires est actif
        // Retourner l'information d'entete et backup precedent
        const heureFormattee = formatterDateString(new Date(catalogue.heure*1000))
        try {
          dateHachageEntetes[heureFormattee] = {
            hachage_contenu: catalogue['en-tete'].hachage_contenu,
            uuid_transaction: catalogue['en-tete'].uuid_transaction,
            heure: catalogue.heure,
            backup_precedent: catalogue.backup_precedent,
          }
        } catch(err) {
          dateHachageEntetes[heureFormattee] = null
        }
      }

      if( opts.verification_hachage ) {
        if( hachagesTransactions[catalogue.transactions_hachage] ) {
          // Match avec transaction (deja calcule)
          hachagesTransactions[catalogue.transactions_hachage].catalogue = true
        } else {
          hachagesTransactions[catalogue.transactions_hachage] = {
            catalogue: true,
            transactions_nomfichier: catalogue.transactions_nomfichier,
          }
        }
      }

      // Verifier si on fait un callback
      await cb(catalogue, pathFichier)
    } else {
      debug("Fichier trouve: %s", pathFichier)
      if(opts.verification_hachage) {
        const hachage = await calculerHachageFichier(pathFichier)
        if(hachagesTransactions[hachage]) {
          // Match avec catalogue
          hachagesTransactions[hachage].transactions = true
        } else {
          hachagesTransactions[hachage] = {transactions: true, transactions_nomfichier: path.basename(pathFichier)}
        }
      }
    }
  }

  var erreursHachage = null, erreursCatalogues = null
  if(opts.verification_hachage) {
    erreursHachage = []

    // Faire un rapport de verifications
    for(let hachage in hachagesTransactions) {
      const infoHachage = hachagesTransactions[hachage]
      if( ! (infoHachage.catalogue && infoHachage.transactions) ) {
        erreursHachage.push({
          hachage,
          transactions_nomfichier: infoHachage.transactions_nomfichier,
        })
      }
    }
  } else {
    hachagesTransactions = null
  }

  if(opts.verification_enchainement) {
    erreursCatalogues = await verifierEntetes(dateHachageEntetes, opts.chainage)
  } else {
    dateHachageEntetes = null
  }

  return {
    dateHachageEntetes, hachagesTransactions,
    erreursHachage, erreursCatalogues
  }
}

async function verifierEntetes(dateHachageEntetes, chainage) {

  var chainage = null, erreursCatalogues = []
  Object.keys(dateHachageEntetes).sort().forEach(dateCatalogue=>{
    const infoCatalogue = dateHachageEntetes[dateCatalogue]
    const chainage_precedent = chainage

    // Placer entete courante pour verification du prochain backup horaire
    chainage = {
      hachage_contenu: infoCatalogue.hachage_contenu,
      uuid_transaction: infoCatalogue.uuid_transaction
    }

    const backup_precedent = infoCatalogue.backup_precedent
    if(!chainage_precedent && !backup_precedent) {
      return  // Rien a faire,
    } else if( ! chainage_precedent || ! backup_precedent ) {
      // Mismatch, on laisse continuer
    } else if(backup_precedent.hachage_contenu === chainage_precedent.hachage_contenu &&
              backup_precedent.uuid_transaction === chainage_precedent.uuid_transaction) {
      return  // Chaine ok
    }

    debug("Erreur, mismatch entetes horaires\n%O\n", backup_precedent, chainage_precedent)

    // Pas correct, on ajoute au rapport
    erreursCatalogues.push({...infoCatalogue, err: 'Erreur enchainement, backup precedent non trouve ou ne correspond pas'})
  })

  return erreursCatalogues
}

async function parcourirArchivesBackup(pathConsignation, domaine, cb, opts) {
  opts = opts || {}

  const repertoireBackup = pathConsignation.trouverPathBackupDomaine(domaine)
  const archives = await genererListeArchives(repertoireBackup)
  debug("Archives sous %s: %O", repertoireBackup, archives)

  // Parcourire toutes les archives - detecter si l'archive est quotidienne ou annuelle
  var dateHachageEntetes = {}, hachagesTransactions = []
  for(let idx in archives) {
    const pathArchive = archives[idx]
    debug("Ouvrir .tar %s", pathArchive)

    var promises = []
    await new Promise((resolve, reject)=>{

      fs.createReadStream(pathArchive)
        .pipe(parse())
        .on('data', entry=>{
          promises.push( processEntryTar(entry, cb, opts) )
        })
        .on('end', ()=>resolve())
        .on('error', err=>reject(err))

    }) // Promise

    // Attendre que toutes les promise (catalogues) de l'archive soient terminees
    // avant de passer a la prochaine archive
    debug("Archive %s, attente %d promesses", pathArchive, promises.length)
    const resultatsPromises = await Promise.all(promises)
    var resultatsFlat = resultatsPromises
      .filter(item=>item)           // Filtrer les entrees undefined
      .reduce((arr, item)=>{        // Aplatir les listes
        if(Array.isArray(item)) {
          return [...arr, ...item]
        }
        else return [...arr, item]
      }, [])

    if(opts.verification_enchainement) {
      // Conserver les entetes pour verifier le chainage
      const listeEntetes = resultatsFlat
        .filter(item=>{               // Conserver backups horaire
          return item && item.heure
        })
        .forEach(item=>{              // Conserver info pour chainage
          const heureFormattee = formatterDateString(new Date(item.heure*1000))
          dateHachageEntetes[heureFormattee] = {
            heure: item.heure,
            backup_precedent: item.backup_precedent,
            uuid_transaction: item.['en-tete'].uuid_transaction,
            hachage_contenu: item.['en-tete'].hachage_contenu,
          }
        })

      debug("Archive %s, entetes catalogues horaires %O", pathArchive, listeEntetes)
    }

    if(opts.verification_hachage) {
      // Conserver les entetes pour verifier le chainage

      const listeEntetes = resultatsPromises
        .filter(item=>{               // Conserver backups horaire et resultats de hachage
          return item && item.heure
        })
        .forEach(item=>{              // Conserver info pour verifier hachage
          hachagesTransactions[item.transactions_hachage] = 'dadada'
        })

      debug("Archive %s finie, hachage transactions %O", pathArchive, hachagesTransactions)
    }

  }

  var erreursCatalogues = null, erreursHachage = null
  if(opts.verification_enchainement) {
    // Verifier les chaines de catalogues horaires
    erreursCatalogues = await verifierEntetes(dateHachageEntetes, opts.chainage)
  } else {
    erreursCatalogues = null
  }

  if(opts.verification_hachage) {

  } else {
    hachagesTransactions = null
  }

  return {
    dateHachageEntetes, hachagesTransactions,
    erreursHachage, erreursCatalogues
  }

}

async function parcourirDomaine(pathConsignation, domaine, cb, opts) {
  // Parcoure tous les fichiers de backup d'un domaine

  await parcourirArchivesBackup(pathConsignation, domaine, cb, opts)
  await parcourirBackupsHoraire(pathConsignation, domaine, cb, opts)
}

async function processEntryTar(entry, cb, opts) {
  opts = opts || {}

  // Traitement d'une entree d'un fichier .tar
  // Si c'est un catalogue, on va invoquer le callback (cb)
  // cb(catalogue: dict, entry.path: str)

  // Tentative d'interruption du stream (non garanti)
  entry.pause()

  debug("Fichier tar, entry : %s", entry.path)
  if(entry.path.toLowerCase().endsWith('.json.xz')) {
    // C'est un catalogue

    return new Promise((resolve, reject)=>{
      var decoder = lzma.createStream('autoDecoder')
      var data = ''

      decoder.on("data", buffer=>{
        data += buffer
      })

      decoder.on("end", async ()=>{
        entry.pause()

        // debug("Data fichier : %s", data)
        const catalogue = JSON.parse(data)
        // C'est le catalogue quotidien
        debug("Catalogue charge : %O", catalogue)
        await cb(catalogue, entry.path)

        resolve(catalogue)

        entry.resume()
      })

      decoder.on("error", err=>{
        reject(err)
        entry.resume()
      })

      // Input tar entry vers lzma
      entry.pipe(decoder)

      entry.resume()  // Continuer a lire si le catalogue est plus grande que le buffer
    }) // Promise data

  } else if(entry.path.toLowerCase().endsWith('.tar')) {
    debug("Archive .tar : %s", entry.path)

    var promises = []
    await new Promise((resolve, reject)=>{

      entry.pipe(parse())
        .on('data', subEntry=>{
          promises.push( processEntryTar(subEntry, cb, opts) )
        })
        .on('end', ()=>resolve())
        .on('error', err=>reject(err))

    }) // Promise

    // Attendre que toutes les promise (catalogues) de l'archive soient terminees
    // avant de passer a la prochaine archive
    debug("Sous-archive %s, attente %d promesses", entry.path, promises.length)
    const resultats = await Promise.all(promises)
    debug("Archive %s finie", entry.path)

    return resultats

  } else if(opts.verification_hachage) {
    // Retourner le resultat de hachage du fichier
    const hachage = await calculerHachageStream(entry)
    return {hachage}
  } else {
    // Ce n'est pas un catalogue, on fait juste resumer
    entry.resume()
    return
  }

}


module.exports = { parcourirBackupsHoraire, parcourirArchivesBackup, parcourirDomaine }
