const debug = require('debug')('millegrilles:util:restaurationBackup')
const fs = require('fs')
const path = require('path')
const readdirp = require('readdirp')
const tar = require('tar')

const { PathConsignation, getFichiersDomaine, getGrosFichiersHoraire } = require('../util/traitementFichier')

// Classe qui s'occupe du staging d'archives et fichiers de backup
// Prepare et valide le contenu du repertoire staging/
class RestaurateurBackup {

  constructor(mq, opts) {
    if(!opts) opts = {}
    this.mq = mq
    this.pki = mq.pki

    const idmg = this.pki.idmg;
    this.pathConsignation = new PathConsignation({idmg})

    // Configuration optionnelle
    this.pathBackupHoraire = opts.backupHoraire || this.pathConsignation.consignationPathBackupHoraire;
    this.pathBackupArchives = opts.archives || this.pathConsignation.consignationPathBackupArchives;
  }

  async restaurerDomaine(req, res, next) {

    const idmg = req.autorisationMillegrille.idmg
    const domaine = req.params.domaine
    const pathRepertoireBackup = path.join(this.pathConsignation.consignationPathBackup, 'domaines')
    debug("restaurerDomaine, Path repertoire backup : %s\nparams : %O\nquery: %O",
      this.pathConsignation.consignationPathBackup, req.params, req.query)

    const fichiers = await getFichiersDomaine(domaine, pathRepertoireBackup)
    sortFichiers(fichiers)  // Trier fichiers pour traitement stream

    // Conserver parametres pour middleware streamListeFichiers
    res.pathRacineFichiers = pathRepertoireBackup
    res.listeFichiers = fichiers.map(item=>{
      return item.path
    })

    debug("Fichiers du backup : %O", res.listeFichiers)

    next()

  }

  async restaurerApplication(req, res, next) {
    const nomApplication = req.params.nomApplication
    const pathRepertoireApplications = this.pathConsignation.trouverPathBackupApplication(nomApplication)

    debug("restaurerApplication : Recherche catalogues sous %s", pathRepertoireApplications)

    // Trouver le catalogue le plus recent
    // Charger tous les catalogues, trier par date
    var settings = {type: 'files', fileFilter: ['*_catalogue_*.json']}

    const cataloguesPromises = await new Promise((resolve, reject)=>{
      const catalogues = []
      readdirp(pathRepertoireApplications, settings)
      .on('data', entry =>{
        const promiseReadFile = new Promise((resolve, reject)=>{
          fs.readFile(entry.fullPath, (err, data)=>{
            if(err) return reject(err)
            try {resolve(JSON.parse(data))}
            catch(err) {reject(err)}
          })
        })
        catalogues.push(promiseReadFile)
      })
      .on('error', err => reject(err))
      .on('end', _  => resolve(catalogues))
    })

    // Verifier si on a au moins un catalogue
    if(cataloguesPromises.length === 0) {
      return res.sendStatus(404)
    }

    // Attendre le chargement de tous les catalogues
    const catalogues = await Promise.all(cataloguesPromises)

    // Trier les catalogues par date
    catalogues.sort((a,b)=>{return b['en-tete'].estampille - a['en-tete'].estampille})

    const catalogueApplication = catalogues[0]  // Garder le plus recent

    const pathArchive = path.join(pathRepertoireApplications, catalogueApplication.archive_nomfichier)

    debug("restaurationBackup.RestaurateurBackup.restaurerApplication Catalogue application : %O", catalogueApplication)

    res.set({
      archive_hachage: catalogueApplication.archive_hachage,
      archive_nomfichier: catalogueApplication.archive_nomfichier,
      archive_epoch: catalogueApplication['en-tete'].estampille,
      cle: catalogueApplication.cle,
      iv: catalogueApplication.iv,
      tag: catalogueApplication.tag,
      format_chiffrage: catalogueApplication.format,
    })

    // Stream
    if(req.method === 'HEAD') {
      // Uniquement transmettre le header (info archive)
      res.sendStatus(200)
    } else {
      // Assumer que c'est un GET
      res.status(200).sendFile(pathArchive)
    }
  }

  async restaurerGrosFichiersHoraire() {
    // Effectue un hard link de tous les grosfichiers sous /horaire vers /local
    const grosFichiers = await getGrosFichiersHoraire(this.pathBackupHoraire)
    debug("Liste grosfichiers\n%O", grosFichiers)

    const listeGrosFichiers = grosFichiers.map(item=>{
      return item.fullPath
    })

    return restaurerListeGrosFichiers(listeGrosFichiers, this.pathConsignation)
  }

  async restaurerGrosFichiersArchives() {
    const pathConsignation = this.pathConsignation
    const pathBackupArchives = this.pathBackupArchives
    const pathStaging = this.pathConsignation.consignationPathBackupStaging

    var fichiersArchives = await getFichiersDomaine('GrosFichiers', pathBackupArchives, {exclureHoraire: true})

    // Eliminer les fichiers d'archives annuelles
    // fichiersArchives = fichiersArchives.filter(item=>item.typeFichier==='quotidien').map(item=>item.fullPath)
    fichiersArchives = fichiersArchives.map(item=>item.fullPath)

    // Extraire les */grosfichiers/* de chaque archive .tar et refaire le lien sous /local
    debug("Debut extraction archives quotidiens grosfichiers : %O", fichiersArchives)

    for(let idx in fichiersArchives) {
      const pathArchive = fichiersArchives[idx]
      await extraireStagingArchive(pathArchive, this.pathConsignation, _traiterArchiveStaging)
    }

    debug("restaurerGrosFichiersArchives complete")
  }

}

async function _traiterArchiveStaging(nomArchive, pathConsignation, pathExtraction) {
  debug("Lien grosfichiers %s sous %O", nomArchive, pathExtraction)

  // Verifier si l'archive est niveau annuel ou quotidien
  var splitNomArchive = nomArchive.split('_')
  var dateArchive = splitNomArchive[1]
  var niveauArchive = null

  if(dateArchive.length === 8) {
    debug("Traiter archive quotidienne extraite sous %s", pathExtraction)

    // Meme traitement que pour le repertoire horaire
    const grosFichiers = await getGrosFichiersHoraire(pathExtraction)
    debug("Liste grosfichiers dans %s\n%O", pathExtraction, grosFichiers)
    const listeGrosFichiers = grosFichiers.map(item=>{
      return item.fullPath
    })

    return restaurerListeGrosFichiers(listeGrosFichiers, pathConsignation)

  } else if(dateArchive.length === 4) {
    debug("Traiter archive annuelle extraite sous %s", pathExtraction)

    var fichiersArchives = await getFichiersDomaine('GrosFichiers', pathExtraction, {exclureHoraire: true})

    // Eliminer les fichiers d'archives annuelles
    fichiersArchives = fichiersArchives.map(item=>item.fullPath)

    // Extraire les */grosfichiers/* de chaque archive .tar et refaire le lien sous /local
    debug("Debut extraction archives quotidiens grosfichiers : %O", fichiersArchives)

    for(let idx in fichiersArchives) {
      const pathArchive = fichiersArchives[idx]
      await extraireStagingArchive(pathArchive, pathConsignation, _traiterArchiveStaging)
    }

  } else {
    throw new Error(`Niveau archive inconnu : ${nomArchive}`)
  }
}

async function getStatFichierBackup(pathFichier, aggregation) {

  const fullPathFichier = path.join(this.pathConsignation.consignationPathBackup, aggregation, pathFichier);

  const {err, size} = await new Promise((resolve, reject)=>{
    fs.stat(fullPathFichier, (err, stat)=>{
      if(err) reject({err});
      resolve({size: stat.size})
    })
  });

  if(err) throw(err);

  return {size, fullPathFichier};
}

async function mkdirs(repertoires) {
  const promises = repertoires.map(repertoire=>{
    return new Promise((resolve, reject) => {
      fs.mkdir(repertoire, {recursive: true, mode: 0o770}, err=>{
        if(err && err.code !== 'EEXIST') reject(err)
        resolve()
      })
    })
  })
  return Promise.all(promises)
}

function sortFichiers(fichiers) {
  // Trier les fichiers pour permettre un traitement direct du stream
  //   1. date backup (string compare)
  //   2. sous-domaine
  //   3. type fichier (ordre : catalogue, transaction)
  fichiers.sort((a,b)=>{
    if(a===b) return 0; if(!a) return -1; if(!b) return 1;
    const typeFichierA = a.typeFichier, typeFichierB = b.typeFichier
    const dateFichierA = a.dateFichier, dateFichierB = b.dateFichier
    const sousdomaineA = a.sousdomaine, sousdomaineB = b.sousdomaine

    var compare = 0

    // // Utiliser longueur date pour type aggregation
    // // 4=annuel, 8=quotidien, 10=horaire, 21=SNAPSHOT
    // const aggA = dateFichierA.length, aggB = dateFichierB.length
    //
    // var compare = aggA - aggB
    // if(compare !== 0) return compare

    compare = dateFichierA.localeCompare(dateFichierB)
    if(compare !== 0) return compare

    compare = sousdomaineA.localeCompare(sousdomaineB)
    if(compare !== 0) return compare

    compare = typeFichierA.localeCompare(typeFichierB)
    if(compare !== 0) return compare

    return compare
  })
}

async function restaurerListeGrosFichiers(listeGrosFichiers, pathConsignation) {

  // Caculer le path pour chaque fichier (avec le fuuid) puis faire un
  // hard link ou le copier si le fichier n'existe pas deja
  for(let idx in listeGrosFichiers) {
    const fullPath = listeGrosFichiers[idx]

    const basename = path.basename(fullPath),
          extname = path.extname(fullPath)

    const chiffre = extname === '.mgs3',
          extension = extname.slice(1)
    const nomfichierSansExtension = basename.replace(extname, '')
    const pathFichier = pathConsignation.trouverPathLocal(nomfichierSansExtension, chiffre, {extension})
    const basedir = path.dirname(pathFichier)

    debug("Path fichier : %s", pathFichier)

    // Creer hard link ou copier fichier
    await new Promise((resolve, reject)=>{
      fs.mkdir(basedir, { recursive: true, mode: 0o770 }, (err)=>{
        if(err) return reject(err)
        fs.link(fullPath, pathFichier, e=>{
          if(e && e.code !== 'EEXIST') {
            return reject(e)
          }
          resolve()
        })
      })
    })
    .catch(err=>{
      console.error("Erreur link grosfichier backup : %s\n%O", fullPath, err)
    })
  }

}

function extraireStagingArchive(pathArchive, pathConsignation, cb) {
  // Extrait une archive tar dans un repertoire, appelle le callback cb(tmpStagingDir) puis nettoie le staging
  const pathStaging = pathConsignation.consignationPathBackupStaging

  return new Promise((resolve, reject)=>{

    const nomArchive = path.basename(pathArchive, '.tar')
    const pathStagingArchive = path.join(pathStaging, nomArchive)

    fs.mkdir(pathStagingArchive, {recursive: true, mode: 0o770}, async err => {
      if(err) throw err
      try {
        if(err) throw err

        await tar.x({
          file: pathArchive,
          C: pathStagingArchive,
        })

        // Appeler le callback avec repertoire temporaire, attendre traitement
        await cb(nomArchive, pathConsignation, pathStagingArchive)

        resolve()
      } catch(err) {
        reject(err)
      } finally {
        fs.rmdir(pathStagingArchive, {recursive: true}, err=>{
          if(err) console.error("extraireStagingArchive: Erreur nettoyage repertoire de staging : %O", err)
        })
      }
    })
  })
}

module.exports = { RestaurateurBackup };
