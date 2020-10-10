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

const { formatterDateString } = require('millegrilles.common/lib/js_formatters')
const {uuidToDate} = require('./UUIDUtils')
const transformationImages = require('./transformationImages')
const {pki, ValidateurSignature} = require('./pki')
const { calculerHachageFichier } = require('./utilitairesHachage')
const {PathConsignation, extraireTarFile, supprimerFichiers, getFichiersDomaine} = require('./traitementFichier')
const {traiterFichiersBackup, traiterGrosfichiers, traiterFichiersApplication} = require('./processFichiersBackup')

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

    const timestampBackup = new Date(req.body.timestamp_backup * 1000);
    if(!timestampBackup) {return reject("Il manque le timestamp du backup");}

    let pathRepertoire = pathConsignation.trouverPathBackupHoraire(timestampBackup)
    let fichiersTransactions = req.files.transactions;
    let fichierCatalogue = req.files.catalogue[0];

    // Deplacer les fichiers de backup vers le bon repertoire /backup
    const fichiersDomaines = await traiterFichiersBackup(fichiersTransactions, fichierCatalogue, pathRepertoire)

    // Transmettre cles du fichier de transactions
    if(req.body.transaction_maitredescles) {
      const transactionMaitreDesCles = JSON.parse(req.body.transaction_maitredescles)
      debug("Transmettre cles du fichier de transactions : %O", transactionMaitreDesCles)
      this.rabbitMQ.transmettreEnveloppeTransaction(transactionMaitreDesCles)
    }

    // Creer les hard links pour les grosfichiers
    const fuuidDict = JSON.parse(req.body.fuuid_grosfichiers)
    if(fuuidDict && Object.keys(fuuidDict).length > 0) {
      await traiterGrosfichiers(pathConsignation, pathRepertoire, fuuidDict)
    }

    return {fichiersDomaines}
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

  async sauvegarderJournalQuotidien(journal) {
    const {domaine, securite, jour} = journal

    const dateJournal = new Date(jour*1000)
    var repertoireBackup = this.pathConsignation.trouverPathBackupHoraire(dateJournal)

    // Remonter du niveau heure a jour
    repertoireBackup = path.dirname(repertoireBackup);

    const dateFormattee = formatterDateString(dateJournal).slice(0, 8)  // Retirer heures
    const nomFichier = domaine + "_catalogue_" + dateFormattee + "_" + securite + ".json.xz"
    const fullPathFichier = path.join(repertoireBackup, nomFichier)

    // debug("Path fichier journal quotidien " + fullPathFichier);
    var compressor = lzma.createCompressor()
    var output = fs.createWriteStream(fullPathFichier)
    compressor.pipe(output)

    const promiseSauvegarde = new Promise((resolve, reject)=>{
      output.on('close', ()=>{resolve()})
      output.on('error', err=>{reject(err)})
    })

    compressor.write(JSON.stringify(journal))
    compressor.end()
    await promiseSauvegarde

    // debug("Fichier cree : " + fullPathFichier);
    return {path: fullPathFichier, nomFichier, dateFormattee}
  }

  // async sauvegarderJournalAnnuel(journal) {
  //   const {domaine, securite, annee} = journal
  //
  //   const dateJournal = new Date(annee*1000)
  //   var repertoireBackup = this.pathConsignation.consignationPathBackupArchives
  //
  //   let year = dateJournal.getUTCFullYear();
  //   const dateFormattee = "" + year;
  //
  //   const nomFichier = domaine + "_catalogue_" + dateFormattee + "_" + securite + ".json.xz";
  //
  //   const fullPathFichier = path.join(repertoireBackup, 'quotidiennes', domaine, nomFichier);
  //
  //   // debug("Path fichier journal mensuel " + fullPathFichier);
  //   var compressor = lzma.createCompressor();
  //   var output = fs.createWriteStream(fullPathFichier);
  //   compressor.pipe(output);
  //
  //   const promiseSauvegarde = new Promise((resolve, reject)=>{
  //     output.on('close', ()=>{
  //       resolve();
  //     });
  //     output.on('error', err=>{
  //       reject(err);
  //     })
  //   });
  //
  //   compressor.write(JSON.stringify(journal));
  //   compressor.end();
  //   await promiseSauvegarde;
  //
  //   const sha512Journal = await calculerHachageFichier(fullPathFichier);
  //
  //   // debug("Fichier cree : " + fullPathFichier);
  //   return {pathJournal: fullPathFichier, hachage: sha512Journal, dateFormattee};
  // }

  // async getStatFichierBackup(pathFichier, aggregation) {
  //
  //   const fullPathFichier = path.join(this.pathConsignation.consignationPathBackup, aggregation, pathFichier);
  //
  //   const {err, size} = await new Promise((resolve, reject)=>{
  //     fs.stat(fullPathFichier, (err, stat)=>{
  //       if(err) reject({err});
  //       resolve({size: stat.size})
  //     })
  //   });
  //
  //   if(err) throw(err);
  //
  //   return {size, fullPathFichier};
  // }

  // async getFichierTarBackupComplet(req, res) {
  //
  //   const tmpFichierBackup = await tmp.file({mode:0o600, prefix: 'backup-download-', postfix: '.tar'})
  //   const fichierBackup = tmpFichierBackup.path
  //   debug("Fichier TMP backup\n%s", fichierBackup)
  //
  //   const dateFormattee = moment().format('YYYY-MM-DD_hhmm')
  //   const idmg = req.autorisationMillegrille.idmg
  //   const downloadFileName = 'backup_' + idmg + '_' + dateFormattee + '.tar'
  //
  //   const pathBackup = this.pathConsignation.consignationPathBackup
  //
  //   // Creer une archive .tar de backup avec les repertoires de fichiers
  //   // horaire, archive et instantanne
  //   debug("Creer archive utilisant path %s", pathBackup)
  //
  //   // Trouver les repertoires existants pour ajouter au tar
  //   const repertoires = ['horaire', 'archives', 'instantanne']
  //
  //   try {
  //
  //     // Faire la liste des sous-repertoires a filtrer
  //     const files = await new Promise((resolve, reject)=>{
  //       fs.readdir(pathBackup, (err, files)=>{
  //         if(err) reject(err)
  //         files.filter(item=>{
  //           return repertoires.includes(item)
  //         })
  //         resolve(files)
  //       })
  //     })
  //
  //     await tar.create( // or tar.create
  //       {
  //         cwd: pathBackup,
  //         file: fichierBackup,
  //       },
  //       files
  //     )
  //
  //     // Creer outputstream de reponse
  //     res.set('Content-Type', 'application/tar')
  //     res.set('Content-Disposition', 'attachment; filename="' + downloadFileName + '"')
  //     res.status(200)
  //
  //     const readStream = fs.createReadStream(fichierBackup)
  //
  //     await new Promise((resolve, reject)=>{
  //       readStream.on('close', _=>{
  //         resolve()
  //       })
  //       readStream.on('error', err=>{
  //         console.error("Erreur transfert\n%O", err)
  //         reject(err)
  //       })
  //       readStream.pipe(res)
  //     })
  //
  //   } catch(err) {
  //     console.error("Erreur traitement TAR file\n%O", err)
  //     res.sendStatus(500)
  //   } finally {
  //     fs.unlink(fichierBackup, err=>{
  //       if(err) console.error("Erreur unlink " + fichierBackup)
  //     })
  //     res.end('')  // S'assurer que le stream est ferme
  //   }
  //
  // }

}

async function getListeDomaines(req, res, next) {
  debug("Retourner la liste des domaines avec au moins un fichier de backup disponible")

  try {
    debug("Path consignation : %O", req.pathConsignation)
    const domaines = await identifierDomaines(req.pathConsignation.consignationPathBackup)
    res.status(200).send({domaines})
  } catch(err) {
    console.error("getListeDomaines: Erreur\n%O", err)
    res.sendStatus(500)
  }

}

async function identifierDomaines(pathRepertoireBackup) {

  const fichiers = await getFichiersDomaine('*', pathRepertoireBackup)

  // Extraire les sous domaines du nom des fichiers, grouper
  var domaines = fichiers.map(item=>{
    const nameSplit = item.basename.split('_')
    const domaine = nameSplit[0]
    return domaine
  }).reduce((dict, item)=>{
    dict[item] = true
    return dict
  }, {})

  // debug("Liste fichiers de backup : %O\nDomaines : %O", fichiers, domaines)
  domaines = Object.keys(domaines)
  debug("Domaines de backup : %O", domaines)

  return domaines
}

module.exports = {TraitementFichierBackup, formatterDateString, getListeDomaines};
