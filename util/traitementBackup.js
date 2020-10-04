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
const {PathConsignation, extraireTarFile, supprimerFichiers} = require('./traitementFichier')
const {traiterFichiersBackup, traiterGrosfichiers} = require('./processFichiersBackup')

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

  async genererListeBackupsHoraire(req) {

    const pathConsignation = new PathConsignation({idmg: req.autorisationMillegrille.idmg})

    const domaine = req.body.domaine;
    const pathRepertoire = path.join(pathConsignation.consignationPathBackup, 'horaire');
    // debug("Path repertoire backup");
    // debug(pathRepertoire);

    const prefixeCatalogue = domaine + "_catalogue";
    const prefixeTransactions = domaine + "_transactions";

    var settings = {
      type: 'files',
      fileFilter: [
        prefixeCatalogue + '_*.json.xz',
        prefixeTransactions + '_*.jsonl.xz',
        prefixeTransactions + '_*.jsonl.xz.mgs1',
      ],
    }

    const {err, backupsHoraire} = await new Promise((resolve, reject)=>{
      // const fichiersCatalogue = [];
      // const fichiersTransactions = [];

      const backupsHoraire = {};

      readdirp(
        pathRepertoire,
        settings,
      )
      .on('data', entry=>{
        // debug(entry);

        const heureBackup = entry.path.split('/').slice(0, 4).join('');
        var entreeBackup = backupsHoraire[heureBackup];
        if(!entreeBackup) {
          entreeBackup = {};
          backupsHoraire[heureBackup] = entreeBackup;
        }

        if(entry.basename.startsWith(prefixeCatalogue)) {
          entreeBackup.catalogue = entry.path;
        } else if(entry.basename.startsWith(prefixeTransactions)) {
          entreeBackup.transactions = entry.path;
        }
      })
      .on('error', err=>{
        reject({err});
      })
      .on('end', ()=>{
        // debug("Fini");
        resolve({backupsHoraire});
      });

    });

    if(err) throw err;

    // Trier les catalgues et transactions par date (tri naturel)
    // catalogues.sort();
    // transactions.sort();

    // return {catalogues, transactions};
    return {backupsHoraire};

  }

  async getStatFichierBackup(pathFichier, aggregation) {

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

  // async sauvegarderJournalQuotidien(journal) {
  //   const {domaine, securite, jour} = journal;
  //
  //   const dateJournal = new Date(jour*1000);
  //   var repertoireBackup = this.pathConsignation.trouverPathBackupHoraire(dateJournal);
  //   // Remonter du niveau heure a jour
  //   repertoireBackup = path.dirname(repertoireBackup);
  //
  //   const dateFormattee = formatterDateString(dateJournal).slice(0, 8)  // Retirer heures
  //
  //   const nomFichier = domaine + "_catalogue_" + dateFormattee + "_" + securite + ".json.xz";
  //
  //   const fullPathFichier = path.join(repertoireBackup, nomFichier);
  //
  //   // debug("Path fichier journal quotidien " + fullPathFichier);
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
  //   // debug("Fichier cree : " + fullPathFichier);
  //   return {path: fullPathFichier, nomFichier, dateFormattee};
  // }

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

module.exports = {TraitementFichierBackup, formatterDateString};
