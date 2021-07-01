const debug = require('debug')('millegrilles:fichiers:transformationImages')
const fs = require('fs')
const path = require('path')
const tmp = require('tmp-promise')
const im = require('imagemagick')
const crypto = require('crypto')
const multibase = require('multibase')
const FFmpeg = require('fluent-ffmpeg')

async function preparerBase64(sourcePath, opts) {
  // Lire le fichier converti en memoire pour transformer en base64
  const fichierBuffer = Buffer.from(await fs.promises.readFile(sourcePath))
  const base64Content = String.fromCharCode.apply(null, multibase.encode('base64', fichierBuffer))
  return base64Content
}

async function genererThumbnailVideo(sourcePath, opts) {
  // Preparer fichier destination decrypte
  // Aussi preparer un fichier tmp pour le thumbnail
  var base64Content, metadata;
  await tmp.file({ mode: 0o600, postfix: '.jpg' }).then(async o => {

    try {
      const previewPath = o.path;
      // Extraire une image du video
      metadata = await genererPreviewVideoPromise(sourcePath, previewPath);

      // Prendre le preview genere et creer le thumbnail
      await tmp.file({ mode: 0o600, postfix: '.jpg' }).then(async tbtmp => {
        const thumbnailPath = tbtmp.path;

        try {
          await _imConvertPromise([previewPath, '-thumbnail', '200x150>', '-quality', '50', thumbnailPath]);

          // Lire le fichier converti en memoire pour transformer en base64
          base64Content = new Buffer.from(await fs.promises.readFile(thumbnailPath)).toString("base64");
        } catch(err) {
          console.error("Erreur creation thumbnail video")
          console.error(err);
        } finally {
          tbtmp.cleanup();
        }

      })

    } catch(err) {
      console.error("Erreur creation thumbnail video")
      console.error(err);
    } finally {
      // Effacer le fichier temporaire
      o.cleanup();
    }
  })

  return {base64Content, metadata};
}

async function genererPreview(sourcePath, destinationPath, opts) {
  await _imConvertPromise([sourcePath+'[0]', '-resize', '720x540>', destinationPath]);
}

async function genererPreviewImage(sourcePath, destinationPath, opts) {
  debug("genererPreviewImage avec %s", sourcePath)
  // const b64Thumbnail = await genererThumbnail(sourcePath)
  //debug("Thumbnail genere en base64\n%s", b64Thumbnail)
  const {mq, chiffrerTemporaire, deplacerVersStorage, clesPubliques, pathConsignation} = opts

  const {metadataImage, nbFrames, conversions} = await determinerConversionsImages(sourcePath)
  debug("Information de conversion d'images : %O", conversions)

  const promisesChiffrage = []

  // Effectuer les conversions pour tous les formats
  for(let cle in conversions) {
    const cp = conversions[cle]
    debug("Executer conversion %s, %O", cle, cp)
    // Creer fichier temporaire avec la bonne extension
    const fichierTmp = await tmp.file({ mode: 0o600, postfix: '.' + cp.ext })
    const paramsConversion = [sourcePath+'[0]', ...cp.params, fichierTmp.path]
    try {
      await _imConvertPromise(paramsConversion)
    } catch(err) {
      fichierTmp.cleanup()  // Supprimer fichier tmp non chiffre
      throw err
    }

    // Creer promise pour continuer le traitement de chiffrage
    const fichierChiffreTmp = await tmp.file({ mode: 0o600, postfix: '.mgs2' })
    const promiseChiffrage = readIdentify(fichierTmp.path).then(async metaConversion=>{
      // Recuperer information image convertie
      debug("Information meta image convertie params %O : %O", cp, metaConversion)

      // Chiffrer le resultat, conserver information pour transactions maitre des cles
      const resultatChiffrage = await opts.chiffrerTemporaire(
        mq, fichierTmp.path, fichierChiffreTmp.path, clesPubliques, {
          identificateurs_document: {type: 'image', fuuid_reference: opts.fuuid}
        })

      let data = null
      if(cle === 'thumb') {
        // Traitement special pour thumbnail, on l'insere inline
        data = await preparerBase64(fichierChiffreTmp.path)
        fichierChiffreTmp.cleanup()  // Supprimer fichier tmp
      } else {
        // Deplacer le fichier vers le stockage permanent
        await deplacerVersStorage(pathConsignation, resultatChiffrage, fichierChiffreTmp)
      }

      const informationImage = {
        cle,
        hachage: resultatChiffrage.meta.hachage_bytes,
        width: metaConversion.width,
        height: metaConversion.height,
        mimetype: metaConversion['mime type'],
        taille: resultatChiffrage.tailleFichier,
        resolution: cp.resolution,
      }
      if(data) informationImage.data_chiffre = data

      // Transmettre transaction info chiffrage
      const domaineActionCles = 'MaitreDesCles.sauvegarderCle'
      const commandeMaitreCles = resultatChiffrage.commandeMaitreCles
      // commandeMaitreCles.identificateurs_document = {
      //   attachement_fuuid: message.fuuid,
      //   type: 'preview',
      // }
      await mq.transmettreCommande(domaineActionCles, commandeMaitreCles)

      return {
        metaConversion,
        informationImage,
        ...resultatChiffrage,
      }

    })
    .catch(err => {
      fichierChiffreTmp.cleanup()
      throw err
    })
    .finally(_=>{
      fichierTmp.cleanup()  // Supprimer fichier tmp non chiffre
    })
    promisesChiffrage.push(promiseChiffrage)

  }

  const chiffrageComplete = await Promise.all(promisesChiffrage)
  debug("Information de chiffrage complete : %O", chiffrageComplete)

  // await _imConvertPromise([sourcePath+'[0]', '-resize', '720x540>', '-quality', '50', destinationPath]);

  return {metadataImage, nbFrames, conversions: chiffrageComplete}
}

async function determinerConversionsImages(sourcePath) {
  let ratioInverse = false,
      operationResize = '>',
      quality = '86',
      valRef = null

  let metadataImage = null, nbFrames = null
  try {
    nbFrames = await readIdentifyFrames(sourcePath)
    debug("Image nb frames : %d", nbFrames)

    metadataImage = await readIdentify(sourcePath)
    debug("Metadata image chargee : %O", metadataImage)

    const {width, height} = metadataImage
    const mimetype = metadataImage['mime type']
    quality = metadataImage.quality
    if(quality && quality < 0.86) {
      quality = ''+Math.round(quality * 100)
    } else {
      quality = '86'
    }

    if(width < height) {
      // Ratio inverse
      ratioInverse = true
      const ratio = width / height
      if(ratio < (9/16)) {
        // Image tres longue, on va inverser le resize (^ plutot que >) pour garder suffisamment de detail
        operationResize = '^'
      }
    }

    // Definir valeur de reference pour la resolution (selon le ratio)
    valRef = ratioInverse?width:height

  } catch(err) {
    debug("Erreur preparation image, aucune meta-information : %O", err)
    const info = await readMetadata(sourcePath)
    debug("Information metadata exif : %O", info)
  }

  const conversions = {
    // Thumbnail : L'image est ramenee sur 128px, et croppee au milieu pour ratio 1:1
    'thumb': {ext: 'jpg', resolution: 128, params: ['-strip', '-resize', '128x128^', '-gravity', 'center', '-extent', '128x128', '-quality', '25']},
    // Poster, utilise pour afficher dans un coin d'ecran/preview
    'poster': {ext: 'jpg', resolution: 240, params: ['-strip', '-resize', ratioInverse?'240x420>':'420x240>', '-quality', '60']},
  }

  if(valRef >= 1080) {
    conversions['image/webp;1080'] = {ext: 'webp', resolution: 1080, params: ['-strip', '-resize', ratioInverse?'1080x1920'+operationResize:'1920x1080'+operationResize, '-quality', quality]}
  }
  if(valRef >= 720) {
    conversions['image/webp;720'] = {ext: 'webp', resolution: 720, params: ['-strip', '-resize', ratioInverse?'720x1280'+operationResize:'1280x720'+operationResize, '-quality', quality]}
  }
  if(valRef >= 480) {
    conversions['image/webp;480'] = {ext: 'webp', resolution: 480, params: ['-strip', '-resize', ratioInverse?'480x854'+operationResize:'854x480'+operationResize, '-quality', quality]}
    conversions['image/jpeg;480'] = {ext: 'jpg', resolution: 480, params: ['-strip', '-resize', ratioInverse?'480x854'+operationResize:'854x480'+operationResize, '-quality', quality]}
  }

  return {metadataImage, nbFrames, conversions}
}


function _imConvertPromise(params) {
  return new Promise((resolve, reject) => {
    console.debug("Conversion")
    console.debug(params)
    im.convert(params,
      function(err, stdout){
        if (err) reject(err);
        resolve();
      });
  });
}

async function genererPreviewVideo(sourcePath, previewPath) {
  var dataVideo;
  return new Promise((resolve, reject) => {
    debug("Extraire preview du video %s vers %s", sourcePath, previewPath)

    // S'assurer d'avoir un .jpg, c'est ce qui indique au convertisseur le format de sortie
    var nomFichierDemande = path.basename(previewPath)
    var nomFichierPreview = nomFichierDemande
    if( ! nomFichierPreview.endsWith('.jpg') ) {
      nomFichierPreview += '.jpg'
    }
    var folderPreview = path.dirname(previewPath)

    debug("Fichier preview demande %s, temporaire : %s", nomFichierDemande, nomFichierPreview)

    new FFmpeg({ source: sourcePath, priority: 10, })
      .on('error', function(err) {
          console.error('An error occurred: ' + err.message);
          reject(err);
      })
      .on('codecData', data => {
        dataVideo = data;
      })
      .on('progress', progress=>{
        debug("Progress : %O", progress)
      })
      .on('end', filenames => {
        debug('Successfully generated thumbnail %s, filenames : %O ', previewPath, filenames);

        debug("Copie de %s", nomFichierPreview)
        if(nomFichierPreview !== nomFichierDemande) {
          // Rename
          fs.rename(path.join(folderPreview, nomFichierPreview), previewPath, err=>{
            if(err) return reject(err)
            resolve({data_video: dataVideo})
          })
        } else {
          resolve({data_video: dataVideo})
        }
      })
      .takeScreenshots(
        {
          count: 1,
          timestamps: ['2%'],   // Prendre snapshot a 2% du debut du video
          filename: nomFichierPreview,
          folder: folderPreview,
          size: '640x?',
        },
        '/'
      );
  });
}

async function genererVideoMp4_480p(sourcePath, destinationPath, opts) {
  if(!opts) opts = {}
  const bitrate = opts.bitrate || '1800k'
        height = opts.height || '480'
  return await new Promise((resolve, reject) => {
    new FFmpeg({source: sourcePath})
      .withVideoBitrate(bitrate)
      .withSize('?x' + height)
      .on('error', function(err) {
          console.error('An error occurred: ' + err.message);
          reject(err);
      })
      .on('end', function(filenames) {

        // let shasum = crypto.createHash('sha256');
        try {
          let s = fs.ReadStream(destinationPath)
          let tailleFichier = 0;
          s.on('data', data => {
            // shasum.update(data)
            tailleFichier += data.length;
          })
          s.on('end', function () {
            // const sha256 = shasum.digest('hex')
            // console.debug('Successfully generated 480p mp4 ' + destinationPath + ", taille " + tailleFichier + ", sha256 " + sha256);
            // return resolve({tailleFichier, sha256});
            return resolve({tailleFichier, bitrate, height})
          })
        } catch (error) {
          return reject(error);
        }

      })
      .saveToFile(destinationPath);
  });
}

function readIdentify(filepath) {
  return new Promise((resolve, reject)=>{
    im.identify(filepath + '[0]', (err, metadata)=>{
      if(err) return reject(err)
      resolve(metadata)
    })
  })
}

function readIdentifyFrames(filepath) {
  /* Detecter nombre de frames (images animees) */
  return new Promise((resolve, reject)=>{
    im.identify(['-format', '%n;', filepath], (err, info)=>{
      if(err) return reject(err)
      try {
        resolve(Number(info.split(';')[0]))
      } catch(err) {
        return resolve(1)
      }
    })
  })
}

module.exports = {
  genererThumbnailVideo, genererPreview, genererPreviewImage,
  genererPreviewVideo, genererVideoMp4_480p
}
