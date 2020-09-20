const fs = require('fs');
const tmp = require('tmp-promise');
const im = require('imagemagick');
const FFmpeg = require('fluent-ffmpeg');
const crypto = require('crypto');

async function genererThumbnail(sourcePath, opts) {
  // Preparer fichier destination decrypte
  // Aussi preparer un fichier tmp pour le thumbnail
  var base64Content;
  await tmp.file({ mode: 0o600, postfix: '.jpg' }).then(async o => {

    try {
      const thumbnailPath = o.path;
      // Convertir l'image - si c'est un gif anime, le fait de mettre [0]
      // prend la premiere image de l'animation pour faire le thumbnail.
      await _imConvertPromise([sourcePath+'[0]', '-thumbnail', '200x150>', '-quality', '50', thumbnailPath]);

      // Lire le fichier converti en memoire pour transformer en base64
      base64Content = new Buffer.from(await fs.promises.readFile(thumbnailPath)).toString("base64");
      // console.debug("Thumbnail b64 genere")
    } catch(err) {
      console.error("Erreur creation thumbnail : %O", err)
    } finally {
      // Effacer le fichier temporaire
      o.cleanup()
    }
  })

  return base64Content;
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
  await _imConvertPromise([sourcePath+'[0]', '-resize', '720x540>', '-quality', '50', destinationPath]);
}

function _imConvertPromise(params) {
  return new Promise((resolve, reject) => {
    // console.debug("Conversion")
    // console.debug(params)
    im.convert(params,
      function(err, stdout){
        if (err) reject(err);
        resolve();
      });
  });
}

async function genererPreviewVideo(sourcePath, previewPath) {
  var dataVideo;
  return await new Promise((resolve, reject) => {
    new FFmpeg({ source: sourcePath })
      .on('error', function(err) {
          console.error('An error occurred: ' + err.message);
          reject(err);
      })
      .on('codecData', data => {
        dataVideo = data;
      })
      .on('end', function(filenames) {
        // console.debug('Successfully generated thumbnail ' + previewPath);
        // console.debug(dataVideo);
        resolve({data_video: dataVideo});
      })
      .takeScreenshots(
        {
          count: 1,
          filename: previewPath
        },
        '/'
      );
  });
}

async function genererVideoMp4_480p(sourcePath, destinationPath) {
  return await new Promise((resolve, reject) => {
    new FFmpeg({source: sourcePath})
      .withVideoBitrate('2500k')
      .withSize('?x480')
      .on('error', function(err) {
          console.error('An error occurred: ' + err.message);
          reject(err);
      })
      .on('end', function(filenames) {

        let shasum = crypto.createHash('sha256');
        try {
          let s = fs.ReadStream(destinationPath)
          let tailleFichier = 0;
          s.on('data', data => {
            shasum.update(data)
            tailleFichier += data.length;
          })
          s.on('end', function () {
            const sha256 = shasum.digest('hex')
            // console.debug('Successfully generated 480p mp4 ' + destinationPath + ", taille " + tailleFichier + ", sha256 " + sha256);
            return resolve({tailleFichier, sha256});
          })
        } catch (error) {
          return reject(error);
        }

      })
      .saveToFile(destinationPath);
  });
}

module.exports = {
  genererThumbnail, genererThumbnailVideo, genererPreview, genererPreviewImage,
  genererPreviewVideo, genererVideoMp4_480p
}
