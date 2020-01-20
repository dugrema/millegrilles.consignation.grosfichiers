const fs = require('fs');
const tmp = require('tmp-promise');
const im = require('imagemagick');
const FFmpeg = require('fluent-ffmpeg');

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
    } finally {
      // Effacer le fichier temporaire
      o.cleanup();
    }
  })

  return base64Content;
}

async function genererThumbnailVideo(sourcePath, opts) {
  // Preparer fichier destination decrypte
  // Aussi preparer un fichier tmp pour le thumbnail
  var base64Content;
  await tmp.file({ mode: 0o600, postfix: '.jpg' }).then(async o => {

    try {
      const previewPath = o.path;
      // Extraire une image du video
      await genererPreviewVideoPromise(sourcePath, previewPath);

      // Prendre le preview genere et creer le thumbnail
      await tmp.file({ mode: 0o600, postfix: '.jpg' }).then(async tbtmp => {
        const thumbnailPath = tbtmp.path;

        try {
          await _imConvertPromise([previewPath, '-thumbnail', '200x150>', '-quality', '50', thumbnailPath]);

          // Lire le fichier converti en memoire pour transformer en base64
          base64Content = new Buffer.from(await fs.promises.readFile(thumbnailPath)).toString("base64");
        } finally {
          tbtmp.cleanup();
        }

      })

    } finally {
      // Effacer le fichier temporaire
      o.cleanup();
    }
  })

  return base64Content;
}

async function genererPreview(sourcePath, destinationPath, opts) {
  await _imConvertPromise([sourcePath+'[0]', '-resize', '720x540>', destinationPath]);
}

function _imConvertPromise(params) {
  return new Promise((resolve, reject) => {
    im.convert(params,
      function(err, stdout){
        if (err) reject(err);
        resolve();
      });
  });
}

async function genererPreviewVideoPromise(sourcePath, previewPath) {
  await new Promise((resolve, reject) => {
    new FFmpeg({ source: sourcePath })
      .on('error', function(err) {
          console.error('An error occurred: ' + err.message);
          reject(err);
      })
      .on('end', function(filenames) {
        console.log('Successfully generated thumbnail ' + previewPath);
        resolve();
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

module.exports = {genererThumbnail, genererThumbnailVideo, genererPreview, genererPreviewVideoPromise}
