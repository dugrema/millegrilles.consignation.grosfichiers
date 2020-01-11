const fs = require('fs');
const tmp = require('tmp-promise');
const im = require('imagemagick');

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

async function genererPreview(sourcePath, destinationPath, opts) {
  await _imConvertPromise([sourcePath+'[0]', '-resize', '720x540>', destinationPath]);
}

function _imConvertPromise(params) {
  return new Promise((resolve, reject) => {
    im.convert(params,
      function(err, stdout){
        if (err) reject(err);
        console.log('stdout:', stdout);
        resolve();
      });
  });
}

module.exports = {genererThumbnail, genererPreview}
