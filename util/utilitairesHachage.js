const crypto = require('crypto')
const fs = require('fs')

async function calculerHachageFichier(pathFichier, opts) {
  if(!opts) opts = {};

  let fonctionHash = opts.fonctionHash || 'sha512'
  fonctionHash = fonctionHash.split('_')[0]  // Enlever _b64 si present

  // Calculer SHA512 sur fichier de backup
  const sha = crypto.createHash(fonctionHash);
  const readStream = fs.createReadStream(pathFichier);

  const resultatSha = await new Promise(async (resolve, reject)=>{
    readStream.on('data', chunk=>{
      sha.update(chunk);
    })
    readStream.on('end', ()=>{
      const resultat = sha.digest('base64');
      resolve({sha: fonctionHash + '_b64:' + resultat})
    });
    readStream.on('error', err=> {
      reject({err});
    });

    readStream.read();
  })

  if(resultatSha.err) {
    throw resultatSha.err;
  } else {
    return resultatSha.sha;
  }
}

module.exports = { calculerHachageFichier }
