const crypto = require('crypto')
const fs = require('fs')

async function calculerHachageFichier(pathFichier, opts) {
  if(!opts) opts = {};

  const readStream = fs.createReadStream(pathFichier)

  // Calculer hachage sur fichier
  return calculerHachageStream(readStream, opts)
}

async function calculerHachageStream(readStream, opts) {
  opts = opts || {}

  let fonctionHash = opts.fonctionHash || 'sha512'
  fonctionHash = fonctionHash.split('_')[0]  // Enlever _b64 si present
  const sha = crypto.createHash(fonctionHash)

  return new Promise(async (resolve, reject)=>{
    readStream.on('data', chunk=>{
      sha.update(chunk)
    })
    readStream.on('end', ()=>{
      const resultat = sha.digest('base64')
      resolve(fonctionHash + '_b64:' + resultat)
    })
    readStream.on('error', err=> {
      reject(err)
    })

    if(readStream.read) readStream.read()
    else readStream.resume()
  })
}

function calculerHachageData(data, opts) {
  if(!opts) opts = {}
  let fonctionHash = opts.fonctionHash || 'sha512'
  fonctionHash = fonctionHash.split('_')[0]  // Enlever _b64 si present

  // Calculer SHA512 sur fichier de backup
  const sha = crypto.createHash(fonctionHash);
  sha.update(data)

  const digest = sha.digest('base64')
  return fonctionHash + '_b64:' + digest
}

module.exports = { calculerHachageFichier, calculerHachageData, calculerHachageStream }
