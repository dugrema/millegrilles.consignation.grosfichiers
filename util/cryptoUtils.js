const debug = require('debug')('millegrilles:fichiers:cryptoUtils')
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const forge = require('node-forge');

const AES_ALGORITHM = 'aes-256-cbc';  // Meme algorithme utilise sur MG en Python
const RSA_ALGORITHM = 'RSA-OAEP';

class Decrypteur {

  decrypter(sourceCryptee, destination, cleSecreteDecryptee, iv, opts) {
    if(!opts) opts = {}
    return new Promise((resolve, reject)=>{
      let cryptoStream = getDecipherPipe4fuuid(cleSecreteDecryptee, iv, opts);

      // console.log("Decryptage fichier " + sourceCryptee + " vers " + destination);
      let writeStream = fs.createWriteStream(destination);

      // Calculer taille et sha256 du fichier decrypte. Necessaire pour transaction.
      const sha512 = crypto.createHash('sha512');
      var sha512Hash = null;
      var tailleFichier = 0;
      var ivLu = false
      cryptoStream.on('data', chunk=>{
        if(!ivLu) {
          ivLu = true

          // Verifier le iv
          const ivExtrait = chunk.slice(0, 16).toString('base64')
          if(ivExtrait !== iv) {
            throw new Error('IV ne correspond pas')
          }

          chunk = chunk.slice(16)
        }

        sha512.update(chunk);
        tailleFichier = tailleFichier + chunk.length;
        writeStream.write(chunk)
      });
      cryptoStream.on('end', ()=>{
        // Comparer hash a celui du header
        sha512Hash = 'sha512_b64:' + sha512.digest('base64')
        // console.debug("Hash fichier " + sha256Hash);
        writeStream.close()
      });

      // , {
      //   transform: (chunk, encoding, cb)=>{
      //     console.debug("Chunk! len: " + chunk.length)
      //     cb(null, chunk)
      //   }
      // }

      writeStream.on('close', ()=>{
        // console.debug("Fermeture fichier decrypte");
        resolve({tailleFichier, sha512Hash});
      });
      writeStream.on('error', ()=>{
        console.error("Erreur decryptage fichier");
        reject();
      });

      // Ouvrir et traiter fichier
      let readStream = fs.createReadStream(sourceCryptee);
      readStream.pipe(cryptoStream);

      // cryptoStream.pipe(writeStream);
      readStream.read()

    });
  }

}

function getDecipherPipe4fuuid(cleSecrete, iv, opts) {
  if(!opts) opts = {}
  // On prepare un decipher pipe pour decrypter le contenu.

  let ivBuffer = Buffer.from(iv, 'base64');
  // console.debug("IV (" + ivBuffer.length + "): ");
  // console.debug(iv);

  let decryptedSecretKey;
  if(typeof cleSecrete === 'string') {
    // decryptedSecretKey = Buffer.from(forge.util.binary.hex.decode(decryptedSecretKey));
    if( opts.cleFormat !== 'hex' ) {
      decryptedSecretKey = Buffer.from(cleSecrete, 'base64');
      decryptedSecretKey = decryptedSecretKey.toString('utf8');
    } else {
      decryptedSecretKey = cleSecrete
    }

    // debug("**** DECRYPTED SECRET KEY **** : %O", decryptedSecretKey)
    var typedArray = new Uint8Array(decryptedSecretKey.match(/[\da-f]{2}/gi).map(function (h) {
     return parseInt(h, 16)
    }));

    decryptedSecretKey = typedArray;
  } else {
    decryptedSecretKey = cleSecrete;
  }

  // console.debug("Cle secrete decryptee (" + decryptedSecretKey.length + ") bytes");

  // Creer un decipher stream
  var decipher = crypto.createDecipheriv('aes256', decryptedSecretKey, ivBuffer);

  return decipher;

}

function decrypterSymmetrique(contenuCrypte, cleSecrete, iv) {
  return new Promise((resolve, reject)=>{

    // console.debug("Params decrypteSymmetrique")
    // console.debug(contenuCrypte);
    // console.debug(cleSecrete);
    // console.debug(iv);

    // Dechiffrage avec Crypto
    // let cleSecreteBuffer = Buffer.from(cleSecrete, 'base64');
    // let ivBuffer = Buffer.from(iv, 'base64');

    // // console.log("Creer decipher secretKey: " + cleSecreteBuffer.toString('base64') + ", iv: " + ivBuffer.toString('base64'));
    // var decipher = crypto.createDecipheriv(AES_ALGORITHM, cleSecreteBuffer, ivBuffer);
    //
    // // console.debug("Decrypter " + contenuCrypte.toString('base64'));
    // let contenuDecrypteString = decipher.update(contenuCrypte, 'base64',  'utf8');
    // contenuDecrypteString += decipher.final('utf8');

    // Dechiffrage avec node-forge
    let cleSecreteBuffer = forge.util.decode64(cleSecrete.toString('base64'));
    let ivBuffer = forge.util.decode64(iv);

    var decipher = forge.cipher.createDecipher('AES-CBC', cleSecreteBuffer);
    decipher.start({iv: ivBuffer});
    let bufferContenu = forge.util.createBuffer(forge.util.decode64(contenuCrypte));
    decipher.update(bufferContenu);
    var result = decipher.finish(); // check 'result' for true/false

    if(!result) {
      reject("Erreur dechiffrage");
    }
    let output = decipher.output;
    // console.debug("Output decrypte: " + result);
    // console.debug(output);
    contenuDecrypteString = output.data.toString('utf8');

    // Verifier le IV (16 premiers bytes)
    // for(let i=0; i<16; i++) {
    //   let xor_result = output.data[i] ^ 0x02; // ivBuffer[i];
    //   console.debug("Output " + i + ": " + output.data[i] + ", ivBuffer: " + ivBuffer[i] + ", xor: " + xor_result);
    // }

    // Enlever 16 bytes pour IV
    contenuDecrypteString = contenuDecrypteString.slice(16);

    // console.debug("Contenu decrypte :");
    // console.debug(contenuDecrypteString);

    // let dictDecrypte = JSON.parse(contenuDecrypteString);
    // console.log("Dict decrypte: ");
    // console.log(dictDecrypte);

    resolve(contenuDecrypteString);
  });
}

module.exports = { Decrypteur, getDecipherPipe4fuuid, decrypterSymmetrique }
