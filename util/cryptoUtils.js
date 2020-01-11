const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

class Decrypteur {

  decrypter(sourceCryptee, destination, cleSecreteDecryptee, iv) {
    return new Promise((resolve, reject)=>{
      let cryptoStream = getDecipherPipe4fuuid(cleSecreteDecryptee, iv);

      // Calculer taille et sha256 du fichier decrypte. Necessaire pour transaction.
      const sha256 = crypto.createHash('sha256');
      var sha256Hash = null;
      var tailleFichier = 0;
      cryptoStream.on('data', chunk=>{
        sha256.update(chunk);
        tailleFichier = tailleFichier + chunk.length;
      });
      cryptoStream.on('end', ()=>{
        // Comparer hash a celui du header
        sha256Hash = sha256.digest('hex');
        console.debug("Hash fichier " + sha256Hash);
      });

      console.log("Decryptage fichier " + sourceCryptee + " vers " + destination);
      let writeStream = fs.createWriteStream(destination);

      writeStream.on('close', ()=>{
        console.debug("Fermeture fichier decrypte");
        resolve({tailleFichier, sha256Hash});
      });
      writeStream.on('error', ()=>{
        console.error("Erreur decryptage fichier");
        reject();
      });

      cryptoStream.pipe(writeStream);

      // Ouvrir et traiter fichier
      let readStream = fs.createReadStream(sourceCryptee);
      readStream.pipe(cryptoStream);
    });
  }

}

function getDecipherPipe4fuuid(cleSecrete, iv) {
  // On prepare un decipher pipe pour decrypter le contenu.

  let ivBuffer = Buffer.from(iv, 'base64');
  console.debug("IV (" + ivBuffer.length + "): ");
  console.debug(iv);

  // decryptedSecretKey = Buffer.from(forge.util.binary.hex.decode(decryptedSecretKey));
  let decryptedSecretKey = Buffer.from(cleSecrete, 'base64');
  decryptedSecretKey = decryptedSecretKey.toString('utf8');

  var typedArray = new Uint8Array(decryptedSecretKey.match(/[\da-f]{2}/gi).map(function (h) {
    return parseInt(h, 16)
  }));
  console.debug("Cle secrete decryptee (" + typedArray.length + ") bytes");

  // Creer un decipher stream
  var decipher = crypto.createDecipheriv('aes256', typedArray, ivBuffer);

  return decipher;

}


module.exports = { Decrypteur }
