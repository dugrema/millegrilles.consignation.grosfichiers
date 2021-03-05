const {decrypter} = require('./cryptoUtils')
const fs = require('fs')

describe('dechiffrageFichier', ()=>{

  test('Dechiffrer fichier', ()=>{
    console.debug("Test")

    // const infoChiffreeGCM = {
    //   "contenu_chiffre": "md6b/7n5Usob5wE0oxu6nk50WSpQJ1t3qrzHhnF2fSz8g",
    //   "iv": "mC2h0U93HJ7a8b5d1WUmWtw",
    //   "cle_secrete": "mMGY1MzQyMWJlZDc4NGFjNDJlZGEyMzIyY2JmODM4Mzg4NTdiNTE2NTg0NDk1ODA0YTRlOTE4YmJiNzJjMmVjMg",
    //   "compute_tag": "miQ8psN9J1mVAzlzeMgQsrg",
    // }
    //
    // const data = Buffer.from(infoChiffreeGCM.contenu_chiffre, 'base64')
    // const fichier = '/tmp/test.txt.mgs2'
    // fs.writeFileSync(fichier, data)
    //
    // decrypter(fichier, '/tmp/test.txt', infoChiffreeGCM.cle_secrete, infoChiffreeGCM.iv, {tag: infoChiffreeGCM.compute_tag})
    //
    // // expect(true).toBe(true)
  })

})
