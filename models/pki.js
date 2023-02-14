const debug = require('debug')('pki')
const { dechiffrerChampsChiffres } = require('@dugrema/millegrilles.nodejs/src/chiffrage')

const L2PRIVE = '2.prive'

async function recupererCle(mq, ref_hachage_bytes, opts) {
    opts = opts || {}
  
    const domaine = opts.domaine || 'CoreTopologie',
          exchange = L2PRIVE
  
    let action = 'getCleConfiguration',
        requete = { ref_hachage_bytes }
  
    debug("Nouvelle requete dechiffrage cle a transmettre (domaine %s, action %s): %O", domaine, action, requete)
    const reponseCle = await mq.transmettreRequete(domaine, requete, {action, exchange, ajouterCertificat: true, decoder: true})
    debug("Reponse requete dechiffrage : %O", reponseCle)
    if(reponseCle.acces !== '1.permis') {
      debug("Cle acces refuse : ", reponseCle)
      throw new Error(`Erreur cle ${ref_hachage_bytes} : ${reponseCle.acces}`)
    }
    debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)
  
    // Dechiffrer cle recue
    const info = reponseCle.cles[ref_hachage_bytes]
    const cleChiffree = info.cle
    const cleSecrete = await mq.pki.decrypterAsymetrique(cleChiffree)
  
    return {cleSecrete, info}
}

async function dechiffrerConfiguration(mq, configuration) {
    const data_chiffre = configuration.data_chiffre
    const cle = await recupererCle(mq, data_chiffre.ref_hachage_bytes)
    const doc = await dechiffrerChampsChiffres(data_chiffre, cle)
    
    configuration.data_dechiffre = doc

    return doc
}

module.exports = { recupererCle, dechiffrerConfiguration }
