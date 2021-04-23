const debug = require('debug')('millegrilles:fichiers:publierUtils')
const readdirp = require('readdirp')

async function preparerPublicationRepertoire(pathRepertoire, cbCommandeHandler) {
  /*
    Prepare l'information et commandes pour publier un repertoire
    - pathRepertoire : repertoire a publier (base exclue de la publication)
    - cbCommandeHandler : fonction (stat)=>{...} invoquee sur chaque entree du repertoire
    Retourne : {bytesTotal}
  */
  var bytesTotal = 0
  const params = {
    alwaysStat: true,
    type: 'files_directories',
  }
  for await(const entry of readdirp(pathRepertoire, params)) {
    // const stat = await fsPromises.stat(entry.fullPath)
    debug("Entry readdirp : %O", entry)
    const stats = entry.stats
    await cbCommandeHandler(entry)
    if(stats.isFile()) {
      bytesTotal += stats.size
    }
  }
  return {bytesTotal}
}

module.exports = {preparerPublicationRepertoire}
