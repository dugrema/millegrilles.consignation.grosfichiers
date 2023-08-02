const debug = require('debug')('utilbackup')

const fs = require('fs')
const zlib = require('zlib')

const forgecommon = require('@dugrema/millegrilles.utiljs/src/forgecommon')

async function verifierCatalogue(pki, pathCatalogue, domaine) {
    let readStream = fs.createReadStream(pathCatalogue)
    let contenu = ''
    await new Promise((resolve, reject)=>{
        readStream.on('error', reject)

        if(pathCatalogue.endsWith('.gz')) {
            readStream = readStream.pipe(zlib.createGunzip())
            readStream.on('error', reject)
        }

        readStream.on('close', resolve)
        readStream.on('data', chunk => { contenu += chunk })
        readStream.read()
    })

    const catalogue = JSON.parse(contenu)
    try { 
        const resultat = await pki.verifierMessage(catalogue) 
        const extensions = forgecommon.extraireExtensionsMillegrille(resultat.certificat)
        if(!extensions.niveauxSecurite || extensions.niveauxSecurite.length === 0) {
            debug("recevoirFichier ERROR catalogue invalide (certificat n'est pas systeme)")
            throw new Error("Catalogue invalide (certificat n'est pas systeme)")
        }
    } catch(err) {
        debug("recevoirFichier ERROR catalogue invalide (signature ou hachage)")
        throw new Error('Catalogue invalide (signature ou hachage)')
    }

    contenu = JSON.parse(catalogue['contenu'])
    if(contenu.domaine !== domaine) {
        debug("recevoirFichier Mauvais domaine pour catalogue %s", pathCatalogue)
        throw new Error('Catalogue invalide (mauvais domaine)')
    }

    debug("Catalogue recu OK")
}

module.exports = { verifierCatalogue }
