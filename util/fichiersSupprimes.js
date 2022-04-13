const debug = require('debug')('util:fichiersSupprimes')
const readdirp = require('readdirp')
const fsPromises = require('fs/promises')
const path = require('path')

async function entretienFichiersSupprimes(mq, pathConsignation) {
    debug("Debut entretien des fichiers supprimes")
    await fsPromises.mkdir(pathConsignation.consignationPathCorbeille, {recursive: true})

    // Detecter les fichiers qui devraient etre mis en attente de suppression
    await parcourirFichiersActifs(mq, pathConsignation)

    // Supprimer les fichiers en attente depuis plus de 14 jours

}

async function parcourirFichiersActifs(mq, pathConsignation) {
    const settingsReaddirp = {
        type: 'files',
    }
    
    const pathFichiersActifs = pathConsignation.consignationPathLocal
    let fichiersVerifier = {}
    let compteur = 0
    for await (const entry of readdirp(pathFichiersActifs, settingsReaddirp)) {
        debug("Fichier : %O", entry)
        const { basename, fullPath } = entry
        fichiersVerifier[basename] = fullPath
        if(++compteur >= 1000) {
            // Executer une batch de verification
            await traiterBatch(mq, pathConsignation, fichiersVerifier)

            // Reset
            fichiersVerifier = {}
            compteur = 0
        }
    }

    if(compteur > 0) {
        // Derniere batch
        await traiterBatch(mq, pathConsignation, fichiersVerifier)
    }

}

async function traiterBatch(mq, pathConsignation, fichiersVerifier) {
    debug("Traiter batch : %O", fichiersVerifier)

    const requete = { fuuids: Object.keys(fichiersVerifier) }
    const domaine = 'GrosFichiers',
          action = 'confirmerEtatFuuids'
    const reponse = await mq.transmettreRequete(domaine, requete, {action})
    debug("Reponse verification : %O", reponse)

    const confirmation = reponse.confirmation || {},
          fichiersResultat = confirmation.fichiers || []
    if(fichiersResultat) {
        for await (const reponseFichier of fichiersResultat) {
            const {fuuid, supprime} = reponseFichier
            if(supprime) {
                debug("Le fichier %s est supprime, on le deplace vers la corbeille", fuuid)
                const pathFichierOriginal = fichiersVerifier[fuuid]
                const pathFichierCorbeille = path.join(pathConsignation.consignationPathCorbeille, fuuid)
                try {
                    fsPromises.rename(pathFichierOriginal, pathFichierCorbeille)
                } catch(err) {
                    fsPromises.copyFile(pathFichierOriginal, pathFichierCorbeille)
                    fsPromises.rm(pathFichierOriginal)
                }
            }
        }
    }

}

module.exports = { entretienFichiersSupprimes }
