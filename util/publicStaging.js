const debug = require('debug')('millegrilles:fichiers:grosfichiers')
const path = require('path');
const fs = require('fs');
const {PathConsignation, TraitementFichier} = require('../util/traitementFichier')
const {getDecipherPipe4fuuid} = require('../util/cryptoUtils')

async function creerStreamDechiffrage(mq, req) {
  const fuuidFichier = req.params.fuuid
  debug("Creer stream dechiffrage, query : %O", req.query)

  // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  const chainePem = mq.pki.chainePEM
  const domaineActionDemandePermission = 'GrosFichiers.demandePermissionDechiffragePublic',
        requetePermission = {fuuid: fuuidFichier}
  const reponsePermission = await mq.transmettreRequete(domaineActionDemandePermission, requetePermission)

  debug("Reponse permission access a %s:\n%O", fuuidFichier, reponsePermission)

  if( ! reponsePermission.roles_permis ) {
    debug("Permission refuse sur %s, le fichier n'est pas public", fuuidFichier)
    return {acces: '0.refuse'}
  }

  // permission['_certificat_tiers'] = chainePem
  const domaineActionDemandeCle = 'MaitreDesCles.dechiffrage'
  const reponseCle = await mq.transmettreRequete(domaineActionDemandeCle, {
    liste_hachage_bytes: reponsePermission.liste_hachage_bytes,
  })
  debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)
  if(reponseCle.acces === '0.refuse') {
    return {acces: responseCle.acces, 'err': 'Acces refuse'}
  }

  var cleChiffree, iv, fuuidEffectif = fuuidFichier, infoVideo = ''

  if(req.query.preview) {
    debug("Utiliser le preview pour extraction")
    fuuidEffectif = reponsePermission['fuuid_preview']
  } else if(req.query.video) {
    const resolution = req.query.video
    debug("Utiliser le video resolution %s pour extraction", resolution)
    // Faire une requete pour trouver le video associe a la resolution
    const domaineRequeteFichier = 'GrosFichiers.documentsParFuuid'
    const infoFichier = await mq.transmettreRequete(domaineRequeteFichier, {fuuid: fuuidFichier})
    debug("Information fichier video : %O", infoFichier)
    infoVideo = infoFichier.versions[fuuidFichier].video[resolution]
    fuuidEffectif = infoVideo.fuuid
    debug("Fuuid effectif pour video %s : %s", resolution, fuuidEffectif)
  }

  var infoClePreview = reponseCle.cles[fuuidEffectif]
  cleChiffree = infoClePreview.cle
  iv = infoClePreview.iv
  tag = infoClePreview.tag

  // Dechiffrer cle recue
  const cleDechiffree = await mq.pki.decrypterAsymetrique(cleChiffree)

  const decipherStream = getDecipherPipe4fuuid(cleDechiffree, iv, {tag})

  return {acces: reponseCle.acces, permission: reponsePermission, fuuidEffectif, decipherStream, infoVideo}
}

async function stagingFichier(pathConsignation, fuuidEffectif, infoStream) {
  // Staging de fichier public

  // Verifier si le fichier existe deja
  const pathFuuidLocal = pathConsignation.trouverPathLocal(fuuidEffectif, true)
  const pathFuuidEffectif = path.join(pathConsignation.consignationPathDownloadStaging, fuuidEffectif)
  var statFichier = await new Promise((resolve, reject) => {
    // S'assurer que le path de staging existe
    fs.mkdir(pathConsignation.consignationPathDownloadStaging, {recursive: true}, err=>{
      if(err) return reject(err)
      // Verifier si le fichier existe
      fs.stat(pathFuuidEffectif, (err, stat)=>{
        if(err) {
          if(err.errno == -2) {
            resolve(null)  // Le fichier n'existe pas, on va le creer
          } else {
            reject(err)
          }
        } else {
          // Touch et retourner stat
          const time = new Date()
          fs.utimes(pathFuuidEffectif, time, time, err=>{
            if(err) {
              debug("Erreur touch %s : %o", pathFuuidEffectif, err)
              return
            }
            resolve({pathFuuidLocal, filePath: pathFuuidEffectif, stat})
          })
        }
      })
    })
  })

  // Verifier si le fichier existe deja - on n'a rien a faire
  if(statFichier) return statFichier

  // Le fichier n'existe pas, on le dechiffre dans staging
  const outStream = fs.createWriteStream(pathFuuidEffectif, {flags: 'wx'})
  return new Promise((resolve, reject)=>{
    outStream.on('close', _=>{
      fs.stat(pathFuuidEffectif, (err, stat)=>{
        if(err) {
          reject(err)
        } else {
          debug("Fin staging fichier %O", stat)
          resolve({pathFuuidLocal, filePath: pathFuuidEffectif, stat})
        }
      })

    })
    outStream.on('error', err=>{
      debug("Erreur staging fichier %s : %O", pathFuuidEffectif, err)
      reject(err)
    })

    debug("Staging fichier %s", pathFuuidEffectif)
    infoStream.decipherStream.writer.pipe(outStream)
    var readStream = fs.createReadStream(pathFuuidLocal);
    readStream.pipe(infoStream.decipherStream.reader)
  })

}

function cleanupStaging() {
  // Supprime les fichiers de staging en fonction de la derniere modification (touch)
  const pathConsignation = new PathConsignation()
  const pathDownloadStaging = pathConsignation.consignationPathDownloadStaging

  // debug("Appel cleanupStagingDownload " + pathDownloadStaging)

  fs.readdir(pathDownloadStaging, (err, files)=>{
    if(err) {
      if(err.code === 'ENOENT') return  // Repertoire n'existe pas
      return console.error("cleanupStagingDownload ERROR: %O", err)
    }

    const expirationMs = new Date().getTime() - STAGING_FILE_TIMEOUT_MSEC

    files.forEach(file=>{
      const filePath = path.join(pathDownloadStaging, file)
      fs.stat(filePath, (err, stat)=>{
        if(err) {
          if(err.code === 'ENOENT') return  // Repertoire n'existe pas
          return console.error("cleanupStagingDownload ERROR: %O", err)
        }

        // debug("Info fichier %s: %O", filePath, stat)
        if(stat.mtimeMs < expirationMs) {
          debug("Cleanup fichier download staging %s", filePath)
          fs.unlink(filePath, err=>{
            if(err) debug("Erreur unlink fichier %O", err)
          })
        }
      })
    })

  })
}

module.exports = {
  stagingFichier, cleanupStaging
}
