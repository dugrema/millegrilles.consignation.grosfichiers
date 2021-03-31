const debug = require('debug')('millegrilles:fichiers:transformationsVideo')
const fs = require('fs')
const fsPromises = require('fs/promises')
const tmpPromises = require('tmp-promise')
const path = require('path')
const multibase = require('multibase')
const FFmpeg = require('fluent-ffmpeg')
const { chargerCleDechiffrage, creerOutputstreamChiffrage } = require('./cryptoUtils')
const { gcmStreamReaderFactory } = require('../util/cryptoUtils')

async function probeVideo(input, opts) {
  opts = opts || {}
  const maxHeight = opts.maxHeight || 720,
        maxBitrate = opts.maxBitrate || 750000

  const resultat = await new Promise((resolve, reject)=>{
    FFmpeg.ffprobe(input, (err, metadata) => {
      if(err) return reject(err)
      resolve(metadata)
    })
  })

  debug("Resultat probe : %O", resultat)

  const infoVideo = resultat.streams.filter(item=>item.codec_type === 'video')[0]
  // debug("Information video : %O", infoVideo)

  // Determiner le bitrate et la taille (verticale) du video pour eviter un
  // upscaling ou augmentation bitrate
  const bitrate = infoVideo.bit_rate,
        height = infoVideo.height,
        nb_frames = infoVideo.nb_frames !== 'N/A'?infoVideo.nb_frames:null

  // debug("Trouve : taille %d, bitrate %d", height, bitrate)

  const tailleEncoding = [2160, 1440, 1080, 720, 480, 360, 240].filter(item=>{
    return item <= height && item <= maxHeight
  })[0]
  const bitRateEncoding = [1200000, 1000000, 750000, 600000, 500000, 400000, 200000].filter(item=>{
    return item <= bitrate && item <= maxBitrate
  })[0]

  // debug("Information pour encodage : height %d, bit rate %d", tailleEncoding, bitRateEncoding)

  return {height: tailleEncoding, bitrate: bitRateEncoding, nb_frames, raw: infoVideo}
}

async function transcoderVideo(streamFactory, outputStream, opts) {
  if(!opts) opts = {}

  var   videoBitrate = opts.videoBitrate || 500000
        height = opts.height || 480

  const videoCodec = opts.videoCodec || 'libx264',
        audioCodec = opts.audioCodec || 'aac',
        audioBitrate = opts.audioBitrate || '64k',
        format = opts.format || 'mp4',
        progressCb = opts.progressCb

  var input = streamFactory()
  var videoInfo = await probeVideo(input, {maxBitrate: videoBitrate, maxHeight: height})
  input.close()
  videoBitrate = videoInfo.bitrate
  height = videoInfo.height

  // videoBitrate = '' + (videoBitrate / 1000) + 'k'
  debug('Utilisation video bitrate : %s', videoBitrate)

  // Tenter transcodage avec un stream - fallback sur fichier direct
  // Va etre utilise avec un decipher sur fichiers .mgs2
  var modeInputStream = true
  input = streamFactory()  // Reset stream (utilise par probeVideo)

  var progressHook, framesTotal = videoInfo.nb_frames, framesCourant = 0
  if(progressCb) {
    progressHook = progress => {
      if(framesTotal) {
        progressCb({framesTotal, ...progress})
      } else {
        progressCb(progress)
        // Conserver le nombre de frames connus pour passe 2
        framesCourant = progress.frames
      }
    }
  }

  // Creer repertoire temporaire pour fichiers de log, outputfile
  const tmpDir = await tmpPromises.dir({unsafeCleanup: true})

  // Passe 1
  debug("Debut passe 1")
  var fichierInputTmp = null  //, fichierOutputTmp = null
  try {
    const videoOpts = { videoBitrate, height, videoCodec }
    const optsTranscodage = {
      progressCb: progressHook,
      tmpDir: tmpDir.path,
    }

    try {
      await transcoderPasse(1, input, null, videoOpts, null, optsTranscodage)
    } catch(err) {
      // Verifier si on a une erreur de streaming (e.g. video .mov n'est pas
      // supporte en streaming)
      const errMsg = err.message
      if(errMsg.indexOf("ffmpeg exited with code 1") === -1) {
        throw err  // Erreur non geree
      }

      debug("Echec parsing stream, dechiffrer dans un fichier temporaire et utiliser directement")
      modeInputStream = false

      // Copier le contenu du stream dans un fichier temporaire
      input = path.join(tmpDir.path, 'input.dat')
      fichierInputTmp = await extraireFichierTemporaire(input, streamFactory())
      // input = fichierInputTmp.path
      debug("Fichier temporaire input pret: %s", input)

      await transcoderPasse(1, input, null, videoOpts, null, optsTranscodage)
    }
    debug("Passe 1 terminee, debut passe 2")

    // Passe 2
    const audioOpts = {audioCodec, audioBitrate}
    if(modeInputStream) {
      // Reset inputstream
      input = streamFactory()
    }

    // Set nombre de frames trouves dans la passe 1 au besoin (sert au progress %)
    if(!framesTotal) framesTotal = framesCourant

    //fichierOutputTmp = await tmpPromises.file({keep: true, postfix: '.' + format})
    const destinationPath = path.join(tmpDir.path, 'output.' + format)
    // const destinationPath = fichierOutputTmp.path
    debug("Fichier temporaire output : %s", destinationPath)

    await transcoderPasse(2, input, destinationPath, videoOpts, audioOpts, optsTranscodage)
    debug("Passe 2 terminee, transferer le fichier output")

    const outputFileReader = fs.createReadStream(destinationPath)
    const promiseOutput = new Promise((resolve, reject)=>{
      outputFileReader.on('error', err=>reject(err))
      outputFileReader.on('end', _=>resolve())
    })
    outputFileReader.pipe(outputStream)
    await promiseOutput

    // Faire un probe de l'output pour recuperer stats
    var videoInfo = await probeVideo(destinationPath)
    debug("Information video transcode: %O", videoInfo.raw)

    return {video: videoOpts, audio: audioOpts, probe: videoInfo.raw}
  } finally {
    if(tmpDir) {
      debug("Suppression du repertoire temporaire %s", tmpDir.path)
      tmpDir.cleanup()
    }
  }
}

function transcoderPasse(passe, source, destinationPath, videoOpts, audioOpts, opts) {
  videoOpts = videoOpts || {}
  audioOpts = audioOpts || {}  // Non-utilise pour passe 1
  opts = opts || {}

  const videoBitrate = videoOpts.videoBitrate,
        height = videoOpts.height,
        videoCodec = videoOpts.videoCodec

  const audioCodec = audioOpts.audioCodec,
        audioBitrate = audioOpts.audioBitrate

  const progressCb = opts.progressCb,
        format = opts.format || 'mp4',
        tmpDir = opts.tmpDir || '/tmp'

  const ffmpegProcessCmd = new FFmpeg(source, {niceness: 10})
    .withVideoBitrate(''+videoBitrate)
    .withSize('?x' + height)
    .videoCodec(videoCodec)

  var passlog = path.join(tmpDir, 'ffmpeg2pass-')
  if(passe === 1) {
    // Passe 1, desactiver traitement stream audio
    ffmpegProcessCmd
      .outputOptions(['-an', '-f', 'null', '-pass', '1', '-passlogfile', passlog])
  } else if(passe === 2) {
    debug("Audio info : %O, format %s", audioOpts, format)
    ffmpegProcessCmd
      .audioCodec(audioCodec)
      .audioBitrate(audioBitrate)
      .outputOptions(['-pass', '2', '-movflags', 'faststart', '-passlogfile', passlog])
  } else {
    throw new Error("Passe doit etre 1 ou 2 (passe=%O)", passe)
  }

  if(progressCb) {
    ffmpegProcessCmd.on('progress', progressCb)
  }

  const processPromise = new Promise((resolve, reject)=>{
    ffmpegProcessCmd.on('error', function(err) {
        // console.error('An error occurred: %O', err);
        reject(err);
    })
    ffmpegProcessCmd.on('end', function(filenames) {
      resolve()
    })
  })

  // Demarrer le traitement
  if(passe === 1) {
    // Aucun ouput a sauvegarder pour passe 1
    ffmpegProcessCmd.saveToFile('/dev/null')
  } else if(passe === 2) {
    ffmpegProcessCmd
      .saveToFile(destinationPath)
  }

  return processPromise
}

async function extraireFichierTemporaire(fichierPath, inputStream) {
  //const fichierInputTmp = await tmpPromises.file({keep: true})
  debug("Fichier temporaire : %s", fichierPath)

  const outputStream = fs.createWriteStream(fichierPath)
  const promiseOutput =  new Promise((resolve, reject)=>{
    outputStream.on('error', err=>{
      reject(err)
      // fichierInputTmp.cleanup()
    })
    outputStream.on('close', _=>{resolve()})
  })

  inputStream.pipe(outputStream)
  // inputStream.read()

  return promiseOutput
}

async function traiterCommandeTranscodage(mq, pathConsignation, message) {
  debug("Commande traiterCommandeTranscodage video recue : %O", message)

  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  const fuuid = message.fuuid

  // Transmettre evenement debut de transcodage
  mq.emettreEvenement({fuuid}, 'evenement.fichiers.transcodageDebut')

  const cleInfo = await chargerCleDechiffrage(mq, fuuid)
  debug("Cle dechiffrage : %O", cleInfo)

  const profil = getProfilTranscodage(message)

  const progressCb = progress => {
    debug("Progress : %O", progress)
  }

  // Creer un factory d'input streams decipher
  const pathFichierChiffre = pathConsignation.trouverPathLocal(fuuid, true);
  const inputStreamFactory = gcmStreamReaderFactory(
    pathFichierChiffre,
    cleInfo.cleSymmetrique,
    cleInfo.metaCle.iv,
    cleInfo.metaCle.tag
  )

  // Recuperer certificats de chiffrage (maitre des cles, millegrille)
  const certificatsPem = cleInfo.clesPubliques.map(item=>item[0])

  // Transmettre transaction info chiffrage
  const identificateurs_document = {
      attachement_fuuid: fuuid,
      type: 'video',
    }
  const domaine = 'GrosFichiers'
  const cipherOutputStream = await creerOutputstreamChiffrage(certificatsPem, identificateurs_document, domaine, {})

  const opts = {...profil, progressCb}
  debug("Debut dechiffrage fichier video, opts : %O", opts)
  const fichierOutputTmp = await tmpPromises.file({prefix: 'video-', keep: true})
  let resultatTranscodage = null
  try {
    const outputStream = fs.createWriteStream(fichierOutputTmp.path)
    cipherOutputStream.pipe(outputStream)
    resultatTranscodage = await transcoderVideo(inputStreamFactory, cipherOutputStream, opts)

    // Deplacer le fichier temporaire vers path consignation
    const fuuidOutput = cipherOutputStream.commandeMaitredescles.hachage_bytes
    const pathLocalOutput = pathConsignation.trouverPathLocal(fuuidOutput)
    await fsPromises.mkdir(path.dirname(pathLocalOutput), {recursive: true})
    debug("Deplacer output transcodage vers %s", pathLocalOutput)
    fsPromises.rename(fichierOutputTmp.path, pathLocalOutput)
  } catch(err) {
    // Cleanup fichier temporaire cas d'erreur
    fichierOutputTmp.cleanup().catch(err=>{debug("Err cleanup fichier video tmp (OK) : %O", err)})
    throw err
  }
  const probeInfo = resultatTranscodage.probe

  const commandeMaitredescles = cipherOutputStream.commandeMaitredescles
  debug("Resultat transcodage = %O\ncommandeMaitredescles = %O", resultatTranscodage, commandeMaitredescles)

  // Transmettre transaction associer video transcode
  const transactionAssocierPreview = {
    fuuid: fuuid,

    mimetype: message.mimetype,
    fuuidVideo: commandeMaitredescles.hachage_bytes,
    hachage: commandeMaitredescles.hachage_bytes,

    width: probeInfo.width,
    height: probeInfo.height,
    codec: profil.videoCodecName,
    bitrate: resultatTranscodage.video.videoBitrate,
    tailleFichier: cipherOutputStream.byteCount,
  }

  debug("Transaction transcoder video : %O", transactionAssocierPreview)
  mq.emettreEvenement({fuuid: message.fuuid}, 'evenement.fichiers.transcodageTermine')

  // Transmettre commande maitre des cles
  const domaineActionCles = 'MaitreDesCles.sauvegarderCle'
  await mq.transmettreCommande(domaineActionCles, commandeMaitredescles)

  // Transmettre transaction pour associer le video au fuuid
  const domaineActionAssocierPreview = 'GrosFichiers.associerVideo'
  await mq.transmettreTransactionFormattee(transactionAssocierPreview, domaineActionAssocierPreview)
}

function getProfilTranscodage(params) {
  const profils = {
    webm: {
      videoBitrate: 750000,
      height: 720,
      videoCodec: 'libvpx-vp9',
      audioCodec: 'libopus',
      audioBitrate: '64k',
      format: 'webm',
      videoCodecName: 'vp9',
    },
    mp4: {
      videoBitrate: 600000,
      height: 480,
      videoCodec: 'libx264',
      audioCodec: 'aac',
      audioBitrate: '64k',
      format: 'mp4',
      videoCodecName: 'h264',
    }
  }

  let profil = null
  switch(params.mimetype) {
    case 'video/webm':
      profil = {...profils.webm, ...params}
    case 'video/mp4':
      profil = {...profils.mp4, ...params}
  }

  return profil
}

module.exports = {
  probeVideo, transcoderVideo, traiterCommandeTranscodage,
}
