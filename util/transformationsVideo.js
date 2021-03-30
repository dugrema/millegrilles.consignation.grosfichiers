const debug = require('debug')('millegrilles:fichiers:transformationsVideo')
const fs = require('fs')
const fsPromises = require('fs/promises')
const tmpPromises = require('tmp-promise')
const FFmpeg = require('fluent-ffmpeg')

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

  return {height: tailleEncoding, bitrate: bitRateEncoding, nb_frames}
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

  videoBitrate = '' + (videoBitrate / 1000) + 'k'
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

  // Passe 1
  debug("Debut passe 1")
  const videoOpts = { videoBitrate, height, videoCodec }
  const optsTranscodage = {progressCb: progressHook}

  var fichierInputTmp = null, fichierOutputTmp = null
  try {
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
      fichierInputTmp = await extraireFichierTemporaire(streamFactory())
      input = fichierInputTmp.path
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

    fichierOutputTmp = await tmpPromises.file({keep: true, postfix: '.' + format})
    const destinationPath = fichierOutputTmp.path
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

    return {video: videoOpts, audio: audioOpts}
  } finally {
    if(fichierInputTmp) {
      debug("Cleanup du fichier temporaire input %s", fichierInputTmp.path)
      fichierInputTmp.cleanup()
    }
    if(fichierOutputTmp) {
      debug("Cleanup du fichier temporaire output %s", fichierOutputTmp.path)
      fichierOutputTmp.cleanup()
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
        format = opts.format || 'mp4'

  const ffmpegProcessCmd = new FFmpeg(source, {niceness: 10})
    .withVideoBitrate(''+videoBitrate)
    .withSize('?x' + height)
    .videoCodec(videoCodec)

  if(passe === 1) {
    // Passe 1, desactiver traitement stream audio
    ffmpegProcessCmd
      .outputOptions(['-an', '-f', 'null', '-pass', '1'])
  } else if(passe === 2) {
    debug("Audio info : %O, format %s", audioOpts, format)
    ffmpegProcessCmd
      .audioCodec(audioCodec)
      .audioBitrate(audioBitrate)
      .outputOptions(['-pass', '2', '-movflags', 'faststart'])
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

async function extraireFichierTemporaire(inputStream) {
  const fichierInputTmp = await tmpPromises.file({keep: true})
  debug("Fichier temporaire : %s", fichierInputTmp.path)

  const outputStream = fs.createWriteStream(fichierInputTmp.path)
  const promiseOutput =  new Promise((resolve, reject)=>{
    outputStream.on('error', err=>{
      reject(err)
      fichierInputTmp.cleanup()
    })
    outputStream.on('close', _=>{resolve(fichierInputTmp)})
  })

  inputStream.pipe(outputStream)
  // inputStream.read()

  return promiseOutput
}

module.exports = {
  probeVideo, transcoderVideo,
}
