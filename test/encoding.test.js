const fs = require('fs')
const FFmpeg = require('fluent-ffmpeg')

const { probeVideo, transcoderVideo } = require('../util/transformationsVideo')

const VIDEOS = [
  '/home/mathieu/Videos/IMG_2010.MOV',
  '/home/mathieu/Videos/IMG_0325.MOV',
  '/home/mathieu/Videos/Flying_with_Geese.mov',
  '/home/mathieu/Videos/1136.mpeg',
  '/home/mathieu/Videos/Dr Quantum - Double Slit Experiment.flv',
]
const VIDEO_SEL = 0

describe('convertir video', ()=>{

  test('video 1 probe', async () => {
    var opts = {
      maxBitrate: 600000,
      maxHeight: 480,
    }
    await probeVideo(VIDEOS[VIDEO_SEL], opts)
  })

  test.only('video 1 mp4', async () => {
    console.debug("Convertir video 1 mp4")
    const opts = {
      videoBitrate: 600000,
      height: 480,
      videoCodec: 'libx264',
      audioCodec: 'aac',
      progressCb: progress,
    }

    await transcoderVideo(VIDEOS[VIDEO_SEL], '/home/mathieu/Videos/output.mp4', opts)
  }, 20 * 60 * 1000)

  test('video 1 webm', async () => {
    console.debug("Convertir video 1 webm")
    const opts = {
      videoBitrate: 750000,
      height: 720,
      videoCodec: 'libvpx-vp9',
      audioCodec: 'libopus',
      progressCb: progress,
    }
    await transcoderVideo(VIDEOS[VIDEO_SEL], '/home/mathieu/Videos/output.webm', opts)
  }, 60 * 60 * 1000)

})

function progress(progress) {
  console.debug("Progress : %O", progress)
}

// async function probeVideo(sourcePath, opts) {
//   opts = opts || {}
//   const maxHeight = opts.maxHeight || 720,
//         maxBitrate = opts.maxBitrate || 750000
//
//   const resultat = await new Promise((resolve, reject)=>{
//     FFmpeg.ffprobe(sourcePath, (err, metadata) => {
//       if(err) return reject(err)
//       resolve(metadata)
//     })
//   })
//
//   // console.debug("Resultat probe : %O", resultat)
//
//   const infoVideo = resultat.streams.filter(item=>item.codec_type === 'video')[0]
//   // console.debug("Information video : %O", infoVideo)
//
//   // Determiner le bitrate et la taille (verticale) du video pour eviter un
//   // upscaling ou augmentation bitrate
//   const bitrate = infoVideo.bit_rate,
//         height = infoVideo.height
//
//   // console.debug("Trouve : taille %d, bitrate %d", height, bitrate)
//
//   const tailleEncoding = [720, 480, 360, 240].filter(item=>{
//     return item <= height && item <= maxHeight
//   })[0]
//   const bitRateEncoding = [1200000, 1000000, 750000, 600000, 500000, 400000, 200000].filter(item=>{
//     return item <= bitrate && item <= maxBitrate
//   })[0]
//
//   console.debug("Information pour encodage : height %d, bit rate %d",
//     tailleEncoding, bitRateEncoding)
//
//   return {height: tailleEncoding, bitrate: bitRateEncoding}
// }
//
// async function transcoderVideo(sourcePath, destinationPath, opts) {
//   if(!opts) opts = {}
//
//   var   videoBitrate = opts.videoBitrate || 500000
//         height = opts.height || 480
//   const videoCodec = opts.videoCodec || 'libx264',
//         audioCodec = opts.audioCodec || 'aac',
//         audioBitrate = opts.audioBitrate || '64k',
//         progressCb = opts.progressCb
//
//   var videoInfo = await probeVideo(sourcePath, {maxBitrate: videoBitrate, maxHeight: height})
//   videoBitrate = videoInfo.bitrate
//   height = videoInfo.height
//
//   videoBitrate = '' + (videoBitrate / 1000) + 'k'
//   console.debug('Utilisation video bitrate : %s', videoBitrate)
//
//   // Tenter transcodage avec un stream - fallback sur fichier direct
//   // Va etre utilise avec un decipher sur fichiers .mgs2
//   var modeInputStream = true
//   var input = fs.createReadStream(sourcePath)
//
//   // Passe 1
//   console.debug("Debut passe 1")
//   const videoOpts = { videoBitrate, height, videoCodec }
//   const optsTranscodage = {progressCb}
//   try {
//     await transcoderPasse(1, input, null, videoOpts, null, optsTranscodage)
//   } catch(err) {
//     // Verifier si on a une erreur de streaming (e.g. video .mov n'est pas
//     // supporte en streaming)
//     const errMsg = err.message
//     if(errMsg.indexOf("ffmpeg exited with code 1") === -1) {
//       throw err  // Erreur non geree
//     }
//
//     console.debug("Echec parsing stream, dechiffrer dans un fichier temporaire et utiliser directement")
//     modeInputStream = false
//     input = sourcePath  // TODO utiliser fichier tmp dechiffre
//     await transcoderPasse(1, input, null, videoOpts, null, optsTranscodage)
//   }
//   console.debug("Passe 1 terminee, debut passe 2")
//
//   // Passe 2
//   const audioOpts = {audioCodec, audioBitrate}
//   if(modeInputStream) {
//     // Reset inputstream
//     input = fs.createReadStream(sourcePath)
//   }
//   await transcoderPasse(2, input, destinationPath, videoOpts, audioOpts, optsTranscodage)
//   console.debug("Passe 2 terminee")
//
//   return {video: videoOpts, audio: audioOpts}
// }
//
// function transcoderPasse(passe, source, destinationPath, videoOpts, audioOpts, opts) {
//   videoOpts = videoOpts || {}
//   audioOpts = audioOpts || {}  // Non-utilise pour passe 1
//   opts = opts || {}
//
//   const videoBitrate = videoOpts.videoBitrate,
//         height = videoOpts.height,
//         videoCodec = videoOpts.videoCodec
//
//   const audioCodec = audioOpts.audioCodec,
//         audioBitrate = audioOpts.audioBitrate
//
//   const progressCb = opts.progressCb
//
//   const ffmpegProcessCmd = new FFmpeg(source, {niceness: 10})
//     .withVideoBitrate(''+videoBitrate)
//     .withSize('?x' + height)
//     .videoCodec(videoCodec)
//
//   if(passe === 1) {
//     // Passe 1, desactiver traitement stream audio
//     ffmpegProcessCmd
//       .outputOptions(['-an', '-f', 'null', '-pass', '1'])
//   } else if(passe === 2) {
//     console.debug("Audio info : %O", audioOpts)
//     ffmpegProcessCmd
//       .audioCodec(audioCodec)
//       .audioBitrate(audioBitrate)
//       .outputOptions(['-pass', '2', '-movflags', 'faststart'])
//   } else {
//     throw new Error("Passe doit etre 1 ou 2 (passe=%O)", passe)
//   }
//
//   if(progressCb) {
//     ffmpegProcessCmd.on('progress', progressCb)
//   }
//
//   const processPromise = new Promise((resolve, reject)=>{
//     ffmpegProcessCmd.on('error', function(err) {
//         // console.error('An error occurred: %O', err);
//         reject(err);
//     })
//     ffmpegProcessCmd.on('end', function(filenames) {
//       resolve()
//     })
//   })
//
//   // Demarrer le traitement
//   if(passe === 1) {
//     // Aucun ouput a sauvegarder pour passe 1
//     ffmpegProcessCmd.saveToFile('/dev/null')
//   } else if(passe === 2) {
//     ffmpegProcessCmd.saveToFile(destinationPath)
//   }
//
//   return processPromise
// }
