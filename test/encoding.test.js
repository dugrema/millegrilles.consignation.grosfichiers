const fs = require('fs')
const FFmpeg = require('fluent-ffmpeg')

const VIDEOS = [
  '/home/mathieu/Videos/IMG_2010.MOV',
  '/home/mathieu/Videos/IMG_0325.MOV',
  '/home/mathieu/Videos/Flying_with_Geese.mov',
  '/home/mathieu/Videos/1136.mpeg',
  '/home/mathieu/Videos/Dr Quantum - Double Slit Experiment.flv',
]
const VIDEO_SEL = 4

describe('convertir video', ()=>{

  test('video 1 probe', async () => {
    var opts = {
      maxHeight: 480,
      maxBitrate: 600000,
    }
    await probeVideo(VIDEOS[VIDEO_SEL])
  })

  test('video 1 mp4', async () => {
    console.debug("Convertir video 1 mp4")
    const opts = {
      videoBitrate: 600000,
      height: 480,
      progressCb: progress,
    }

    await genererVideoMp4(VIDEOS[VIDEO_SEL], '/home/mathieu/Videos/output.mp4')
  }, 1200000)

  test.only('video 1 webm', async () => {
    console.debug("Convertir video 1 webm")
    const opts = {
      videoBitrate: 750000,
      height: 720,
      videoCodec: 'libvpx-vp9',
      audioCodec: 'libopus',
      progressCb: progress,
    }
    await genererVideoMp4(VIDEOS[VIDEO_SEL], '/home/mathieu/Videos/output.webm', opts)
  }, 1200000)

})

function progress(progress) {
  console.debug("Progress : %O", progress)
}

async function probeVideo(sourcePath, opts) {
  opts = opts || {}
  const maxHeight = opts.maxHeight || 720,
        maxBitrate = opts.maxBitrate || 750000

  const resultat = await new Promise((resolve, reject)=>{
    FFmpeg.ffprobe(sourcePath, (err, metadata) => {
      if(err) return reject(err)
      resolve(metadata)
    })
  })

  // console.debug("Resultat probe : %O", resultat)

  const infoVideo = resultat.streams.filter(item=>item.codec_type === 'video')[0]
  // console.debug("Information video : %O", infoVideo)

  // Determiner le bitrate et la taille (verticale) du video pour eviter un
  // upscaling ou augmentation bitrate
  const bitRate = infoVideo.bit_rate,
        height = infoVideo.height

  const tailleEncoding = [720, 480, 360, 240].filter(item=>{
    return item<=height && item < maxHeight
  })[0]
  const bitRateEncoding = [750000, 600000, 500000, 400000, 200000].filter(item=>{
    return item<=bitRate && item < maxBitrate
  })[0]

  console.debug("Information pour encodage : height %d, bit rate %d",
    tailleEncoding, bitRateEncoding)

  return {height: tailleEncoding, bitrate: bitRateEncoding}
}

async function genererVideoMp4(sourcePath, destinationPath, opts) {
  if(!opts) opts = {}

  var   videoBitrate = opts.videoBitrate || 500000
        height = opts.height || 480
  const videoCodec = opts.videoCodec || 'libx264',
        audioCodec = opts.audioCodec || 'aac',
        audioBitrate = opts.audioBitrate || '64k',
        progressCb = opts.progressCb

  var videoInfo = await probeVideo(sourcePath, {maxBitrate: videoBitrate, maxHeight: height})
  videoBitrate = videoInfo.bitrate
  height = videoInfo.height

  videoBitrate = '' + (videoBitrate / 1000) + 'k'
  console.debug('Utilisation video bitrate : %s', videoBitrate)

  // Passe 1
  const ffmpegProcessPass1 = new FFmpeg(sourcePath, {niceness: 10})
    .withVideoBitrate(''+videoBitrate)
    .withSize('?x' + height)
    .videoCodec(videoCodec)
    .outputOptions(['-an', '-f', 'null', '-pass', '1'])

  if(progressCb) {
    ffmpegProcessPass1.on('progress', progressCb)
  }

  const processPromise1 = new Promise((resolve, reject)=>{
    ffmpegProcessPass1.on('error', function(err) {
        console.error('An error occurred: ' + err.message);
        reject(err);
    })
    ffmpegProcessPass1.on('end', function(filenames) {
      resolve()
    })
  })

  console.debug("Debut passe 1")

  ffmpegProcessPass1.saveToFile('/dev/null')
  await processPromise1

  console.debug("Passe 1 terminee")

  // Passe 2
  const ffmpegProcessPass2 = new FFmpeg(sourcePath, {niceness: 10})
    .withVideoBitrate(''+videoBitrate)
    .withSize('?x' + height)
    .videoCodec(videoCodec)
    .audioCodec(audioCodec)
    .audioBitrate(audioBitrate)
    .outputOptions(['-pass', '2', '-movflags', 'faststart'])

  if(progressCb) {
    ffmpegProcessPass2.on('progress', progressCb)
  }

  const processPromise2 = new Promise((resolve, reject)=>{
    ffmpegProcessPass2.on('error', function(err) {
        console.error('An error occurred: ' + err.message);
        reject(err);
    })
    ffmpegProcessPass2.on('end', function(filenames) {
      resolve()
    })
  })

  ffmpegProcessPass2.saveToFile(destinationPath)
  await processPromise2

  return {videoBitrate, audioBitrate, height, videoCodec}
}
