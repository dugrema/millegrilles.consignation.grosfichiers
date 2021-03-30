const fs = require('fs')
const FFmpeg = require('fluent-ffmpeg')

const { probeVideo, transcoderVideo } = require('../util/transformationsVideo')

const VIDEOS = [
  '/home/mathieu/Videos/IMG_2010.MOV',
  '/home/mathieu/Videos/IMG_0325.MOV',
  '/home/mathieu/Videos/Flying_with_Geese.mov',
  '/home/mathieu/Videos/1136.mpeg',
  '/home/mathieu/Videos/Dr Quantum - Double Slit Experiment.flv',
  '/home/mathieu/Videos/bennythesupercop.wmv',
]
const VIDEO_SEL = 0

describe('convertir video', ()=>{

  test('video 1 probe', async () => {

    var input = fs.createReadStream(VIDEOS[VIDEO_SEL])
    var opts = {
      maxBitrate: 1000000,
      maxHeight: 1080,
    }
    const resultat = await probeVideo(input, opts)
    console.debug("Resultat probe : %O", resultat)

  })

  test('video 1 mp4', async () => {
    console.debug("Convertir video 1 mp4")
    const opts = {
      videoBitrate: 600000,
      height: 480,
      videoCodec: 'libx264',
      audioCodec: 'aac',
      progressCb: progress,
    }

    const streamFactory = () => {return fs.createReadStream(VIDEOS[VIDEO_SEL])}
    const outputStream = fs.createWriteStream('/home/mathieu/Videos/output.mp4')

    await transcoderVideo(streamFactory, outputStream, opts)
  }, 20 * 60 * 1000)

  test.only('video 1 webm', async () => {
    console.debug("Convertir video 1 webm")
    const opts = {
      videoBitrate: 750000,
      height: 720,
      videoCodec: 'libvpx-vp9',
      audioCodec: 'libopus',
      format: 'webm',
      progressCb: progress,
    }

    const streamFactory = () => {return fs.createReadStream(VIDEOS[VIDEO_SEL])}
    const outputStream = fs.createWriteStream('/home/mathieu/Videos/output.webm')

    await transcoderVideo(streamFactory, outputStream, opts)
  }, 60 * 60 * 1000)

})

function progress(progress, opts) {
  console.debug("Progress : %O", progress)
  const frames = progress.frames,
        framesTotal = progress.framesTotal

  if(frames && framesTotal) {
    // Calculer le % par rapport au frames connus (2e passe)
    let pct = Math.floor(frames / framesTotal * 100)
    console.debug("Progres %d%", pct)
  }
}
