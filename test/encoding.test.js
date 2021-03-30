const fs = require('fs')
const FFmpeg = require('fluent-ffmpeg')
const {creerCipher} = require('@dugrema/millegrilles.common/lib/chiffrage')
const multibase = require('multibase')

const { probeVideo, transcoderVideo } = require('../util/transformationsVideo')
const { gcmStreamReaderFactory } = require('../util/cryptoUtils')

const VIDEOS = [
  '/home/mathieu/Videos/IMG_2010.MOV',
  '/home/mathieu/Videos/IMG_0325.MOV',
  '/home/mathieu/Videos/Flying_with_Geese.mov',
  '/home/mathieu/Videos/1136.mpeg',
  '/home/mathieu/Videos/Dr Quantum - Double Slit Experiment.flv',
  '/home/mathieu/Videos/bennythesupercop.wmv',
]
const VIDEO_SEL = 0

const VIDEO_CHIFFRE = '/home/mathieu/Videos/output.mgs2'

describe('preparation fichier chiffre', () => {
  test('chiffrer', async () => {
    console.debug("Chiffrer")
    const cipher = await creerCipherChiffrage()
    const readStream = fs.createReadStream(VIDEOS[VIDEO_SEL])
    const writeStream = fs.createWriteStream(VIDEO_CHIFFRE)
    readStream.on('data', data=>{
      // console.debug("Data recu : %O", data)
      let cipherText = cipher.update(data)
      writeStream.write(cipherText)
    })

    await new Promise((resolve, reject)=>{
      readStream.on('end', async _ =>{
        var resultat = await cipher.finish()
        console.debug("Resultat : %O", resultat)

        var mbValeur = multibase.encode('base64', resultat.password)
        mbValeur = String.fromCharCode.apply(null, mbValeur)
        console.debug("Password en multibase : %s", mbValeur)

        resolve(resultat)
      })
      readStream.on('error', err=>{reject(err)})
    })
    readStream.read()
  }, 60000)
})

var paramsChiffrage = {
  password: 'mLBZe5OGUyH2/HAebuM4RPEdpOtA7I+NQfZ/ofV6r2MM',
  iv: 'mA5dYPHho39l/fGSS',
  tag: 'm4shMtiBolpilcBwHBCNVXA',
  hachage_bytes: 'z8VwZJEariwZsJUPYFUthn9fZEk5P5QXSvwKexm5yCXuun1oBNiTvwTBKb9qHzL6RE4R4WZvEL3LaLLkZSX9wwnBdHo'
}

// var input = fs.createReadStream(VIDEOS[VIDEO_SEL])
const streamFactory = gcmStreamReaderFactory(
  VIDEO_CHIFFRE,
  multibase.decode(paramsChiffrage.password),
  paramsChiffrage.iv, paramsChiffrage.tag
)

describe('convertir video', ()=>{

  test('video 1 probe', async () => {

    // var input = fs.createReadStream(VIDEOS[VIDEO_SEL])
    var opts = {
      maxBitrate: 1000000,
      maxHeight: 1080,
    }
    const resultat = await probeVideo(streamFactory(), opts)
    console.debug("Resultat probe : %O", resultat)
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

    // const streamFactory = () => {return fs.createReadStream(VIDEOS[VIDEO_SEL])}
    const outputStream = fs.createWriteStream('/home/mathieu/Videos/output.mp4')

    await transcoderVideo(streamFactory, outputStream, opts)
  }, 20 * 60 * 1000)

  test('video 1 webm', async () => {
    console.debug("Convertir video 1 webm")
    const opts = {
      videoBitrate: 750000,
      height: 720,
      videoCodec: 'libvpx-vp9',
      audioCodec: 'libopus',
      format: 'webm',
      progressCb: progress,
    }

    // const streamFactory = () => {return fs.createReadStream(VIDEOS[VIDEO_SEL])}
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

async function creerCipherChiffrage() {
  const cipher = await creerCipher()

  const cipherWrapper = {
    update: cipher.update,
    finish: async () => {
      const infoChiffrage = await cipher.finish()
      return infoChiffrage
    }
  }

  return cipherWrapper
}
