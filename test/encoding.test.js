const fs = require('fs')
const FFmpeg = require('fluent-ffmpeg')
const {creerCipher} = require('@dugrema/millegrilles.common/lib/chiffrage')
const multibase = require('multibase')

const { probeVideo, transcoderVideo } = require('../util/transformationsVideo')
const { gcmStreamReaderFactory, creerOutputstreamChiffrage } = require('../util/cryptoUtils')


const VIDEOS = [
  '/home/mathieu/Videos/IMG_2010.MOV',
  '/home/mathieu/Videos/IMG_0325.MOV',
  '/home/mathieu/Videos/Flying_with_Geese.mov',
  '/home/mathieu/Videos/1136.mpeg',
  '/home/mathieu/Videos/Dr Quantum - Double Slit Experiment.flv',
  '/home/mathieu/Videos/bennythesupercop.wmv',
  '/home/mathieu/Videos/032.MOV',
]
const VIDEO_SEL = 6

const VIDEO_CHIFFRE = '/home/mathieu/Videos/output.mgs2'

var paramsChiffrage = {
  password: 'm4I2rSaVWChni/Q9Kg6oj+aWK7lg4V6w9hi72zsSZzG8',
  iv: 'mEM2Js7U99js0ijvm',
  tag: 'm0oHXG1fLsDDWcHyJJO5qFg',
  hachage_bytes: 'z8VubQk7gsD175ejMUigitntZqhA41VKdNVir4ETxLZKGtsvXyiA5RzpH9KTrmk5sLxrY3uuM34D7MdPoka7U369VMY'
}

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

        paramsChiffrage = {
          password: mbValeur,
          ...resultat.meta
        }

        console.debug("Params chiffrage : %O", paramsChiffrage)

        resolve(resultat)
      })
      readStream.on('error', err=>{reject(err)})
    })
    readStream.read()
  }, 60000)
})

// var input = fs.createReadStream(VIDEOS[VIDEO_SEL])
const streamFactory = _ => {
  const factory = gcmStreamReaderFactory(
    VIDEO_CHIFFRE,
    multibase.decode(paramsChiffrage.password),
    paramsChiffrage.iv, paramsChiffrage.tag
  )
  return factory()
}

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
    const outputCipher = await creerOutputstreamChiffrage(
      CERT_MAITREDESCLES, {fuuid: 'test_doc'}, 'TestDomaine', {DEBUG: false})
    const outputStream = fs.createWriteStream('/home/mathieu/Videos/output.transcode.mgs2')
    outputCipher.pipe(outputStream)

    await transcoderVideo(streamFactory, outputCipher, opts)

    console.debug("Resultat chiffrage : %O", outputCipher.commandeMaitredescles)

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

CERT_MAITREDESCLES = `
-----BEGIN CERTIFICATE-----
MIIEBDCCAuygAwIBAgIUIt+kkcgRnfPrdTEUzrhFiRohxUowDQYJKoZIhvcNAQEL
BQAwgYgxLTArBgNVBAMTJGY4ZjkyZGE4LTU5ZDAtNDZlYy1hNzFmLWE1ZjFmNjFi
ZDlhMjEWMBQGA1UECxMNaW50ZXJtZWRpYWlyZTE/MD0GA1UEChM2ejJXMkVDblA5
ZWF1TlhENjI4YWFpVVJqNnRKZlNZaXlnVGFmZkMxYlRiQ05IQ3RvbWhvUjdzMB4X
DTIxMDMwNzExMzM1NVoXDTIxMDQwNjExMzU1NVowaDE/MD0GA1UECgw2ejJXMkVD
blA5ZWF1TlhENjI4YWFpVVJqNnRKZlNZaXlnVGFmZkMxYlRiQ05IQ3RvbWhvUjdz
MRMwEQYDVQQLDAptYWl0cmVjbGVzMRAwDgYDVQQDDAdtZy1kZXY0MIIBIjANBgkq
hkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA/P0l0LtJrpb08980g7PMoKuXN4klQNxl
9IoKMIAVdFkDp6lvoJAUELlmPlbfHiEVDelvyOx2PY0KJ1A3CGV9p8DA2EUtNcDd
xAqS+VWuq2mQOFxfifEmfshOfGrW9hfGxWOW+y6u1+OU7X3s7AFaXoMYkhxAmzvJ
X/5dYlbydr6vCDuNNftETstaNVt0ZCyQaO31v2IgfVB5HHuW5x+jmXRttVj2Vm2/
q7+Wo7uDwIOMtOyEeQibSltvJHI8Xjv7ALQ3NL8tNTomGmqMAJtm3x88CLSFurs8
5vnW5taYyb5jU5EPKWxxfMJwG1T0k7n/9K6MOle9yGep3Qwc4kiMDwIDAQABo4GE
MIGBMB0GA1UdDgQWBBS87JY7uEPTu8lHdaan34PFseXBNTAfBgNVHSMEGDAWgBRv
FMptlwNhRz7M7fBzV1CKerUVSDAMBgNVHRMBAf8EAjAAMAsGA1UdDwQEAwIE8DAQ
BgQqAwQABAg0LnNlY3VyZTASBgQqAwQBBAptYWl0cmVjbGVzMA0GCSqGSIb3DQEB
CwUAA4IBAQBUPFNbvQSlMsTVN7QPJnyWxDxFpRUbLFm4TGbMuuA1T2FEvMSe1svc
bFD22uW0xfYfFnkvv/Q1EV2tHYhsOzmO19ZOHU/DcoyUeiCZd5uZ9OKDtUyyp+LF
BibtGiDGLIs9qHIK5x8RCLnfhutCgcVluoCbo8x5VjhuWYKSfyGKMC0ogpbBGcIJ
1PKEppEhwNY7Ge0TN0EpFa+IaLyR7ewUQHX/TqzPwzkmcfw6wXdoYeB6liFB9TYm
HPowsHXTRgh09zQC2hYjFMKoWuA7ggxEE7DZGo+1Pz4B7G0yLMUSAEmjjR7Ng+bf
xpXrfd+ozOrpiY5mEq6mYNQV65g/KKnv
-----END CERTIFICATE-----`
