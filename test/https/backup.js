const debug = require('debug')('test')
const axios = require('axios')
const fs = require('fs')
const https = require('https')

const CERT_CA_FILE = process.env.MG_MQ_CAFILE,
      CERT_FILE = process.env.MG_MQ_CERTFILE,
      KEY_CA_FILE = process.env.MG_MQ_KEYFILE

// Preparer certificats, mots de passe
function chargerCredendials() {
    const credentials = {
      millegrille: fs.readFileSync(CERT_CA_FILE).toString('utf-8'),
      cert: fs.readFileSync(CERT_FILE).toString('utf-8'),
      key: fs.readFileSync(KEY_CA_FILE).toString('utf-8'),
    }
    return credentials
}

const credentials = chargerCredendials()

const _httpsAgent = new https.Agent({
    rejectUnauthorized: false,
    ca: credentials.millegrille,
    cert: credentials.cert,
    key: credentials.key,
})

debug("Run test")
axios({
    method: 'POST', 
    url: 'https://localhost:444/fichiers/test',
    httpsAgent: _httpsAgent,
}).then(reponse=>{
    debug("Resultat requete : %O", reponse)
}).catch(err=>{
    debug("ERR : Erreur axios : %O", err)
})
