const debug = require('debug')('millegrilles:fichiers:amqpdao')
const fs = require('fs')
const { MilleGrillesPKI, MilleGrillesAmqpDAO } = require('@dugrema/millegrilles.nodejs')

const EXPIRATION_MESSAGE_DEFAUT = 30 * 60 * 1000  // 15 minutes en millisec

async function init(storeConsignation, opts) {
  opts = opts || {}

  // Preparer certificats
  const certPem = fs.readFileSync(process.env.MG_MQ_CERTFILE).toString('utf-8')
  const keyPem = fs.readFileSync(process.env.MG_MQ_KEYFILE).toString('utf-8')
  const certMillegrillePem = fs.readFileSync(process.env.MG_MQ_CAFILE).toString('utf-8')

  // Charger certificats, PKI
  const certPems = {
    millegrille: certMillegrillePem,
    cert: certPem,
    key: keyPem,
  }

  // Charger PKI
  const instPki = new MilleGrillesPKI()
  const qCustom = {
    'backup': {name: 'fichiers/backup', autostart: false},
    'publication': {name: 'fichiers/publication'},
    'actions': {name: 'fichiers/actions'},
  }
  const amqpdao = new MilleGrillesAmqpDAO(instPki, {qCustom, exchange: '2.prive'})
  await instPki.initialiserPkiPEMS(certPems)

  if(opts.redisClient) {
    // Injecter le redisClient dans pki
    instPki.redisClient = opts.redisClient
  }

  // Connecter a MilleGrilles avec AMQP DAO
  const mqConnectionUrl = process.env.MG_MQ_URL
  await amqpdao.connect(mqConnectionUrl)

  // Attacher les evenements, cles de routage
  await initialiserMessageHandlers(amqpdao, storeConsignation)

  // Middleware, injecte l'instance
  const middleware = (req, res, next) => {
    req.amqpdao = amqpdao
    next()
  }

  return {middleware, amqpdao}
}

async function initialiserMessageHandlers(rabbitMQ, storeConsignation) {
  // Creer objets de connexion a MQ - importer librairies requises
  const {PkiMessages} = require('../messages/pki');
  rabbitMQ.enregistrerListenerConnexion(new PkiMessages(rabbitMQ));

  // const {TorrentMessages} = require('../messages/torrent');
  // rabbitMQ.enregistrerListenerConnexion(new TorrentMessages(rabbitMQ));

  // const {DecrypterFichier} = require('../messages/crypto');
  // rabbitMQ.enregistrerListenerConnexion(new DecrypterFichier(rabbitMQ));

  // const {GenerateurMedia} = require('../messages/media');
  // rabbitMQ.enregistrerListenerConnexion(new GenerateurMedia(rabbitMQ));

  // const {PublicateurAWS} = require('../messages/aws');
  // rabbitMQ.enregistrerListenerConnexion(new PublicateurAWS(rabbitMQ));

  const backup = require('../messages/backup')
  backup.init(rabbitMQ, storeConsignation)
  rabbitMQ.enregistrerListenerConnexion(backup)

  const publication = require('../messages/publication')
  publication.init(rabbitMQ, storeConsignation)
  rabbitMQ.enregistrerListenerConnexion(publication)

  const entretien = require('../messages/entretien')
  entretien.init(rabbitMQ, storeConsignation)
  rabbitMQ.enregistrerListenerConnexion(entretien)

  const actions = require('../messages/actions')
  actions.init(rabbitMQ, storeConsignation)
  rabbitMQ.enregistrerListenerConnexion(actions)

  return {rabbitMQ};
}

module.exports = {init}
