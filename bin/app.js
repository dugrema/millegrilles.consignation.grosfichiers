const debug = require('debug')('millegrilles:fichiers:app')
var express = require('express')
var path = require('path')

const {InitialiserGrosFichiers} = require('../routes/grosfichiers')
// const {InitialiserBackup} = require('../routes/backup')
const {verificationCertificatSSL, ValidateurSignature} = require('../util/pki')

function initialiser(mq, consignationManager, opts) {
  debug("Initialiser app, opts : %O", opts)
  opts = opts || {}
  const middleware = opts.middleware,
        idmg = mq.pki.idmg

  var app = express()

  if(middleware) {
    // Ajouter middleware a mettre en premier
    middleware.forEach(item=>{
      app.use(item)
    })
  }

  // Ajouter composant d'autorisation par certificat client SSL
  app.use(verificationCertificatSSL)

  // Inject RabbitMQ pour la MilleGrille detectee sous etape SSL
  app.use((req, res, next)=>{
    req.consignationManager = consignationManager
    next()
  })

  app.use(express.json())

  app.use(express.urlencoded({ extended: false }))

  app.use(express.static(path.join(__dirname, 'public')))

  // app.all('/backup/*', InitialiserBackup())
  const traitementGrosFichiers = InitialiserGrosFichiers(mq, consignationManager, opts)
  app.all('/fichiers_transfert/*', traitementGrosFichiers)

  // catch 404 and forward to error handler
  app.use(function(req, res, next) {
    console.error("Ressource inconnue");
    res.sendStatus(404);
  })

  // error handler
  app.use(function(err, req, res, next) {
    console.error("Erreur generique\n%O", err);
    res.sendStatus(err.status || 500);
  })

  return app;
}

module.exports = {initialiser};
