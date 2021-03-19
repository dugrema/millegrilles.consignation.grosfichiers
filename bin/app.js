const debug = require('debug')('millegrilles:fichiers:app')
var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');

// var indexRouter = require('./routes/index');
const {InitialiserGrosFichiers} = require('../routes/grosfichiers');
const {InitialiserBackup} = require('../routes/backup');
const {verificationCertificatSSL, ValidateurSignature} = require('../util/pki');
const {PathConsignation} = require('../util/traitementFichier')

function initialiser() {

  var app = express();

  // Ajouter composant d'autorisation par certificat client SSL
  app.use(verificationCertificatSSL)

  // Inject RabbitMQ pour la MilleGrille detectee sous etape SSL
  app.use((req, res, next)=>{
    //const idmg = req.autorisationMillegrille.idmg
    const idmg = req.idmg
    //const rabbitMQ = req.fctRabbitMQParIdmg(idmg)
    // req.amqpdao = rabbitMQ  // Nouvelle approche
    req.pathConsignation = new PathConsignation({idmg})
    next()
  })

  app.use(express.json())

  app.use(express.urlencoded({ extended: false }))

  app.use(express.static(path.join(__dirname, 'public')))

  app.all('^/backup/*', InitialiserBackup())
  app.all('^/fichiers/*', InitialiserGrosFichiers())

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
