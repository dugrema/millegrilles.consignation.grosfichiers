const debug = require('debug')('millegrilles:fichiers:app')
var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');

// var indexRouter = require('./routes/index');
const {InitialiserGrosFichiers} = require('../routes/grosfichiers');
const {InitialiserBackup} = require('../routes/backup');
const {verificationCertificatSSL, ValidateurSignature} = require('../util/pki');

function initialiser() {

  var app = express();

  // Ajouter composant d'autorisation par certificat client SSL
  app.use(verificationCertificatSSL)

  // Inject RabbitMQ pour la MilleGrille detectee sous etape SSL
  app.use((req, res, next)=>{
    const idmg = req.autorisationMillegrille.idmg
    const rabbitMQ = req.fctRabbitMQParIdmg(idmg)
    req.rabbitMQ = rabbitMQ
    req.amqpdao = rabbitMQ  // Nouvelle approche
    next()
  })

  app.use(express.json())

  app.use(express.urlencoded({ extended: false }))

  app.use(express.static(path.join(__dirname, 'public')))

  const backup = InitialiserBackup()
  app.all('^/backup/*', (req, res, next)=>{debug("BACKUP"); next()}, backup)
  app.all('^/fichiers/*', (req, res, next)=>{debug("FICHIERS"); next()}, InitialiserGrosFichiers())

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
