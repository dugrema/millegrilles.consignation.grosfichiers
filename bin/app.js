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
    next()
  })

  app.use(express.json());

  app.use(express.urlencoded({ extended: false }));

  app.use(express.static(path.join(__dirname, 'public')));

  const backup = InitialiserBackup()
  app.all('^/backup/*', (req, res, next)=>{debug("BACKUP interne"); next()}, backup)

  app.all('^/fichiers/backup/*', (req, res, next)=>{debug("BACKUP externe"); next()}, backup)

  app.all('^/fichiers/*', (req, res, next)=>{debug("FICHIERS!"); next()}, InitialiserGrosFichiers());

  // catch 404 and forward to error handler
  app.use(function(req, res, next) {
    console.error("Ressource inconnue");
    res.sendStatus(404);
  });

  // error handler
  app.use(function(err, req, res, next) {
    // set locals, only providing error in development
    // res.locals.message = err.message;
    // res.locals.error = req.app.get('env') === 'development' ? err : {};
    //
    // // render the error page
    // res.status(err.status || 500);
    // res.render('error');
    console.error("Erreur generique");
    console.error(err);
    res.sendStatus(err.status || 500);
  });

  return app;
}

module.exports = {initialiser};
