var amqplib = require('amqplib');
var os = require('os');
var fs = require('fs');
var uuidv4 = require('uuid/v4');
var pki = require('./pki.js');

class RabbitMQWrapper {

  constructor() {
    this.idmg = process.env.MG_IDMG;

    this.url = null;
    this.connection = null;
    this.channel = null;
    this.reply_q = null;
    this.consumerTag = null;

    this.compteurMessages = 0;

    this.reconnectTimeout = null; // Timer de reconnexion - null si inactif


    // Correlation avec les reponses en attente.
    // Cle: uuid de CorrelationId
    // Valeur: callback
    this.pendingResponses = {};

    // this.nomMilleGrille = this._trouverNomMilleGrille()
    // this.setHostname();

    this.routingKeyManager = new RoutingKeyManager(this);
    this.routingKeyCertificat = null;

    this.connexionListeners = [];  // Listeners a appeler lors de la connexion

    // Conserver une liste de certificats connus, permet d'eviter la
    // sauvegarder redondante du meme certificat
    this.certificatsConnus = {};
  }

  connect(url) {
    this.url = url;
    return this._connect();
  }

  _connect() {

    let mq_cacert = process.env.MG_MQ_CAFILE,
        mq_cert = process.env.MG_MQ_CERTFILE,
        mq_key = process.env.MG_MQ_KEYFILE;

    if(this.connection === null) {
      let options = {}
      if(mq_cacert !== undefined) {
        var cacert = fs.readFileSync(mq_cacert);
        options['ca'] = [cacert];
      }
      if(mq_cert !== undefined) {
        var cert = fs.readFileSync(mq_cert);
        options['cert'] = cert;
      }
      if(mq_key !== undefined) {
        var key = fs.readFileSync(mq_key);
        options['key'] = key;
      }
      options['credentials'] = amqplib.credentials.external();

      return amqplib.connect(this.url, options)
      .then( conn => {
        console.info("Connexion a RabbitMQ reussie");
        this.connection = conn;

        conn.on('close', (reason)=>{
          console.warn("Fermeture connexion RabbitMQ");
          console.info(reason);
          this.scheduleReconnect();
        });

        return conn.createChannel();
      }).then( (ch) => {
        this.channel = ch;
        console.info("Channel ouvert");
        return this.ecouter();
      // }).then(()=>{
        // console.log("Connexion et channel prets");

        // // Transmettre le certificat
        // let fingerprint = this.transmettreCertificat();

        // Enregistrer routing key du certificat
        // Permet de repondre si un autre composant demande notre certificat
        // this.routingKeyCertificat = 'pki.requete.' + fingerprint;
        // console.debug("Enregistrer routing key: " + fingerprint);
        // this.channel.bindQueue(this.reply_q.queue, 'millegrilles.noeuds', this.routingKeyCertificat);

      }).catch(err => {
        this.connection = null;
        console.error("Erreur connexion RabbitMQ");
        console.error(err);
        this.scheduleReconnect();
      });

    }

  }

  scheduleReconnect() {
    // Met un timer pour se reconnecter
    const dureeAttente = 30;

    if(!this.reconnectTimeout) {
      var mq = this;
      this.reconnectTimeout = setTimeout(()=>{
        console.info("Reconnexion en cours");
        mq.reconnectTimeout = null;
        mq._connect();
      }, dureeAttente*1000);

      console.info("Reconnexion a MQ dans " + dureeAttente + " secondes");

      var conn = this.connection, channel = this.channel;
      this.connection = null;
      this.channel = null;

      if(channel) {
        try {
          channel.close();
        } catch (err) {
          console.debug("Erreur fermeture channel");
          // console.debug(err);
        }
      }

      if(this.connection) {
        try {
          conn.close();
        } catch (err) {
          console.warn("Erreur fermeture connection");
          console.info(err);
        }
      }
    }
  }

  ecouter() {
    var compteurMessages = 0;

    let promise = new Promise((resolve, reject) => {

      // Creer Q pour ecouter
      this.channel.assertQueue('', {
        durable: false,
        exclusive: true,
      })
      .then( (q) => {
        console.log("Queue cree"),
        console.log(q);
        this.reply_q = q;

        // Appeler listeners de connexion
        for(let idx in this.connexionListeners) {
          let listener = this.connexionListeners[idx];
          listener.on_connecter();
        }

        const routingKeyManager = this.routingKeyManager;

        this._consume();

        resolve();
      })
      .catch( err => {
        console.error("Erreur creation Q pour ecouter");
        reject(err);
      })
    });

    return promise;

  }

  _consume() {
    return this.channel.consume(
      this.reply_q.queue,
      async (msg) => {
        // const noMessage = this.compteurMessages;
        // this.compteurMessages = noMessage + 1;

        // console.debug("Message recu " + noMessage);
        let correlationId = msg.properties.correlationId;
        let callback = this.pendingResponses[correlationId];
        if(callback) {
          delete this.pendingResponses[correlationId];
        }

        // let messageContent = decodeURIComponent(escape(msg.content));
        let messageContent = msg.content.toString();
        let routingKey = msg.fields.routingKey;
        let json_message = JSON.parse(messageContent);
        // console.debug(json_message);

        if(routingKey && routingKey.startsWith('pki.certificat.')) {
          // Sauvegarder le certificat localement pour usage futur
          pki.sauvegarderMessageCertificat(messageContent, json_message.fingerprint);
          return; // Ce message ne correspond pas au format standard
        }

        // Valider le contenu du message - hachage et signature
        let hashTransactionCalcule = pki.hacherTransaction(json_message);
        let hashTransactionRecu = json_message['en-tete']['hachage-contenu'];
        if(hashTransactionCalcule !== hashTransactionRecu) {
          console.warn("Erreur hachage incorrect : " + hashTransactionCalcule + ", message dropped");
          console.debug(messageContent);

          return;
        }

        pki.verifierSignatureMessage(json_message)
        .then(signatureValide=>{
          if(signatureValide) {
            this.traiterMessageValide(json_message, msg, callback);
          } else {
            // Cleanup au besoin
            delete this.pendingResponses[correlationId];
          }
        })
        .catch(err=>{
          if(err.inconnu) {
            // Message inconnu, on va verifier si c'est une reponse de
            // certificat.
            if(json_message.resultats && json_message.resultats.certificat_pem) {
              // On laisse le message passer, c'est un certificat
              // console.debug("Certificat recu");
              callback(msg);
            } else {
              // On tente de charger le certificat
              let fingerprint = json_message['en-tete'].certificat;
              console.warn("Certificat inconnu, on fait une demande : " + fingerprint);
              this.demanderCertificat(fingerprint)
              .then(reponse=>{
                console.debug("Reponse demande certificat " + fingerprint);
                // console.debug(reponse);

                var etatCertificat = this.certificatsConnus[fingerprint];

                if(!etatCertificat) {

                  // Creer un placeholder pour messages en attente sur ce
                  // certificat.
                  etatCertificat = {
                    reponse: reponse.resultats,
                    certificatSauvegarde: false,
                    callbacks: [],
                    timer: setTimeout(()=>{console.error("Timeout traitement certificat " + fingerprint);}, 5000),
                  }

                  this.certificatsConnus[fingerprint] = etatCertificat;

                  // Sauvegarder le certificat et tenter de valider le message en attente
                  pki.sauvegarderMessageCertificat(JSON.stringify(reponse.resultats))
                  .then(()=>pki.verifierSignatureMessage(json_message))
                  .then(signatureValide=>{
                    if(signatureValide) {
                      this.traiterMessageValide(json_message, msg, callback);
                      clearTimeout(etatCertificat.timer);

                      etatCertificat.certificatSauvegarde = true;

                      while(etatCertificat.callbacks.length > 0) {
                        const callbackMessage = this.certificatsConnus[fingerprint].callbacks.pop();
                        try {
                          console.debug("Callback apres reception certificat " + fingerprint);
                          callbackMessage();
                        } catch (err) {
                          console.error("Erreur callback certificat " + fingerprint);
                        }
                      }

                      // Cleanup memoire
                      this.certificatsConnus[fingerprint] = {certificatSauvegarde: true};

                    } else {
                      console.warn("Signature invalide, message dropped");
                    }
                  })
                  .catch(err=>{
                    console.warn("Message non valide apres reception du certificat, message dropped");
                    console.debug(err);
                  });

                } else {

                  if(etatCertificat.certificatSauvegarde) {
                    this.traiterMessageValide(json_message, msg, callback);
                  } else {
                    // Inserer callback a executer lors de la reception du certificat
                    etatCertificat.callbacks.push(
                      () => {this.traiterMessageValide(json_message, msg, callback);}
                    );
                  }
                };

              })
              .catch(err=>{
                console.warn("Certificat non charge, message dropped");
                console.debug(err);
              })
            }
          }
        });


        // if(correlationId && this.pendingResponses[correlationId]) {
        //   // On a recu un message de reponse
        //   let callback = this.pendingResponses[correlationId];
        //   if(callback) {
        //     callback(msg);
        //     delete this.pendingResponses[correlationId];
        //   }
        // } else if(routingKey) {
        //   // Traiter le message via handlers
        //   let blockingPromise = this.routingKeyManager.handleMessage(routingKey, messageContent, msg.properties);
        //   if(blockingPromise) {
        //     // console.debug("Promise recue dans consume, on bloque noMessage=" + noMessage);
        //     // On arrete de consommer le temps de traiter le message
        //     this.channel.cancel(this.consumerTag)
        //     .then(()=>{
        //       blockingPromise.finally(()=>{
        //         // console.debug("Resumer consume apres noMessage=" + noMessage);
        //         this._consume();
        //       })
        //     })
        //     .catch(err=>{
        //       console.error("Erreur cancel reader consume");
        //     });
        //   }
        //
        // } else {
        //   console.warn("Recu message sans correlation Id ou routing key");
        //   console.warn(msg);
        // }
        // console.debug("Message traite " + noMessage);
      },
      {noAck: true}
    ).then(tag=>{
      // console.debug("Consumer Tag ");
      // console.debug(tag);
      this.consumerTag = tag.consumerTag;
    })
  }

  traiterMessageValide(json_message, msg, callback) {
    let routingKey = msg.fields.routingKey;
    if(callback) {
      callback(json_message);
    } else if(routingKey) {
      // Traiter le message via handlers
      let blockingPromise = this.routingKeyManager.handleMessage(routingKey, msg.content, msg.properties);
      if(blockingPromise) {
        // console.debug("Promise recue dans consume, on bloque noMessage=" + noMessage);
        // On arrete de consommer le temps de traiter le message
        this.channel.cancel(this.consumerTag)
        .then(()=>{
          blockingPromise.finally(()=>{
            // console.debug("Resumer consume apres noMessage=" + noMessage);
            this._consume();
          })
        })
        .catch(err=>{
          console.error("Erreur cancel reader consume");
        });
      }

    } else {
      console.warn("Recu message sans correlation Id ou routing key");
      console.warn(msg);
    }

  }

  enregistrerListenerConnexion(listener) {
    this.connexionListeners.push(listener);
    if(this.channel) {
      // La connexion existe deja, on force l'execution de l'evenement.
      listener.on_connecter();
    }
  }

  // Utiliser cette methode pour simplifier le formattage d'une transaction.
  // Il faut fournir le contenu de la transaction et le domaine (routing)
  transmettreTransactionFormattee(message, domaine) {
    let messageFormatte = message;  // Meme objet si ca cause pas de problemes
    let infoTransaction = this._formatterInfoTransaction(domaine);
    messageFormatte['en-tete'] = infoTransaction;

    // Signer le message avec le certificat
    this._signerMessage(messageFormatte);

    return this.transmettreEnveloppeTransaction(messageFormatte, domaine);

    // const jsonMessage = JSON.stringify(message);
    //
    // // Transmettre la nouvelle transaction. La promise permet de traiter
    // // le message de reponse.
    // let routingKey = 'transaction.nouvelle';
    // let promise = this._transmettre(routingKey, jsonMessage, correlation);
    //
    // return promise;
  }

  transmettreEnveloppeTransaction(transactionFormattee, domaine) {
    const jsonMessage = JSON.stringify(transactionFormattee);
    const correlation = transactionFormattee['en-tete']['uuid-transaction'];
    const routingKey = 'transaction.nouvelle';
    let promise = this._transmettre(routingKey, jsonMessage, correlation);

    return promise;
  }

  formatterTransaction(domaine, message) {
    let messageFormatte = message;  // Meme objet si ca cause pas de problemes
    let infoTransaction = this._formatterInfoTransaction(domaine);
    const correlation = infoTransaction['uuid-transaction'];
    messageFormatte['en-tete'] = infoTransaction;

    // Signer le message avec le certificat
    this._signerMessage(messageFormatte);
    return messageFormatte;
  }

  // Transmet reponse (e.g. d'une requete)
  // Repond directement a une Q (exclusive)
  transmettreReponse(message, replyTo, correlationId) {
    const messageFormatte = this.formatterTransaction(replyTo, message);
    const jsonMessage = JSON.stringify(messageFormatte);

    // Faire la publication
    return new Promise((resolve, reject)=>{
      this.channel.publish(
        '',
        replyTo,
        Buffer.from(jsonMessage),
        {
          correlationId: correlationId
        },
        function(err, ok) {
          if(err) {
            console.error("Erreur MQ Callback");
            console.error(err);
            reject(err);
            return;
          }
          // console.debug("Reponse transmise");
          // console.debug(ok);
          resolve(ok);
        }
      );
    });

  }

  _formatterInfoTransaction(domaine) {
    // Ces valeurs n'ont de sens que sur le serveur.
    // Calculer secondes UTC (getTime retourne millisecondes locales)
    let dateUTC = (new Date().getTime()/1000) + new Date().getTimezoneOffset()*60;
    let tempsLecture = Math.trunc(dateUTC);
    let sourceSystem = 'coupdoeil/' + 'dev2.maple.mdugre.info' + "@" + pki.getCommonName();
    let infoTransaction = {
      'domaine': domaine,
       //'source-systeme': sourceSystem,
      'idmg': this.idmg,
      'uuid-transaction': uuidv4(),
      'estampille': tempsLecture,
      'certificat': pki.getFingerprint(),
      'hachage-contenu': '',  // Doit etre calcule a partir du contenu
      'version': 6
    };

    return infoTransaction;
  }

  _signerMessage(message) {
    // Produire le hachage du contenu avant de signer - le hash doit
    // etre inclus dans l'entete pour faire partie de la signature.
    let hachage = pki.hacherTransaction(message);
    message['en-tete']['hachage-contenu'] = hachage;

    // Signer la transaction. Ajoute l'information du certificat dans l'entete.
    let signature = pki.signerTransaction(message);
    message['_signature'] = signature;
  }

  // Methode qui permet de transmettre une transaction au backend RabbitMQ
  // Les metadonnees sont ajoutees automatiquement
  _transmettreTransaction(routingKey, message) {
    let jsonMessage = JSON.stringify(message);

    // Le code doit uniquement etre execute sur le serveur
    // console.log("Message: routing=" + routingKey + " message=" + jsonMessage);
    try {
      // console.log("Message a transmettre: " + routingKey + " = " + jsonMessage);
      this.channel.publish(
        'millegrilles.noeuds',
        routingKey,
         new Buffer(jsonMessage),
         {
           correlationId: message['correlation'],
           replyTo: this.reply_q.queue,
         },
         function(err, ok) {
           console.error("Erreur MQ Callback");
           console.error(err);
         }
      );
    }
    catch (e) {
      console.error("Erreur MQ");
      console.error(e);
      this.reconnect(); // Tenter de se reconnecter
    }
  }

  transmettreRequete(routingKey, message) {

    const infoTransaction = this._formatterInfoTransaction(routingKey);

    message['en-tete'] = infoTransaction;
    this._signerMessage(message);

    const correlation = infoTransaction['uuid-transaction'];
    const jsonMessage = JSON.stringify(message);

    // Transmettre requete - la promise permet de traiter la reponse
    const promise = this._transmettre(routingKey, jsonMessage, correlation);
    return promise;
  }

  emettreEvenement(message, routingKey) {

    const infoTransaction = this._formatterInfoTransaction(routingKey);

    message['en-tete'] = infoTransaction;
    this._signerMessage(message);

    const jsonMessage = JSON.stringify(message);

    // Transmettre requete - la promise permet de traiter la reponse
    const promise = this._transmettre(routingKey, jsonMessage);
    return promise;
  }

  _transmettre(routingKey, jsonMessage, correlationId) {
    // Setup variables pour timeout, callback
    let timeout, fonction_callback;

    let promise = new Promise((resolve, reject) => {

      var processed = false;
      const pendingResponses = this.pendingResponses;
      fonction_callback = function(msg, err) {
        // Cleanup du callback
        delete pendingResponses[correlationId];
        clearTimeout(timeout);

        if(msg && !err) {
          resolve(msg);
        } else {
          reject(err);
        }
      };

      // Exporter la fonction de callback dans l'objet RabbitMQ.
      // Permet de faire la correlation lorsqu'on recoit la reponse.
      pendingResponses[correlationId] = fonction_callback;

      // Faire la publication
      this.channel.publish(
        'millegrilles.noeuds',
        routingKey,
        Buffer.from(jsonMessage),
        {
          correlationId: correlationId,
          replyTo: this.reply_q.queue,
        },
        function(err, ok) {
          console.error("Erreur MQ Callback");
          console.error(err);
          delete pendingResponses[correlationId];
          reject(err);
        }
      );

    });

    // Lancer un timer pour permettre d'eviter qu'une requete ne soit
    // jamais nettoyee ou repondue.
    timeout = setTimeout(
      () => {fonction_callback(null, {'err': 'mq.timeout'})},
      15000
    );

    return promise;
  };

  _publish(routingKey, jsonMessage) {
    // Faire la publication
    this.channel.publish(
      'millegrilles.noeuds',
      routingKey,
      Buffer.from(jsonMessage),
      (err, ok) => {
        console.error("Erreur MQ Callback");
        console.error(err);
        if(correlationId) {
          delete pendingResponses[correlationId];
        }
      }
    );
  }

  // Retourne un document en fonction d'un domaine
  get_document(domaine, filtre) {
    // Verifier que la MilleGrille n'a pas deja d'empreinte usager
    let requete = {
      "requetes": [
        {
          "filtre": filtre
        }
      ]
    }
    let promise = this.transmettreRequete(
      'requete.' + domaine,
      requete
    )
    .then((msg) => {
      let messageContent = decodeURIComponent(escape(msg.content));
      let json_message = JSON.parse(messageContent);
      let document_recu = json_message['resultats'][0][0];
      return(document_recu);
    })

    return promise;
  }

  demanderCertificat(fingerprint) {
    var requete = {fingerprint}
    var routingKey = 'requete.millegrilles.domaines.Pki.certificat';
    return this.transmettreRequete(routingKey, requete)
    .then(reponse=>{
      let messageContent = decodeURIComponent(escape(reponse.content));
      let json_message = JSON.parse(messageContent);
      return json_message;
    })
  }

}

class RoutingKeyManager {

  constructor(mq) {

    // Lien vers RabbitMQ, donne acces au channel, Q et routing keys
    this.mq = mq;

    // Dictionnaire de routing keys
    //   cle: string (routing key sur RabbitMQ)
    //   valeur: liste de callbacks
    this.registeredRoutingKeyCallbacks = {};

    this.handleMessage.bind(this);
  }

  async handleMessage(routingKey, messageContent, properties) {
    let callback = this.registeredRoutingKeyCallbacks[routingKey];
    var promise;
    if(callback) {
      let json_message = JSON.parse(messageContent);
      let opts = {
        properties
      }
      promise = callback(routingKey, json_message, opts);
      // if(promise) {
      //   console.debug("Promise recue");
      // } else {
      //   console.debug("Promise non recue");
      // }
    } else {
      console.warn("Routing key pas de callback: " + routingKey);
    }

    return promise;
  }

  addRoutingKeyCallback(callback, routingKeys) {
    for(var routingKey_idx in routingKeys) {
      let routingKeyName = routingKeys[routingKey_idx];
      this.registeredRoutingKeyCallbacks[routingKeyName] = callback;

      // Ajouter la routing key
      console.info("Ajouter callback pour routingKey " + routingKeyName);
      this.mq.channel.bindQueue(this.mq.reply_q.queue, 'millegrilles.noeuds', routingKeyName);
    }
  }

  removeRoutingKeys(routingKeys) {
    for(var routingKey_idx in routingKeys) {
      let routingKeyName = routingKeys[routingKey_idx];
      delete this.registeredRoutingKeyCallbacks[routingKeyName];

      // Retirer la routing key
      console.info("Enlever routingKeys " + routingKeyName);
      this.mq.channel.unbindQueue(this.mq.reply_q.queue, 'millegrilles.noeuds', routingKeyName);
    }
  }

}

const rabbitMQ = new RabbitMQWrapper();

module.exports = rabbitMQ;
