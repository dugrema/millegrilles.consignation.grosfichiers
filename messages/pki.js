const pki = require('../util/pki');

class PkiMessages {

  constructor(mq) {
    this.mq = mq;
    this.fingerprint = null;
    this.routingKeys = null;
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.fingerprint = transmettreCertificat(this.mq);
    this.routingKeys = [
      'pki.requete.' + this.fingerprint,
      'pki.requete.role.fichiers',
    ]

    const mq = this.mq;
    this.mq.routingKeyManager.addRoutingKeyCallback(
      function(routingKeys, message, opts) {transmettreCertificat(mq, routingKeys, message, opts)},
      this.routingKeys
    );
  }

}

function transmettreCertificat(mq, routingKeys, message, opts) {
  // console.debug("transmettreCertificat");
  // console.debug(routingKeys);
  // console.debug(message);
  // console.debug(opts);

  var replyTo, correlationId;
  if(opts && opts.properties) {
    replyTo = opts.properties.replyTo;
    correlationId = opts.properties.correlationId;
  }

  let messageCertificat = pki.preparerMessageCertificat();
  let fingerprint = messageCertificat.fingerprint;

  if(replyTo && correlationId) {
    mq.transmettreReponse(messageCertificat, replyTo, correlationId);
  } else {
    let messageJSONStr = JSON.stringify(messageCertificat);
    mq._publish(
      'pki.certificat.' + fingerprint, messageJSONStr
    );
  }

  return fingerprint;
}

module.exports = {PkiMessages};
