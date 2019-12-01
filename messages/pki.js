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
    this.fingerprint = this.transmettreCertificat();
    this.routingKeys = ['pki.requete.' + this.fingerprint]

    this.mq.routingKeyManager.addRoutingKeyCallback(this.transmettreCertificat, this.routingKeys);
  }

  transmettreCertificat() {
    let messageCertificat = pki.preparerMessageCertificat();
    let fingerprint = messageCertificat.fingerprint;
    let messageJSONStr = JSON.stringify(messageCertificat);
    this.mq._publish(
      'pki.certificat.' + fingerprint, messageJSONStr
    );

    return fingerprint;
  }


}

module.exports = {PkiMessages};
