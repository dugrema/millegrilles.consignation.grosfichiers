class TorrentMessages {

  constructor(mq) {
    this.mq = mq;
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback(this.commandeTorrent, ['commande.torrent.#']);
    this.mq.routingKeyManager.addRoutingKeyCallback(this.requeteTorrent, ['requete.torrent.#']);
  }

  // transmettreCertificat() {
  //   let messageCertificat = pki.preparerMessageCertificat();
  //   let fingerprint = messageCertificat.fingerprint;
  //   let messageJSONStr = JSON.stringify(messageCertificat);
  //   this.mq._publish(
  //     'pki.certificat.' + fingerprint, messageJSONStr
  //   );
  //
  //   return fingerprint;
  // }

  commandeTorrent(routingKey, message) {
    console.debug("Commande torrent " + routingKey);
    console.debug(message);
  }

  requeteTorrent(routingKey, message) {
    console.debug("Requete torrent " + routingKey);
    console.debug(message);
  }

}

module.exports = {TorrentMessages};
