const crypto = require('crypto');
const x509 = require('x509');
const stringify = require('json-stable-stringify');

class PKIUtils {
  // Classe qui supporte des operations avec certificats et cles privees.

  constructor() {
    let mq_cacert = process.env.MG_MQ_CAFILE,
        mq_cert = process.env.MG_MQ_CERTFILE,
        mq_key = process.env.MG_MQ_KEYFILE;

    this.cacertFile = mq_cacert;
    this.certFile = mq_cert;
    this.keyFile = mq_key;

    this.cle = null;
    this.cert = null;

    this.chargerPEMs();
    this._verifierCertificat();
  }

  chargerPEMs() {
    const fs = require('fs');
    console.log("PKI: Chargement cle " + this.keyFile + " et cert " + this.certFile);
    this.cle = fs.readFileSync(this.keyFile);

    // Charger le certificat pour conserver commonName, fingerprint
    this.chargerCertificat();
  }

  _verifierCertificat() {
    this.getFingerprint();
  }

  chargerCertificat() {
    let parsedCert = x509.parseCert(this.certFile);
    let fingerprint = parsedCert['fingerPrint'];

    // Pour correspondre au format Python, enlever les colons (:) et
    // mettre en lowercase.
    fingerprint = fingerprint.replace(/:/g, '').toLowerCase();
    console.log("Certificat fingerprint: " + fingerprint);

    //console.log(parsedCert);
    this.commonName = parsedCert.subject.commonName;
    console.log("Certificat du noeud, sujet CN: " + this.commonName)

    this.fingerprint = fingerprint;
  }

  getFingerprint() {
    return this.fingerprint;
  }

  getCommonName() {
    return this.commonName;
  }

  signerTransaction(transaction) {

    let signature = 'N/A';
    const sign = crypto.createSign('SHA512');

    // Stringify en json trie
    let transactionJson = stringify(transaction);
    // console.log("Message utilise pour signature: " + transactionJson);

    // Creer algo signature et signer
    sign.write(transactionJson);
    let parametresSignature = {
      "key": this.cle,
      "padding": crypto.constants.RSA_PKCS1_PSS_PADDING
    }
    signature = sign.sign(parametresSignature, 'base64');

    return signature;
  }

  hacherTransaction(transaction) {
    let hachage_transaction = 'N/A';
    const hash = crypto.createHash('sha256');

    // Copier transaction sans l'entete
    let copie_transaction = {};
    for(let elem in transaction) {
      if (elem != 'en-tete') {
        copie_transaction[elem] = transaction[elem];
      }
    }

    // Stringify en json trie
    let transactionJson = stringify(copie_transaction);
    // console.log("Message utilise pour hachage: " + transactionJson);

    // Creer algo signature et signer
    hash.write(transactionJson);
    //hash.end();

    hachage_transaction = hash.digest('base64')

    return hachage_transaction;
  }

};

const pki = new PKIUtils();
module.exports = pki;
