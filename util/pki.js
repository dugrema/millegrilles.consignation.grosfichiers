const crypto = require('crypto');
// const x509 = require('x509');
const forge = require('node-forge');
const stringify = require('json-stable-stringify');
const fs = require('fs');

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
    this.ca = null;

    this.chargerPEMs();
    this._verifierCertificat();
  }

  chargerPEMs() {
    console.log("PKI: Chargement cle " + this.keyFile + " et cert " + this.certFile);
    this.cle = fs.readFileSync(this.keyFile);
    this.ca = fs.readFileSync(this.cacertFile);

    // Charger le certificat pour conserver commonName, fingerprint
    this.chargerCertificat();
  }

  _verifierCertificat() {
    this.getFingerprint();
  }

  chargerCertificat() {
    // let parsedCert = x509.parseCert(this.certFile);
    // let fingerprint = parsedCert['fingerPrint'];
    const pemCert = fs.readFileSync(this.certFile);
    var cert = forge.pki.certificateFromPem(pemCert);
    const fingerprint = forge.md.sha1.create().update(forge.asn1.toDer(forge.pki.certificateToAsn1(cert)).getBytes()).digest().toHex();

    this.cert = cert;

    // Pour correspondre au format Python, enlever les colons (:) et
    // mettre en lowercase.
    // fingerprint = fingerprint.replace(/:/g, '').toLowerCase();
    console.log("Certificat fingerprint: " + fingerprint);

    console.log(cert);
    this.commonName = cert.subject.getField('CN').value;
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

  preparerMessageCertificat() {
    // Retourne un message qui peut etre transmis a MQ avec le certificat
    // utilise par ce noeud. Sert a verifier la signature des transactions.
    let certificatBuffer = fs.readFileSync(this.certFile, 'utf8');

    let transactionCertificat = {
        evenement: 'pki.certificat',
        fingerprint: this.fingerprint,
        certificat_pem: certificatBuffer,
    }

    return transactionCertificat;
  }

};

const pki = new PKIUtils();
module.exports = pki;
