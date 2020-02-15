const crypto = require('crypto');
const forge = require('node-forge');
const stringify = require('json-stable-stringify');
const fs = require('fs');
const path = require('path');

const REPERTOIRE_CERTS_TMP = '/tmp/consignationfichiers.certs';

const PEM_CERT_DEBUT = '-----BEGIN CERTIFICATE-----';
const PEM_CERT_FIN = '-----END CERTIFICATE-----';

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
    this.caStore = null;
    this.caIntermediaires = [];
    this.cacheCertsParFingerprint = {};

    this.chargerPEMs();
    this._verifierCertificat();
  }

  chargerCertificatPEM(pem) {
    let parsedCert = forge.pki.certificateFromPem(pem);
    return parsedCert;
  }

  chargerPEMs() {
    // Preparer repertoire pour sauvegarder PEMS
    fs.mkdir(REPERTOIRE_CERTS_TMP, {recursive: true, mode: 0o700}, e=>{
      if(e) {
        throw new Error(e);
      }
    });

    console.log("PKI: Chargement cle " + this.keyFile + " et cert " + this.certFile);
    this.cle = fs.readFileSync(this.keyFile);
    this.ca = fs.readFileSync(this.cacertFile);

    // Charger le certificat pour conserver commonName, fingerprint
    this.chargerCertificats();
  }

  _verifierCertificat() {
    this.getFingerprint();
  }

  async chargerCertificats() {

    // Charger certificat local
    await new Promise((resolve, reject) => {
      fs.readFile(this.certFile, (err, data)=>{
        if(err) {
          return reject(err);
        }

        console.debug("CERT PEM DATA")
        var certs = splitPEMCerts(data.toString('utf8'));
        // console.debug(certs);

        this.certPEM = certs[0];
        // console.debug(this.certPEM);
        let parsedCert = this.chargerCertificatPEM(this.certPEM);
        // console.debug(parsedCert);

        this.fingerprint = getCertificateFingerprint(parsedCert);
        this.cert = parsedCert;
        this.commonName = parsedCert.subject.getField('CN').value;

        // Sauvegarder certificats intermediaires
        let intermediaire = this.chargerCertificatPEM(certs[1]);
        this.caIntermediaires = [intermediaire];

        // console.log("Certificat du noeud. Sujet CN: " +
        //   this.commonName + ", fingerprint: " + this.fingerprint);
        resolve();
      })
    })
    .catch(err=>{
      throw new Error(err);
    })

    // Creer le CA store pour verifier les certificats.
    let parsedCACert = this.chargerCertificatPEM(this.ca);
    this.caStore = forge.pki.createCaStore([parsedCACert]);
    // console.debug("CA store");
    // console.debug(this.caStore);

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
      if (elem != 'en-tete' && ! elem.startsWith('_')) {
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

  getFingerprintFromForge(cert) {
    return forge.md.sha1.create().update(forge.asn1.toDer(forge.pki.certificateToAsn1(cert)).getBytes()).digest().toHex();
  }

  // Sauvegarde un message de certificat en format JSON
  async sauvegarderMessageCertificat(message, fingerprint) {
    let fichierExiste = fingerprint && await new Promise((resolve, reject)=>{
      if(fingerprint) {
        // Verifier si le fichier existe deja
        let fichier = path.join(REPERTOIRE_CERTS_TMP, fingerprint + '.json');
        fs.access(fichier, fs.constants.F_OK, (err) => {
          let existe = ! err;
          console.debug("Fichier existe ? " + existe);
          resolve(existe);
        });
      } else {
        resolve(false);
      }
    });

    if( ! fichierExiste ) {
      let json_message = JSON.parse(message);
      let certificat_pem = json_message.certificat_pem;

      let certificat = this.chargerCertificatPEM(certificat_pem);
      let fingerprintCalcule = getCertificateFingerprint(certificat);
      let fichier = path.join(REPERTOIRE_CERTS_TMP, fingerprintCalcule + '.json');

      // Sauvegarder sur disque
      fs.writeFile(fichier, message, ()=>{
        console.debug("Fichier certificat " + fingerprintCalcule + ".json sauvegarde");
      });
    } else {
      console.debug("Fichier certificat existe deja : " + fingerprint + ".json");
    }
  }

  // Charge la chaine de certificats pour ce fingerprint
  async getCertificate(fingerprint) {
    let certificat = this.cacheCertsParFingerprint[fingerprint];
    if( ! certificat ) {
      // Verifier si le certificat existe sur le disque
      certificat = await new Promise((resolve, reject)=>{
        let fichier = path.join(REPERTOIRE_CERTS_TMP, fingerprint + '.json');
        let pem = fs.readFile(fichier, (err, data)=>{
          if(err) {
            return reject(err);
          }

          if(!data) {
            return resolve(); // No data
          }

          let messageJson = JSON.parse(data.toString());
          let pem = messageJson.certificat_pem;
          let intermediaires = messageJson.certificats_intermediaires;

          if( ! intermediaires ) {
            // On va tenter d'introduire le certificat de MilleGrille local
            intermediaires = this.caIntermediaires;
          }

          let certificat = this.chargerCertificatPEM(pem);

          let chaine = [certificat, ...intermediaires];
          // console.debug("CHAINE");
          // console.debug(chaine);

          let fingerprintCalcule = getCertificateFingerprint(certificat);
          if(fingerprintCalcule !== fingerprint) {
            // Supprimer fichier invalide
            fs.unlink(fichier, ()=>{});
            return reject('Fingerprint ' + fingerprintCalcule + ' ne correspond pas au fichier : ' + fingerprint + '.json. Fichier supprime.');
          }

          // Valider le certificat avec le store
          let valide = true;
          try {
            forge.pki.verifyCertificateChain(this.caStore, chaine);
          } catch (err) {
            valide = false;
            console.log('Certificate verification failure: ' +
              JSON.stringify(err, null, 2));
          }

          if(valide) {
            this.cacheCertsParFingerprint[fingerprintCalcule] = chaine;
          } else {
            certificat = null;
          }

          resolve(chaine);

        });
      })
      .catch(err=>{
        if(err.code === 'ENOENT') {
          // Fichier non trouve, ok.
        } else {
          console.error("Erreur acces fichier cert");
          console.error(err);
        }
      });
    }
    return certificat;
  }

  // Verifie la signature d'un message
  // Retourne vrai si le message est valide, faux si invalide.
  async verifierSignatureMessage(message) {
    let fingerprint = message['en-tete']['certificat'];
    let signatureBase64 = message['_signature'];
    let signature = Buffer.from(signatureBase64, 'base64');
    let certificatChaine = await this.getCertificate(fingerprint);
    if( ! certificatChaine ) {
      console.debug("Certificat inconnu : " + fingerprint);
      throw new CertificatInconnu("Certificat inconnu : " + fingerprint);
    }
    const certificat = certificatChaine[0];

    let messageFiltre = {};
    for(let cle in message) {
      if( ! cle.startsWith('_') ) {
        messageFiltre[cle] = message[cle];
      }
    }
    // Stringify en ordre (stable)
    messageFiltre = stringify(messageFiltre);

    let keyLength = certificat.publicKey.n.bitLength();
    // Calcul taille salt:
    // http://bouncy-castle.1462172.n4.nabble.com/Is-Bouncy-Castle-SHA256withRSA-PSS-compatible-with-OpenSSL-RSA-PSS-padding-with-SHA256-digest-td4656843.html
    let saltLength = (keyLength - 512) / 8 - 2;
    // console.debug("Salt length: " + saltLength);

    var pss = forge.pss.create({
      md: forge.md.sha512.create(),
      mgf: forge.mgf.mgf1.create(forge.md.sha512.create()),
      saltLength,
      // optionally pass 'prng' with a custom PRNG implementation
    });
    var md = forge.md.sha512.create();
    md.update(messageFiltre, 'utf8');

    try {
      var publicKey = certificat.publicKey;
      let valide = publicKey.verify(md.digest().getBytes(), signature, pss);
      return valide;
    } catch (err) {
      console.debug("Erreur verification signature");
      console.debug(err);
      return false;
    }

  }
};

function getCertificateFingerprint(cert) {
  const fingerprint = forge.md.sha1.create()
    .update(forge.asn1.toDer(forge.pki.certificateToAsn1(cert)).getBytes())
    .digest()
    .toHex();
  return fingerprint;
}

function splitPEMCerts(certs) {
  var splitCerts = certs.split(PEM_CERT_DEBUT).map(c=>{
    return PEM_CERT_DEBUT + c;
  });
  return splitCerts.slice(1);
}

class CertificatInconnu extends Error {
  constructor(message) {
    super(message);
    this.inconnu = true;
  }
}

const pki = new PKIUtils();
module.exports = pki;
