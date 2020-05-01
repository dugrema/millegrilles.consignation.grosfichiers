const {RabbitMQWrapper, decoderMessage} = require('./rabbitMQ')
const {PKIUtils} = require('./pki');

// Constantes
REQUETE_MILLEGRILLES_ACTIVES = 'requete.millegrilles.domaines.Hebergement.requeteMilleGrillesActives';
REQUETE_TROUSSEAU = 'requete.millegrilles.domaines.MaitreDesCles.trousseauHebergement';
COMMANDE_AJOUTER_COMPTE = 'commande.servicemonitor.ajouterCompte';

class HebergementFichiersMQ {

  constructor(mq) {
    this.mq = mq;
    this.millegrilles = {};
  }

  async executer() {

    try {
      console.debug("Transmettre requete pour MilleGrilles hebergees");
      const reponse = await this.mq.transmettreRequete(REQUETE_MILLEGRILLES_ACTIVES, {});
      const content = JSON.parse(reponse.content.toString('utf-8'));
      const milleGrillesHebergees = content.resultats;
      console.debug("MilleGrilles hebergees");
      console.debug(milleGrillesHebergees);

      // Demarrage hebergement des millegrilles manquantes
      for(let idx in milleGrillesHebergees) {
        const infoMilleGrille = milleGrillesHebergees[idx];
        const idmg = infoMilleGrille.idmg;
        if(!this.millegrilles[idmg]) {
          this.demarrerHebergementIdmg(idmg, infoMilleGrille);
        }
      }

      // Desactiver millegrilles qui ne sont plus dans la liste

    } catch(err) {
      console.error("Erreur entretien hebergement");
      console.error(err);
    } finally {
      // Boucle executer entretien
      setTimeout(() => {this.executer()}, 60000);
    }

  }

  async demarrerHebergementIdmg(idmg, infoMilleGrille) {
    console.info("Demarrage hebergement %s", idmg);
    // console.debug(infoMilleGrille);

    const requete = {'idmg': [idmg]}
    const trousseau = decoderMessage(await this.mq.transmettreRequete(REQUETE_TROUSSEAU, requete));
    const {certificat_pem, certificats, cle, motdepasse_chiffre} = trousseau.resultats[0];
    const {hebergement, hote_pem, intermediaire, millegrille} = certificats;

    console.debug("Trousseau pour MilleGrille %s", idmg);
    // console.debug(trousseau);
    // console.debug(certificats);

    // const certChain = [certificat_pem, certificats.intermediaire].join('\n');
    // Batir la chaine de connexion MQ :
    //   1. certificat_pem : certificat de coupdoeil heberge
    //   2. hebergement : certificat XS de la MilleGrille hebergee
    //   3. hote_pem : certificat intermediaire de la MilleGrille hote
    //   4. this.mq.pki.ca : certificat racine de la MilleGrille hote
    const hoteChain = [certificat_pem, hebergement, hote_pem];
    const hoteChainStr = hoteChain.join('\n');

    // Ajouter le compte pour la MilleGrille hebergee
    const commandeAjouterCompte = {
        'certificat': hoteChain[0],
        'chaine': hoteChain.slice(1),
    }
    console.debug("Ajouter compte coupdoeil pour hebergemenet %s", idmg);
    await this.mq.transmettreCommande(COMMANDE_AJOUTER_COMPTE, commandeAjouterCompte)

    // Batir la chaine de validation de la MilleGrille hebergee
    //   1. certificat_pem : certificat de coupdoeil heberge
    //   2. intermediaire : certificat intermediaire de la MilleGrille hebergee
    //   3. millegrille : certificat racine de la MilleGrille hebergee
    const hebergeChain = [certificat_pem, intermediaire];
    const hebergeChainStr = hebergeChain.join('\n');

    const motDePasse = await this.mq.pki.decrypterAsymetrique(motdepasse_chiffre);
    // console.debug("Mot de passe dechiffre : %s", motDePasse);

    const certPems = {
      millegrille: millegrille,
      hoteMillegrille: this.mq.pki.ca,
      cert: hebergeChainStr,
      hote: hoteChainStr,
      key: cle,
      password: motDePasse,
    };
    const pki = new PKIUtils(certPems);
    await pki.chargerPEMs(certPems);
    // console.debug(pki);

    // Connexion a RabbitMQ
    // amqps://mq:5673/[idmg]
    const rabbitMQ = new RabbitMQWrapper(pki);
    const mqConnectionUrl = process.env.MG_MQ_URL;
    await rabbitMQ.connect(mqConnectionUrl);

    this.millegrilles[idmg] = {
      rabbitMQ,
    }
  }

}

module.exports = {HebergementFichiersMQ};
