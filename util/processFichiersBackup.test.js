const processFichiersBackup = require('./processFichiersBackup')
const tmp = require('tmp')
const path = require('path')
const fs = require('fs')
const tar = require('tar')
const { calculerHachageFichier } = require('./utilitairesHachage')

var fichiersTmp = []

// Mocks
var tmpdir
const pathConsignation = {
  trouverPathBackupDomaine: domaine=>{return tmpdir.name},
  trouverPathBackupHoraire: domaine=>{return path.join(tmpdir.name, 'horaire')},
  trouverPathBackupSnapshot: domaine=>{return path.join(tmpdir.name, 'snapshot')},
  // consignationPathBackup: tmpdir.name,
  trouverPathFuuidExistant: fuuid=>{return path.join(tmpdir.name, fuuid + '.mgs1')}
}

describe('processFichiersBackup', ()=>{

  beforeEach(()=>{
    // Creer repertoire temporaire
    tmpdir = tmp.dirSync()
    pathConsignation.consignationPathBackup = tmpdir.name
    // console.info("TMP dir : %s", tmpdir.name)
  })

  afterEach(()=>{
    // Nettoyer repertoire temporaire
    for(let i in fichiersTmp) {
      const file = fichiersTmp[i]
      try { fs.unlinkSync(file) } catch (err) {}
    }

    fs.rmdir(tmpdir.name, {recursive: true}, err=>{
      if(err) console.error("Erreur suppression %s", tmpdir.name)
    })
    //tmpdir.removeCallback()
  })

  test('devrait etre importe', ()=>{
    expect(processFichiersBackup).toBeInstanceOf(Object);
  });

  it('traiterFichiersBackup backup regulier', async ()=>{
    // Traiter un fichier de backup horaire avec catalogue et transactions
    const amqpdao = {
      pki: {
        verifierSignatureMessage: message => {return true}
      }
    }

    // Creer fichier pour catalogue, hook supprimer tmp
    await creerFichierDummy(path.join(tmpdir.name, 'fichier1.txt.init'), {
      domaine: 'domaine.test',
      hachage_transactions: 'sha512_b64:1+ROfpt6khAjwaJfsH274cNSZlpekgjU9iUTuyOTCM8htjm53L8eMJP7qy4v6MGx9CbU5O0z9AdBNiFW73YsVQ==',
    }, {lzma: true})
    creerFichierDummy(path.join(tmpdir.name, 'fichier2.txt.init'), 'Transaction')

    const fichiersTransactions = {
        originalname: 'fichier2.txt',
        path: path.join(tmpdir.name, 'fichier2.txt.init'),
      },
      fichierCatalogue = {
        originalname: 'fichier1.txt',
        path: path.join(tmpdir.name, 'fichier1.txt.init'),
      }

    const resultat = await processFichiersBackup.traiterFichiersBackup(amqpdao, pathConsignation, fichiersTransactions, fichierCatalogue)
    console.info("Resultat hachage : %O", resultat)

    const infoCatalogue = fs.statSync(path.join(tmpdir.name, 'horaire/fichier1.txt'))
    expect(infoCatalogue).toBeDefined()
    const infoTransaction = fs.statSync(path.join(tmpdir.name, 'horaire/fichier2.txt'))
    expect(infoTransaction).toBeDefined()

    expect(resultat['ok']).toBe(true)

  })

  it('traiterFichiersBackup snapshot nouveau', async ()=>{
    // Traiter un fichier de backup horaire avec catalogue et transactions
    const amqpdao = {
      pki: {
        verifierSignatureMessage: message => {return true}
      }
    }

    // Creer fichier pour catalogue, hook supprimer tmp
    await creerFichierDummy(path.join(tmpdir.name, 'fichier1.txt.init'), {
      domaine: 'domaine.test',
      hachage_transactions: 'sha512_b64:1+ROfpt6khAjwaJfsH274cNSZlpekgjU9iUTuyOTCM8htjm53L8eMJP7qy4v6MGx9CbU5O0z9AdBNiFW73YsVQ==',
      snapshot: true,
    }, {lzma: true})
    creerFichierDummy(path.join(tmpdir.name, 'fichier2.txt.init'), 'Transaction')

    const fichiersTransactions = {
        originalname: 'fichier2.txt',
        path: path.join(tmpdir.name, 'fichier2.txt.init'),
      },
      fichierCatalogue = {
        originalname: 'fichier1.txt',
        path: path.join(tmpdir.name, 'fichier1.txt.init'),
      }

    const resultat = await processFichiersBackup.traiterFichiersBackup(amqpdao, pathConsignation, fichiersTransactions, fichierCatalogue)
    console.info("Resultat hachage : %O", resultat)

    const infoCatalogue = fs.statSync(path.join(tmpdir.name, 'snapshot/catalogue.json.xz'))
    expect(infoCatalogue).toBeDefined()
    const infoTransaction = fs.statSync(path.join(tmpdir.name, 'snapshot/transactions.jsonl.xz.mgs1'))
    expect(infoTransaction).toBeDefined()

    expect(resultat['ok']).toBe(true)
  })

  it('traiterFichiersBackup snapshot override', async ()=>{
    // Traiter un fichier de backup horaire avec catalogue et transactions
    const amqpdao = {
      pki: {
        verifierSignatureMessage: message => {return true}
      }
    }

    // Creer deux sets de backup snapshot
    await creerFichierDummy(path.join(tmpdir.name, 'fichier1.txt.init'), {
      domaine: 'domaine.test',
      hachage_transactions: 'sha512_b64:1+ROfpt6khAjwaJfsH274cNSZlpekgjU9iUTuyOTCM8htjm53L8eMJP7qy4v6MGx9CbU5O0z9AdBNiFW73YsVQ==',
      snapshot: true,
      valeur: 1,
    }, {lzma: true})
    creerFichierDummy(path.join(tmpdir.name, 'fichier2.txt.init'), 'Transaction')

    await creerFichierDummy(path.join(tmpdir.name, 'fichier3.txt.init'), {
      domaine: 'domaine.test',
      hachage_transactions: 'sha512_b64:1+ROfpt6khAjwaJfsH274cNSZlpekgjU9iUTuyOTCM8htjm53L8eMJP7qy4v6MGx9CbU5O0z9AdBNiFW73YsVQ==',
      snapshot: true,
      valeur: 2,
    }, {lzma: true})
    creerFichierDummy(path.join(tmpdir.name, 'fichier4.txt.init'), 'Transaction')

    // Premier snapshot
    fichiersTransactions = {
      originalname: 'fichier2.txt',
      path: path.join(tmpdir.name, 'fichier2.txt.init'),
    }
    fichierCatalogue = {
      originalname: 'fichier1.txt',
      path: path.join(tmpdir.name, 'fichier1.txt.init'),
    }
    const resultat1 = await processFichiersBackup.traiterFichiersBackup(amqpdao, pathConsignation, fichiersTransactions, fichierCatalogue)
    console.info("Resultat hachage : %O", resultat1)
    expect(fs.statSync(path.join(tmpdir.name, 'snapshot/catalogue.json.xz'))).toBeDefined()
    expect(fs.statSync(path.join(tmpdir.name, 'snapshot/transactions.jsonl.xz.mgs1'))).toBeDefined()
    expect(resultat1['ok']).toBe(true)

    // Deuxieme snapshot, devrait ecraser le premier snapshot
    fichiersTransactions = {
      originalname: 'fichier4.txt',
      path: path.join(tmpdir.name, 'fichier4.txt.init'),
    }
    fichierCatalogue = {
      originalname: 'fichier3.txt',
      path: path.join(tmpdir.name, 'fichier3.txt.init'),
    }
    const resultat2 = await processFichiersBackup.traiterFichiersBackup(amqpdao, pathConsignation, fichiersTransactions, fichierCatalogue)
    console.info("Resultat hachage : %O", resultat2)
    expect(fs.statSync(path.join(tmpdir.name, 'snapshot/catalogue.json.xz'))).toBeDefined()
    expect(fs.statSync(path.join(tmpdir.name, 'snapshot/transactions.jsonl.xz.mgs1'))).toBeDefined()
    expect(resultat2['ok']).toBe(true)

    const catalogue2 = await processFichiersBackup.chargerLzma(path.join(tmpdir.name, 'snapshot/catalogue.json.xz'))
    expect(catalogue2.valeur).toBe(2)
  })


  it('sauvegarderFichiersApplication', async () => {
    // Tester la sauvegarde du catalogue (dict) et transfere le fichier
    // d'archive vers le repertoire de backup d'application

    const pathApplication = path.join(tmpdir.name, 'backup', 'application.txt'),
          pathBackupApplication = path.join(tmpdir.name, 'backup')

    fs.mkdirSync(pathBackupApplication)

    creerFichierDummy(pathApplication, 'dadido')

    catalogue = {
      archive_nomfichier: 'application.txt',
      catalogue_nomfichier: 'catalogue',
    }

    expect.assertions(2)
    return processFichiersBackup.sauvegarderFichiersApplication(catalogue, {path: pathApplication}, pathBackupApplication)
    .then(()=>{
      // Verifier que le catalogue et archive application sont crees
      const pathApplication = path.join(tmpdir.name, 'backup', 'application.txt')
      expect(fs.statSync(pathApplication)).toBeDefined()

      const pathCatalogue = path.join(tmpdir.name, 'backup', 'catalogue')
      expect(fs.statSync(pathCatalogue)).toBeDefined()
    })
  })

  it('rotationArchiveApplication aucunes archives', async () => {
    const pathBackupApplication = path.join(tmpdir.name, 'backup')

    expect.assertions(1)
    return processFichiersBackup.rotationArchiveApplication(pathBackupApplication)
    .then(result=>{
      expect(true).toBeDefined()  // On veut juste une invocation sans erreurs
    })
  })

  it('rotationArchiveApplication 1 archive', async () => {
    const pathCatalogue = path.join(tmpdir.name, 'catalogue.1.json')
    const pathApplication = path.join(tmpdir.name, 'application.1.tar.xz')

    creerFichierDummy(pathApplication, 'dadido')
    creerFichierDummy(pathCatalogue, JSON.stringify({
      archive_nomfichier: 'application.1.tar.xz',
      catalogue_nomfichier: 'catalogue.1.json',
      'en-tete': {estampille: 1}
    }))

    expect.assertions(2)
    // 1 archive existante, rien ne devrait changer
    return processFichiersBackup.rotationArchiveApplication(tmpdir.name)
    .then(result=>{
      expect(fs.statSync(pathCatalogue)).toBeDefined()
      expect(fs.statSync(pathApplication)).toBeDefined()
    })
  })

  it('rotationArchiveApplication 2 archives', async () => {
    const pathCatalogues = [], pathApplications = []
    for(let i=0; i<2; i++) {
      pathCatalogues.push(path.join(tmpdir.name, `catalogue.${i}.json`))
      pathApplications.push(path.join(tmpdir.name, `application.${i}.tar.xz`))

      creerFichierDummy(pathApplications[i], 'dadido')
      creerFichierDummy(pathCatalogues[i], JSON.stringify({
        archive_nomfichier: `application.${i}.tar.xz`,
        catalogue_nomfichier: `catalogue.${i}.json`,
        'en-tete': {estampille: i}
      }))
    }

    expect.assertions(4)
    // 1 archive existante, rien ne devrait changer
    return processFichiersBackup.rotationArchiveApplication(tmpdir.name)
    .then(result=>{
      for(let i=0; i<2; i++) {
        var pathCatalogue = pathCatalogues[i]
        var pathApplication = pathApplications[i]
        expect(fs.statSync(pathCatalogue)).toBeDefined()
        expect(fs.statSync(pathApplication)).toBeDefined()
      }
    })
  })

  it('rotationArchiveApplication 3 archives', async () => {
    // La premiere (plus vieille archive va etre supprimee)

    const pathCatalogues = [], pathApplications = []
    for(let i=0; i<3; i++) {
      pathCatalogues.push(path.join(tmpdir.name, `catalogue.${i}.json`))
      pathApplications.push(path.join(tmpdir.name, `application.${i}.tar.xz`))

      creerFichierDummy(pathApplications[i], 'dadido')
      creerFichierDummy(pathCatalogues[i], JSON.stringify({
        archive_nomfichier: `application.${i}.tar.xz`,
        catalogue_nomfichier: `catalogue.${i}.json`,
        'en-tete': {estampille: i}
      }))
    }

    expect.assertions(6)
    // 1 archive existante, rien ne devrait changer
    return processFichiersBackup.rotationArchiveApplication(tmpdir.name)
    .then(result=>{
      for(let i=1; i<3; i++) {
        var pathCatalogue = pathCatalogues[i]
        var pathApplication = pathApplications[i]
        expect(fs.statSync(pathCatalogue)).toBeDefined()
        expect(fs.statSync(pathApplication)).toBeDefined()
      }

      // S'assurer que la premiere (plus vieille) archive et catalogue sont supprimes
      expect(()=>fs.statSync(pathCatalogues[0])).toThrow()
      expect(()=>fs.statSync(pathApplications[0])).toThrow()

    })
  })

  it('traiterFichiersApplication', async () => {
    var appels_transmettreEnveloppeTransaction = []
    const amqpdao = {
      transmettreEnveloppeTransaction: (transaction)=>{
        appels_transmettreEnveloppeTransaction.push(transaction)
        return ''
      }
    }

    const transactionCatalogue = {
            archive_hachage: 'sha512_b64:m9+VOgb9/QzOb/myd745kqAk2XFl308AgjUTBpS4NpTJeELbY39FfPxsZsECbGX/cyIeaVrISi2v4Z7XsMAGQg==',
            archive_nomfichier: 'application.txt',
            catalogue_nomfichier: 'catalogue.json',
          },
          transactionMaitreDesCles = {},
          fichierApplication = path.join(tmpdir.name, 'application.txt')

    creerFichierDummy(fichierApplication, 'dadido')

    expect.assertions(1)
    return processFichiersBackup.traiterFichiersApplication(
      amqpdao, transactionCatalogue, transactionMaitreDesCles, {path: fichierApplication}, tmpdir.name)
      .then(()=>{
        expect(appels_transmettreEnveloppeTransaction.length).toBe(2)
      })
  })


  it('sauvegarderCatalogueQuotidien', async () => {

    const catalogue = {
      domaine: 'test',
      securite: '1.public',
      jour: new Date("2020-01-01 12:00").getTime()/1000,
    }

    const resultat = await processFichiersBackup.sauvegarderCatalogueQuotidien(
      pathConsignation, catalogue)

    // console.debug("Resultat : %O", resultat)
    expect(resultat.path).toBe(`${tmpdir.name}/test_catalogue_20200101.json.xz`)
    expect(resultat.nomFichier).toBe('test_catalogue_20200101.json.xz')
    expect(resultat.dateFormattee).toBe('20200101')

    expect(fs.statSync(`${tmpdir.name}/test_catalogue_20200101.json.xz`)).toBeDefined()

  })

  it('sauvegarderCatalogueAnnuel', async () => {

    const catalogue = {
      domaine: 'test',
      securite: '1.public',
      annee: new Date("2020-01-01 12:00").getTime()/1000,
    }

    const resultat = await processFichiersBackup.sauvegarderCatalogueAnnuel(
      pathConsignation, catalogue)

    // console.debug("Resultat : %O", resultat)
    expect(resultat.path).toBe(`${tmpdir.name}/test_catalogue_2020.json.xz`)
    expect(resultat.nomFichier).toBe('test_catalogue_2020.json.xz')
    expect(resultat.dateFormattee).toBe('2020')

    expect(fs.statSync(`${tmpdir.name}/test_catalogue_2020.json.xz`)).toBeDefined()

  })

  it('traiterBackupQuotidien sans grosfichiers', async () => {
    var appels_transmettreEnveloppeTransaction = []
    const amqpdao = {
      transmettreEnveloppeTransaction: (transaction)=>{
        appels_transmettreEnveloppeTransaction.push(transaction)
        return ''
      },
      formatterTransaction: (domaine, transaction) => {
        transaction['en-tete'] = {domaine}
        transaction['_signature'] = 'dummy'
        return transaction
      }
    }

    fs.mkdirSync(path.join(tmpdir.name, 'horaire'))
    await processFichiersBackup.sauvegarderLzma(path.join(tmpdir.name, 'horaire', 'catalogue_horaire_00.json.xz'), {
      transactions_nomfichier: 'transactions_00.jsonl.xz',
      transactions_hachage: 'sha512_b64:xSsI8/pzk+sB4lrKS13PJfM34MOd/Now8/TcGGaCrfnZTCvOLIgRfvp060A8MdvopXN9N1mWC6PeY6vJN4Lr6g==',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      heure: new Date('2020-01-01 00:00').getTime()/1000,
    })
    creerFichierDummy(path.join(tmpdir.name, 'horaire', 'transactions_00.jsonl.xz'), 'dadadon')

    await processFichiersBackup.sauvegarderLzma(path.join(tmpdir.name, 'horaire', 'catalogue_horaire_01.json.xz'), {
      transactions_nomfichier: 'transactions_01.jsonl.xz',
      transactions_hachage: 'sha512_b64:ssgOVrjt8LGsh5PS/qorlWooxKsrNPqaYCGbEnJsSC2u3wIaxiluAZbBrVI/g1dLNF3qV5NDgA5VTJdkuyKNbg==',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      heure: new Date('2020-01-01 01:00').getTime()/1000,
    })
    creerFichierDummy(path.join(tmpdir.name, 'horaire', 'transactions_01.jsonl.xz'), 'dadada')

    const catalogue = {
      fichiers_horaire: {
        '00': {
          catalogue_nomfichier: 'catalogue_horaire_00.json.xz',
          transactions_nomfichier: 'transactions_00.jsonl.xz',
          catalogue_hachage: '',
          transactions_hachage: 'sha512_b64:xSsI8/pzk+sB4lrKS13PJfM34MOd/Now8/TcGGaCrfnZTCvOLIgRfvp060A8MdvopXN9N1mWC6PeY6vJN4Lr6g==',
        }
      },
      'en-tete': {
        domaine: 'domaine.test'
      },
      '_signature': 'DADADA',
      domaine: 'domaine.test',
      securite: '1.public',
      jour: new Date("2020-01-01").getTime()/1000,
    }

    expect.assertions(9)
    return processFichiersBackup.traiterBackupQuotidien(amqpdao, pathConsignation, catalogue)
    .then(resultat=>{
      // console.debug("Resultat : %O", resultat)

      expect(resultat.archive_hachage).toBeDefined()
      expect(resultat.grosfichiers).toBeUndefined()
      expect(resultat.archive_nomfichier).toBe('domaine.test_20200101.tar')
      expect(resultat.fichiersInclure[0]).toBe('domaine.test_catalogue_20200101.json.xz')
      expect(resultat.fichiersInclure[1]).toBe('horaire/catalogue_horaire_00.json.xz')
      expect(resultat.fichiersInclure[2]).toBe('horaire/transactions_00.jsonl.xz')
      expect(resultat.fichiersInclure[3]).toBe('horaire/catalogue_horaire_01.json.xz')
      expect(resultat.fichiersInclure[4]).toBe('horaire/transactions_01.jsonl.xz')

      const fichierTar = fs.statSync(path.join(tmpdir.name, 'domaine.test_20200101.tar'))
      // console.info("Fichier tar: %O", fichierTar)
      expect(fichierTar.size).toBeGreaterThan(2048)

    })
  })

  // it('traiterBackupQuotidien avec 2 grosfichiers', async () => {
  //   var appels_transmettreEnveloppeTransaction = []
  //   const amqpdao = {
  //     transmettreEnveloppeTransaction: (transaction)=>{
  //       appels_transmettreEnveloppeTransaction.push(transaction)
  //       return ''
  //     },
  //     formatterTransaction: (domaine, transaction) => {
  //       transaction['en-tete'] = {domaine, 'uuid-transaction': 'uuid-dummy'}
  //       transaction['_signature'] = 'dummy'
  //       return transaction
  //     }
  //   }
  //
  //   fs.mkdirSync(path.join(tmpdir.name, 'horaire'))
  //   await processFichiersBackup.sauvegarderLzma(path.join(tmpdir.name, 'horaire', 'catalogue_horaire_02.json.xz'), {
  //     transactions_nomfichier: 'transactions_02.jsonl.xz',
  //     transactions_hachage: 'sha512_b64:xSsI8/pzk+sB4lrKS13PJfM34MOd/Now8/TcGGaCrfnZTCvOLIgRfvp060A8MdvopXN9N1mWC6PeY6vJN4Lr6g==',
  //     'en-tete': {
  //       hachage_contenu: 'dummy',
  //       'uuid-transaction': 'uuid-catalogue-02'
  //     },
  //     heure: new Date('2020-01-01 02:00').getTime()/1000,
  //     fuuid_grosfichiers: {
  //       'abcd-1234': {hachage: 'sha512_b64:xSsI8/pzk+sB4lrKS13PJfM34MOd/Now8/TcGGaCrfnZTCvOLIgRfvp060A8MdvopXN9N1mWC6PeY6vJN4Lr6g=='},
  //       'abcd-1235': {hachage: 'sha512_b64:ssgOVrjt8LGsh5PS/qorlWooxKsrNPqaYCGbEnJsSC2u3wIaxiluAZbBrVI/g1dLNF3qV5NDgA5VTJdkuyKNbg=='}
  //     }
  //   })
  //   creerFichierDummy(path.join(tmpdir.name, 'horaire', 'transactions_02.jsonl.xz'), 'dadadon')
  //
  //   // Grosfichiers
  //   creerFichierDummy(path.join(tmpdir.name, 'abcd-1234.mgs1'), 'dadadon')
  //   creerFichierDummy(path.join(tmpdir.name, 'abcd-1235.mgs1'), 'dadada')
  //
  //   const catalogue = {
  //     fichiers_horaire: {
  //       '02': {
  //         catalogue_nomfichier: 'catalogue_horaire_02.json.xz',
  //         transactions_nomfichier: 'transactions_02.jsonl.xz',
  //         catalogue_hachage: '',
  //         transactions_hachage: 'sha512_b64:xSsI8/pzk+sB4lrKS13PJfM34MOd/Now8/TcGGaCrfnZTCvOLIgRfvp060A8MdvopXN9N1mWC6PeY6vJN4Lr6g==',
  //       }
  //     },
  //     'en-tete': {
  //       domaine: 'domaine.test'
  //     },
  //     '_signature': 'DADADA',
  //     domaine: 'domaine.test',
  //     securite: '1.public',
  //     jour: new Date("2020-01-01").getTime()/1000,
  //   }
  //
  //   expect.assertions(9)
  //   return processFichiersBackup.traiterBackupQuotidien(amqpdao, pathConsignation, catalogue)
  //   .then(resultat=>{
  //     // console.debug("Resultat : %O\nCatalogue: %O", resultat, catalogue)
  //
  //     expect(resultat.archive_hachage).toBeDefined()
  //     expect(resultat.archive_nomfichier).toBe('domaine.test_20200101.tar')
  //     expect(resultat.fichiersInclure[0]).toBe('domaine.test_catalogue_20200101.json.xz')
  //     expect(resultat.fichiersInclure[1]).toBe('horaire/catalogue_horaire_02.json.xz')
  //     expect(resultat.fichiersInclure[2]).toBe('horaire/transactions_02.jsonl.xz')
  //
  //     expect(catalogue.grosfichiers).toBeDefined()
  //     expect(catalogue.grosfichiers['abcd-1234'].nomFichier).toBe('abcd-1234.mgs1')
  //     expect(catalogue.grosfichiers['abcd-1235'].nomFichier).toBe('abcd-1235.mgs1')
  //
  //     const fichierTar = fs.statSync(path.join(tmpdir.name, 'domaine.test_20200101.tar'))
  //     // console.info("Fichier tar: %O", fichierTar)
  //     expect(fichierTar.size).toBeGreaterThan(2048)
  //
  //   })
  // })

  it('genererBackupQuotidien', async () => {
    var appels_transmettreEnveloppeTransaction = []
    const mq = {
      formatterTransaction: (domaine, transaction) => {
        transaction['en-tete'] = {domaine}
        transaction['_signature'] = 'dummy'
        return transaction
      },
      transmettreTransactionFormattee: (transaction)=>{
        appels_transmettreEnveloppeTransaction.push(transaction)
        return ''
      },
      transmettreEnveloppeTransaction: (transaction)=>{
        appels_transmettreEnveloppeTransaction.push(transaction)
        return ''
      }
    }

    const catalogue = {
      domaine: 'domaine.test',
      securite: '1.public',
      jour: new Date('2020-01-01').getTime()/1000,
      fichiers_horaire: {},
    }

    fs.mkdirSync(path.join(tmpdir.name, 'horaire'))
    await processFichiersBackup.sauvegarderLzma(path.join(tmpdir.name, 'horaire', 'catalogue_horaire.json.xz'), {
      transactions_nomfichier: 'transactions_00.jsonl.xz',
      transactions_hachage: 'sha512_b64:xSsI8/pzk+sB4lrKS13PJfM34MOd/Now8/TcGGaCrfnZTCvOLIgRfvp060A8MdvopXN9N1mWC6PeY6vJN4Lr6g==',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      heure: new Date('2020-01-01 12:00').getTime()/1000,
    })
    creerFichierDummy(path.join(tmpdir.name, 'horaire', 'transactions_00.jsonl.xz'), 'dadadon')

    expect.assertions(7)
    return processFichiersBackup.genererBackupQuotidien(mq, pathConsignation, catalogue)
    .then(result=>{
      // console.debug("Resultat : %O", result)
      expect(result).toBeDefined()
      expect(result.archive_hachage).toBeDefined()
      expect(result.archive_nomfichier).toBe('domaine.test_20200101.tar')

      // console.debug("Messages emis : %O", appels_transmettreEnveloppeTransaction)
      expect(appels_transmettreEnveloppeTransaction.length).toBe(2)
      expect(appels_transmettreEnveloppeTransaction[0].fichiers_horaire['12']).toBeDefined()
      expect(appels_transmettreEnveloppeTransaction[1].archive_nomfichier).toBe('domaine.test_20200101.tar')

      // S'assurer que le repertoire horaire/ a ete supprime (tous les fichiers sont archives)
      expect(()=>fs.statSync(path.join(tmpdir.name, 'horaire'))).toThrow()
    })
  })

  it('traiterBackupAnnuel', async () => {
    var appels_transmettreEnveloppeTransaction = []
    const amqpdao = {
      transmettreEnveloppeTransaction: (transaction)=>{
        appels_transmettreEnveloppeTransaction.push(transaction)
        return ''
      },
      formatterTransaction: (domaine, transaction) => {
        transaction['en-tete'] = {domaine, 'uuid-transaction': 'uuid-dummy'}
        transaction['_signature'] = 'dummy'
        return transaction
      }
    }

    // Creer fichier de catalogue quotidien
    const pathCatalogueQuotidien = path.join(tmpdir.name, 'catalogue_quotidien_20200101.json.xz')
    await processFichiersBackup.sauvegarderLzma(pathCatalogueQuotidien, {
      transactions_nomfichier: 'transactions_00.jsonl.xz',
      transactions_hachage: 'sha512_b64:xSsI8/pzk+sB4lrKS13PJfM34MOd/Now8/TcGGaCrfnZTCvOLIgRfvp060A8MdvopXN9N1mWC6PeY6vJN4Lr6g==',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      heure: new Date('2020-01-01 00:00').getTime()/1000,
    })

    // Preparer une archive quotidienne .tar avec le fichier de catalogue
    // Creer nouvelle archive quotidienne
    await tar.c(
      {
        file: path.join(tmpdir.name, 'quotidien_20200101.tar'),
        cwd: tmpdir.name,
      },
      ['catalogue_quotidien_20200101.json.xz']
    )

    const hachageArchive = await calculerHachageFichier(path.join(tmpdir.name, 'quotidien_20200101.tar'),)
    const catalogue = {
      fichiers_quotidien: {
        '20200101': {
          archive_nomfichier: 'quotidien_20200101.tar',
          archive_hachage: hachageArchive,
        }
      },
      domaine: 'domaine.test',
      securite: '1.public',
      annee: new Date("2020-01-01").getTime()/1000,
    }

    // Supprimer archive quotidienne
    fs.unlinkSync(pathCatalogueQuotidien)

    expect.assertions(4)
    return processFichiersBackup.traiterBackupAnnuel(amqpdao, pathConsignation, catalogue)
    .then(resultat=>{
      // console.debug("Resultat traiterBackupAnnuel : %O", resultat)

      expect(fs.statSync(path.join(tmpdir.name, resultat.archive_nomfichier))).toBeDefined()
      expect(resultat.archive_nomfichier).toBe('domaine.test_2020.tar')
      expect(resultat.fichiersInclure[0]).toBe('domaine.test_catalogue_2020.json.xz')
      expect(resultat.fichiersInclure[1]).toBe('quotidien_20200101.tar')
    })
  })

  it('genererBackupAnnuel', async () => {
    var appels_transmettreEnveloppeTransaction = []
    const mq = {
      formatterTransaction: (domaine, transaction) => {
        transaction['en-tete'] = {domaine}
        transaction['_signature'] = 'dummy'
        return transaction
      },
      transmettreTransactionFormattee: (transaction)=>{
        appels_transmettreEnveloppeTransaction.push(transaction)
        return ''
      },
      transmettreEnveloppeTransaction: (transaction)=>{
        appels_transmettreEnveloppeTransaction.push(transaction)
        return ''
      }
    }

    // Creer fichier de catalogue quotidien
    const pathCatalogueQuotidien = path.join(tmpdir.name, 'catalogue_quotidien_20200101.json.xz')
    await processFichiersBackup.sauvegarderLzma(pathCatalogueQuotidien, {
      transactions_nomfichier: 'transactions_00.jsonl.xz',
      transactions_hachage: 'sha512_b64:xSsI8/pzk+sB4lrKS13PJfM34MOd/Now8/TcGGaCrfnZTCvOLIgRfvp060A8MdvopXN9N1mWC6PeY6vJN4Lr6g==',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      heure: new Date('2020-01-01 00:00').getTime()/1000,
    })

    // Preparer une archive quotidienne .tar avec le fichier de catalogue
    // Creer nouvelle archive quotidienne
    await tar.c(
      {
        file: path.join(tmpdir.name, 'quotidien_20200101.tar'),
        cwd: tmpdir.name,
      },
      ['catalogue_quotidien_20200101.json.xz']
    )

    const hachageArchive = await calculerHachageFichier(path.join(tmpdir.name, 'quotidien_20200101.tar'),)
    const catalogue = {
      fichiers_quotidien: {
        '20200101': {
          archive_nomfichier: 'quotidien_20200101.tar',
          archive_hachage: hachageArchive,
        }
      },
      domaine: 'domaine.test',
      securite: '1.public',
      annee: new Date("2020-01-01").getTime()/1000,
    }

    // Supprimer archive quotidienne
    fs.unlinkSync(pathCatalogueQuotidien)

    expect.assertions(5)
    return processFichiersBackup.genererBackupAnnuel(mq, pathConsignation, catalogue)
    .then(resultat=>{
      // console.debug("Resultat genererBackupAnnuel: %O\nTransactions: %O", resultat, appels_transmettreEnveloppeTransaction)

      expect(fs.statSync(path.join(tmpdir.name, resultat.archive_nomfichier))).toBeDefined()
      expect(()=>fs.statSync(path.join(tmpdir.name, 'quotidien_20200101.tar'))).toThrow()

      expect(appels_transmettreEnveloppeTransaction.length).toBe(2)
      expect(appels_transmettreEnveloppeTransaction[0].fichiers_quotidien["20200101"]).toBeDefined()
      expect(appels_transmettreEnveloppeTransaction[1].archive_nomfichier).toBe('domaine.test_2020.tar')
    })
  })

  it('verifierGrosfichiersBackup 1 fichier OK', async () => {
    const infoGrosfichiers = {
      'abcd-1234': {hachage: 'sha512_b64:m9+VOgb9/QzOb/myd745kqAk2XFl308AgjUTBpS4NpTJeELbY39FfPxsZsECbGX/cyIeaVrISi2v4Z7XsMAGQg=='},
    }

    creerFichierDummy(path.join(tmpdir.name, 'abcd-1234.mgs1'), 'dadido')

    expect.assertions(3)
    return processFichiersBackup.verifierGrosfichiersBackup(pathConsignation, infoGrosfichiers)
    .then(resultat=>{
      // console.info("Resultat : %O", resultat[0])
      if(resultat[0].err) console.error("Erreur : %O", resultat[0].err)
      expect(resultat[0].err).toBeUndefined()
      expect(resultat[0].fuuid).toBe('abcd-1234')
      expect(resultat[0].nomFichier).toBe('abcd-1234.mgs1')
    })
  })

  it('verifierGrosfichiersBackup 1 fichier manquant', async () => {
    const infoGrosfichiers = {
      'abcd-1234': {hachage: 'sha512_b64:m9+VOgb9/QzOb/myd745kqAk2XFl308AgjUTBpS4NpTJeELbY39FfPxsZsECbGX/cyIeaVrISi2v4Z7XsMAGQg=='},
    }
    expect.assertions(1)
    return processFichiersBackup.verifierGrosfichiersBackup(pathConsignation, infoGrosfichiers)
    .then(resultat=>{
      // console.info("Resultat : %O", resultat[0])
      expect(resultat[0].err).toBeDefined()
    })
  })

  it('verifierGrosfichiersBackup 1 fichier mauvais hachage', async () => {
    const infoGrosfichiers = {
      'abcd-1234': {hachage: 'sha512_b64:DUMMY'},
    }
    expect.assertions(1)
    return processFichiersBackup.verifierGrosfichiersBackup(pathConsignation, infoGrosfichiers)
    .then(resultat=>{
      // console.info("Resultat : %O", resultat[0])
      expect(resultat[0].err).toBeDefined()
    })
  })

})

async function creerFichierDummy(pathFichier, contenu, opts) {
  opts = opts || {}

  fichiersTmp.push(pathFichier)

  if(opts.lzma) return processFichiersBackup.sauvegarderLzma(pathFichier, contenu)

  fs.writeFileSync(pathFichier, contenu)
}
