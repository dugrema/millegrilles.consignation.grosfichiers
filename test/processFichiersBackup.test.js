const processFichiersBackup = require('../util/processFichiersBackup')
const tmp = require('tmp')
const path = require('path')
const fs = require('fs')
const tar = require('tar')
const { calculerHachageFichier } = require('../util/utilitairesHachage')

var fichiersTmp = []

// Mocks
var tmpdir
const pathConsignation = {
  trouverPathBackupDomaine: domaine=>{return tmpdir.name},
  trouverPathBackupHoraire: domaine=>{return path.join(tmpdir.name, 'horaire')},
  trouverPathBackupSnapshot: domaine=>{return path.join(tmpdir.name, 'snapshot')},
  // consignationPathBackup: tmpdir.name,
  trouverPathFuuidExistant: fuuid=>{return path.join(tmpdir.name, fuuid + '.mgs2')}
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
    fs.rmdir(tmpdir.name, {recursive: true}, err=>{
      if(err) console.error("Erreur suppression %s", tmpdir.name)
    })
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
      transactions_hachage: 'z8Vx6tEfdbz6udDHH6RaCTjvFhATgP4CPSvnbmaffPY4ttehdNUYFX278XAZA9nXqq8HGPL46oXjyfaoE3hJ252pH2t',
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

    var resultat = await processFichiersBackup.traiterFichiersBackup(amqpdao, pathConsignation, fichiersTransactions, fichierCatalogue)
    console.info("Resultat hachage : %O", resultat)

    const infoCatalogue = fs.statSync(path.join(tmpdir.name, 'horaire/fichier1.txt'))
    expect(infoCatalogue).toBeDefined()
    const infoTransaction = fs.statSync(path.join(tmpdir.name, 'horaire/fichier2.txt'))
    expect(infoTransaction).toBeDefined()

    expect(resultat['ok']).toBe(true)

  })

  it('traiterFichiersBackup backup regulier verifier repertoire snapshot supprime', async ()=>{
    // Traiter un fichier de backup horaire avec catalogue et transactions
    const amqpdao = {
      pki: {
        verifierSignatureMessage: message => {return true}
      }
    }

    fs.mkdirSync(path.join(tmpdir.name, 'snapshot'))
    creerFichierDummy(path.join(tmpdir.name, 'snapshot/fichier.txt'), 'DUMMY')

    // Creer fichier pour catalogue, hook supprimer tmp
    await creerFichierDummy(path.join(tmpdir.name, 'fichier1.txt.init'), {
      domaine: 'domaine.test',
      transactions_hachage: 'z8Vx6tEfdbz6udDHH6RaCTjvFhATgP4CPSvnbmaffPY4ttehdNUYFX278XAZA9nXqq8HGPL46oXjyfaoE3hJ252pH2t',
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

    var absent = false
    try {
      const info = fs.statSync(path.join(tmpdir.name, 'snapshot'))
      console.debug("INFO recu, pas suppose : %O", info)
    } catch(err) {
      console.debug("Erreur sync : %O", err)
      absent = true
    }
    expect(absent).toBe(true)

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
      transactions_hachage: 'z8Vx6tEfdbz6udDHH6RaCTjvFhATgP4CPSvnbmaffPY4ttehdNUYFX278XAZA9nXqq8HGPL46oXjyfaoE3hJ252pH2t',
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
    const infoTransaction = fs.statSync(path.join(tmpdir.name, 'snapshot/transactions.jsonl.xz.mgs2'))
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
      transactions_hachage: 'z8Vx6tEfdbz6udDHH6RaCTjvFhATgP4CPSvnbmaffPY4ttehdNUYFX278XAZA9nXqq8HGPL46oXjyfaoE3hJ252pH2t',
      snapshot: true,
      valeur: 1,
    }, {lzma: true})
    creerFichierDummy(path.join(tmpdir.name, 'fichier2.txt.init'), 'Transaction')

    // Inserer fichiers dummy pour verifier override
    fs.mkdirSync(path.join(tmpdir.name, 'snapshot/'))
    creerFichierDummy(path.join(tmpdir.name, 'snapshot/catalogue.json.xz'), 'catalogue dummy')
    creerFichierDummy(path.join(tmpdir.name, 'snapshot/transactions.jsonl.xz.mgs2'), 'transactions dummy')

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
    expect(fs.statSync(path.join(tmpdir.name, 'snapshot/transactions.jsonl.xz.mgs2'))).toBeDefined()
    expect(resultat1['ok']).toBe(true)

    const catalogue2 = await processFichiersBackup.chargerLzma(path.join(tmpdir.name, 'snapshot/catalogue.json.xz'))
    expect(catalogue2.valeur).toBe(1)
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
    return processFichiersBackup.sauvegarderFichiersApplication(catalogue, pathApplication, pathBackupApplication)
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
      },
      transmettreEnveloppeCommande: (transaction)=>{
        appels_transmettreEnveloppeTransaction.push(transaction)
        return ''
      }
    }

    const transactionCatalogue = {
            archive_hachage: 'z8VvuHZrjzp2T1Uy86Ktf3wqRsm79QoWqfQXbVQqazT4UoMji9uhQnyTDFRSfS7KBPcWmTxc2PjrHQWTkXU7KekgxEm',
            archive_nomfichier: 'application.txt',
            catalogue_nomfichier: 'catalogue.json',
            'en-tete': {estampille: 1}
          },
          transactionMaitreDesCles = {},
          fichierApplication = path.join(tmpdir.name, 'application.txt'),
          fichierCatalogue = path.join(tmpdir.name, 'catalogue.json')
          fichierMaitrecles = path.join(tmpdir.name, 'maitrecles.json.autre')

    creerFichierDummy(fichierCatalogue, JSON.stringify(transactionCatalogue))
    creerFichierDummy(fichierMaitrecles, JSON.stringify(transactionMaitreDesCles))
    creerFichierDummy(fichierApplication, 'dadido')

    // expect.assertions(1)

    await processFichiersBackup.traiterFichiersApplication(
      amqpdao, fichierCatalogue, fichierMaitrecles, fichierApplication, tmpdir.name)

    expect(appels_transmettreEnveloppeTransaction.length).toBe(2)
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

  it('traiterBackupQuotidien 2 horaires meme jour', async () => {
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
      transactions_hachage: 'z8VwjAw4P6LaxnDJJpWSLi3fsKFoz9Zsv6hGprDxMTmLsTckN7UgJsYwa6uFsgbfnaC9YArKZg2mzLEtR3ANhJZLSQR',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      heure: new Date('2020-01-01 00:00').getTime()/1000,
    })
    creerFichierDummy(path.join(tmpdir.name, 'horaire', 'transactions_00.jsonl.xz'), 'dadadon')

    await processFichiersBackup.sauvegarderLzma(path.join(tmpdir.name, 'horaire', 'catalogue_horaire_01.json.xz'), {
      transactions_nomfichier: 'transactions_01.jsonl.xz',
      transactions_hachage: 'z8VwMrJNg3SpCg3tmtAjzhb84RkWngHJXnF1CnfY2XGUFbStuuKdXo7fEuxpYjCcd2fAqTiv2H7MRAEECBoyX8fQKA5',
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
          transactions_hachage: 'z8VwjAw4P6LaxnDJJpWSLi3fsKFoz9Zsv6hGprDxMTmLsTckN7UgJsYwa6uFsgbfnaC9YArKZg2mzLEtR3ANhJZLSQR',
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

    const resultat = await processFichiersBackup.traiterBackupQuotidien(amqpdao, pathConsignation, catalogue)
    // console.debug("Resultat : %O", resultat)

    expect(resultat.archive_hachage).toBeDefined()
    expect(resultat.grosfichiers).toBeUndefined()
    expect(resultat.archive_nomfichier).toBe('domaine.test_20200101.tar')
    expect(resultat.fichiersInclure.length).toBe(5)
    expect(resultat.fichiersInclure[0]).toBe('domaine.test_catalogue_20200101.json.xz')
    expect(resultat.fichiersInclure[1]).toBe('horaire/catalogue_horaire_00.json.xz')
    expect(resultat.fichiersInclure[2]).toBe('horaire/transactions_00.jsonl.xz')
    expect(resultat.fichiersInclure[3]).toBe('horaire/catalogue_horaire_01.json.xz')
    expect(resultat.fichiersInclure[4]).toBe('horaire/transactions_01.jsonl.xz')

    const fichierTar = fs.statSync(path.join(tmpdir.name, 'domaine.test_20200101.tar'))
    // console.info("Fichier tar: %O", fichierTar)
    expect(fichierTar.size).toBeGreaterThan(2048)
  })

  it('traiterBackupQuotidien 2 horaires dans 2 mois differents', async () => {
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
      transactions_hachage: 'z8VwjAw4P6LaxnDJJpWSLi3fsKFoz9Zsv6hGprDxMTmLsTckN7UgJsYwa6uFsgbfnaC9YArKZg2mzLEtR3ANhJZLSQR',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      heure: new Date('2020-01-31 23:00').getTime()/1000,
    })
    creerFichierDummy(path.join(tmpdir.name, 'horaire', 'transactions_00.jsonl.xz'), 'dadadon')

    await processFichiersBackup.sauvegarderLzma(path.join(tmpdir.name, 'horaire', 'catalogue_horaire_01.json.xz'), {
      transactions_nomfichier: 'transactions_01.jsonl.xz',
      transactions_hachage: 'z8VwMrJNg3SpCg3tmtAjzhb84RkWngHJXnF1CnfY2XGUFbStuuKdXo7fEuxpYjCcd2fAqTiv2H7MRAEECBoyX8fQKA5',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      heure: new Date('2020-02-01 00:00').getTime()/1000,
    })
    creerFichierDummy(path.join(tmpdir.name, 'horaire', 'transactions_01.jsonl.xz'), 'dadada')

    const catalogue = {
      '_signature': 'DADADA',
      domaine: 'domaine.test',
      securite: '1.public',
      jour: new Date("2020-01-31").getTime()/1000,
    }

    const resultat = await processFichiersBackup.traiterBackupQuotidien(amqpdao, pathConsignation, catalogue)
    console.debug("Resultat : %O", resultat)

    expect(resultat.fichiersInclure.length).toBe(3)

    expect(resultat.fichiersInclure[0]).toBe('domaine.test_catalogue_20200131.json.xz')
    expect(resultat.fichiersInclure[1]).toBe('horaire/catalogue_horaire_00.json.xz')
    expect(resultat.fichiersInclure[2]).toBe('horaire/transactions_00.jsonl.xz')
  })

  it('traiterBackupQuotidien regenerer signature', async () => {
    // Test de changement du catalogue et regeneration de la signature

    var appels_transmettreEnveloppeTransaction = []
    var params_opts = null
    const amqpdao = {
      transmettreEnveloppeTransaction: (transaction)=>{
        appels_transmettreEnveloppeTransaction.push(transaction)
        return ''
      },
      formatterTransaction: (domaine, transaction, opts) => {
        transaction['en-tete'] = {domaine}
        transaction['_signature'] = 'dummy'
        params_opts = opts
        return transaction
      }
    }

    fs.mkdirSync(path.join(tmpdir.name, 'horaire'))
    await processFichiersBackup.sauvegarderLzma(path.join(tmpdir.name, 'horaire', 'catalogue_horaire_00.json.xz'), {
      transactions_nomfichier: 'transactions_00.jsonl.xz',
      transactions_hachage: 'z8VwjAw4P6LaxnDJJpWSLi3fsKFoz9Zsv6hGprDxMTmLsTckN7UgJsYwa6uFsgbfnaC9YArKZg2mzLEtR3ANhJZLSQR',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      heure: new Date('2020-01-01 00:00').getTime()/1000,
    })
    creerFichierDummy(path.join(tmpdir.name, 'horaire', 'transactions_00.jsonl.xz'), 'dadadon')

    const catalogue = {
      fichiers_horaire: {},
      'en-tete': {
        domaine: 'domaine.test',
        fingerprint_certificat: 'DUMMY A REMPLACER',
      },
      '_signature': 'DUMMY A REMPLACER',
      '_certificat': 'DUMMY A REMPLACER',
      domaine: 'domaine.test',
      securite: '1.public',
      jour: new Date("2020-01-01").getTime()/1000,
    }

    const resultat = await processFichiersBackup.traiterBackupQuotidien(amqpdao, pathConsignation, catalogue)
    console.debug("Resultat : %O", resultat)
    const catalogueResultat = resultat.catalogue

    expect(params_opts.attacherCertificat).toBe(true)

    // Note : amqpdao est un mock qui met _signature a dummy. On fait juste
    // tester que la logique a retire en-tete et _certificat
    expect(catalogueResultat['_signature']).toBe('dummy')
    expect(catalogueResultat['en-tete'].fingerprint_certificat).toBeUndefined()
    expect(catalogueResultat['_certificat']).toBeUndefined()
  })

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
      },
      emettreEvenement: (message, domaine)=>{
        appels_transmettreEnveloppeTransaction.push({message, domaine})
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
      transactions_hachage: 'z8VwjAw4P6LaxnDJJpWSLi3fsKFoz9Zsv6hGprDxMTmLsTckN7UgJsYwa6uFsgbfnaC9YArKZg2mzLEtR3ANhJZLSQR',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      heure: new Date('2020-01-01 12:00').getTime()/1000,
    })
    creerFichierDummy(path.join(tmpdir.name, 'horaire', 'transactions_00.jsonl.xz'), 'dadadon')

    const result = await processFichiersBackup.genererBackupQuotidien(mq, pathConsignation, catalogue)
    console.debug("Resultat : %O\nMessages:\n%O", result, appels_transmettreEnveloppeTransaction)
    expect(result).toBeDefined()
    expect(result.archive_hachage).toBeDefined()
    expect(result.archive_nomfichier).toBe('domaine.test_20200101.tar')

    // console.debug("Messages emis : %O", appels_transmettreEnveloppeTransaction)
    expect(appels_transmettreEnveloppeTransaction.length).toBe(4)
    expect(appels_transmettreEnveloppeTransaction[0].domaine).toBe('evenement.Backup.backupMaj')
    expect(appels_transmettreEnveloppeTransaction[0].message.evenement).toBe('backupQuotidienDebut')
    expect(appels_transmettreEnveloppeTransaction[3].domaine).toBe('evenement.Backup.backupMaj')
    expect(appels_transmettreEnveloppeTransaction[3].message.evenement).toBe('backupQuotidienTermine')

    expect(appels_transmettreEnveloppeTransaction[1].fichiers_horaire['12']).toBeDefined()
    expect(appels_transmettreEnveloppeTransaction[2].archive_nomfichier).toBe('domaine.test_20200101.tar')

    // S'assurer que le repertoire horaire/ a ete supprime (tous les fichiers sont archives)
    expect(()=>fs.statSync(path.join(tmpdir.name, 'horaire'))).toThrow()
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
      transactions_hachage: 'z8VwjAw4P6LaxnDJJpWSLi3fsKFoz9Zsv6hGprDxMTmLsTckN7UgJsYwa6uFsgbfnaC9YArKZg2mzLEtR3ANhJZLSQR',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      jour: new Date('2020-01-01 00:00').getTime()/1000,
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
      console.debug("Resultat traiterBackupAnnuel : %O", resultat)

      expect(fs.statSync(path.join(tmpdir.name, resultat.archive_nomfichier))).toBeDefined()
      expect(resultat.archive_nomfichier).toBe('domaine.test_2020.tar')
      expect(resultat.fichiersInclure[0]).toBe('domaine.test_catalogue_2020.json.xz')
      expect(resultat.fichiersInclure[1]).toBe('quotidien_20200101.tar')
    })
  })

  it('traiterBackupAnnuel regenerer signature', async () => {
    var appels_transmettreEnveloppeTransaction = []
    var amqpdao_opts = null
    const amqpdao = {
      transmettreEnveloppeTransaction: (transaction)=>{
        appels_transmettreEnveloppeTransaction.push(transaction)
        return ''
      },
      formatterTransaction: (domaine, transaction, opts) => {
        transaction['en-tete'] = {domaine, 'uuid-transaction': 'uuid-dummy'}
        transaction['_signature'] = 'dummy'
        amqpdao_opts = opts
        return transaction
      }
    }

    // Creer fichier de catalogue quotidien
    const pathCatalogueQuotidien = path.join(tmpdir.name, 'catalogue_quotidien_20200101.json.xz')
    await processFichiersBackup.sauvegarderLzma(pathCatalogueQuotidien, {
      transactions_nomfichier: 'transactions_00.jsonl.xz',
      transactions_hachage: 'z8VwjAw4P6LaxnDJJpWSLi3fsKFoz9Zsv6hGprDxMTmLsTckN7UgJsYwa6uFsgbfnaC9YArKZg2mzLEtR3ANhJZLSQR',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      jour: new Date('2020-01-01 00:00').getTime()/1000,
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
      fichiers_quotidien: {},
      domaine: 'domaine.test',
      securite: '1.public',
      annee: new Date("2020-01-01").getTime()/1000,
    }

    // Supprimer archive quotidienne
    fs.unlinkSync(pathCatalogueQuotidien)

    const resultat = await processFichiersBackup.traiterBackupAnnuel(amqpdao, pathConsignation, catalogue)
    console.debug("Resultat traiterBackupAnnuel : %O", resultat)
    const catalogueResultat = resultat.catalogue

    expect(amqpdao_opts.attacherCertificat).toBe(true)

    expect(catalogueResultat['_signature']).toBe('dummy')
    expect(catalogueResultat['_certificat']).toBeUndefined()
    expect(catalogueResultat['en-tete'].fingerprint_certificat).toBeUndefined()

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
      },
      emettreEvenement: (message, domaine)=>{
        appels_transmettreEnveloppeTransaction.push({message, domaine})
        return ''
      }
    }

    // Creer fichier de catalogue quotidien
    const pathCatalogueQuotidien = path.join(tmpdir.name, 'catalogue_quotidien_20200101.json.xz')
    await processFichiersBackup.sauvegarderLzma(pathCatalogueQuotidien, {
      transactions_nomfichier: 'transactions_00.jsonl.xz',
      transactions_hachage: 'z8VwjAw4P6LaxnDJJpWSLi3fsKFoz9Zsv6hGprDxMTmLsTckN7UgJsYwa6uFsgbfnaC9YArKZg2mzLEtR3ANhJZLSQR',
      'en-tete': {
        hachage_contenu: 'dummy',
      },
      jour: new Date('2020-01-01 00:00').getTime()/1000,
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

    const resultat = await processFichiersBackup.genererBackupAnnuel(mq, pathConsignation, catalogue)
    // console.debug("Resultat genererBackupAnnuel: %O\nTransactions:\n%O", resultat, appels_transmettreEnveloppeTransaction)

    expect(fs.statSync(path.join(tmpdir.name, resultat.archive_nomfichier))).toBeDefined()
    expect(()=>fs.statSync(path.join(tmpdir.name, 'quotidien_20200101.tar'))).toThrow()

    expect(appels_transmettreEnveloppeTransaction.length).toBe(4)
    expect(appels_transmettreEnveloppeTransaction[0].domaine).toBe('evenement.Backup.backupMaj')
    expect(appels_transmettreEnveloppeTransaction[0].message.evenement).toBe('backupAnnuelDebut')
    expect(appels_transmettreEnveloppeTransaction[3].domaine).toBe('evenement.Backup.backupMaj')
    expect(appels_transmettreEnveloppeTransaction[3].message.evenement).toBe('backupAnnuelTermine')

    expect(appels_transmettreEnveloppeTransaction[1].fichiers_quotidien["20200101"]).toBeDefined()
    expect(appels_transmettreEnveloppeTransaction[2].archive_nomfichier).toBe('domaine.test_2020.tar')

  })

  // it('verifierGrosfichiersBackup 1 fichier OK', async () => {
  //   const infoGrosfichiers = {
  //     'abcd-1234': {hachage: 'sha512_b64:m9+VOgb9/QzOb/myd745kqAk2XFl308AgjUTBpS4NpTJeELbY39FfPxsZsECbGX/cyIeaVrISi2v4Z7XsMAGQg=='},
  //   }
  //
  //   creerFichierDummy(path.join(tmpdir.name, 'abcd-1234.mgs1'), 'dadido')
  //
  //   expect.assertions(3)
  //   return processFichiersBackup.verifierGrosfichiersBackup(pathConsignation, infoGrosfichiers)
  //   .then(resultat=>{
  //     // console.info("Resultat : %O", resultat[0])
  //     if(resultat[0].err) console.error("Erreur : %O", resultat[0].err)
  //     expect(resultat[0].err).toBeUndefined()
  //     expect(resultat[0].fuuid).toBe('abcd-1234')
  //     expect(resultat[0].nomFichier).toBe('abcd-1234.mgs1')
  //   })
  // })
  //
  // it('verifierGrosfichiersBackup 1 fichier manquant', async () => {
  //   const infoGrosfichiers = {
  //     'abcd-1234': {hachage: 'sha512_b64:m9+VOgb9/QzOb/myd745kqAk2XFl308AgjUTBpS4NpTJeELbY39FfPxsZsECbGX/cyIeaVrISi2v4Z7XsMAGQg=='},
  //   }
  //   expect.assertions(1)
  //   return processFichiersBackup.verifierGrosfichiersBackup(pathConsignation, infoGrosfichiers)
  //   .then(resultat=>{
  //     // console.info("Resultat : %O", resultat[0])
  //     expect(resultat[0].err).toBeDefined()
  //   })
  // })
  //
  // it('verifierGrosfichiersBackup 1 fichier mauvais hachage', async () => {
  //   const infoGrosfichiers = {
  //     'abcd-1234': {hachage: 'sha512_b64:DUMMY'},
  //   }
  //   expect.assertions(1)
  //   return processFichiersBackup.verifierGrosfichiersBackup(pathConsignation, infoGrosfichiers)
  //   .then(resultat=>{
  //     // console.info("Resultat : %O", resultat[0])
  //     expect(resultat[0].err).toBeDefined()
  //   })
  // })

})

describe('processFichiersBackup', ()=>{

  const sampleCreation = require('./sampleCreation')

  beforeEach(()=>{
    // Creer repertoire temporaire
    tmpdir = tmp.dirSync()
    pathConsignation.consignationPathBackup = tmpdir.name
    // console.info("TMP dir : %s", tmpdir.name)
  })

  afterEach(()=>{
    fs.rmdir(tmpdir.name, {recursive: true}, err=>{
      if(err) console.error("Erreur suppression %s", tmpdir.name)
    })
  })

  it('backup horaire -> quotidien', async ()=>{
    await sampleCreation.creerSamplesHoraire({rep: tmpdir.name})

    var appels_transmettreEnveloppeTransaction = []
    const mq = {
      formatterTransaction: (domaine, transaction) => {
        transaction['en-tete'] = {domaine}
        transaction['_signature'] = 'dummy'
        return transaction
      },
    }

    const catalogue = {
      jour: new Date("2020-01-01").getTime()/1000,
      domaine: 'domaine.test',
      securite: '1.public',
      fichiers_horaire: {},
      // 'en-tete': {},
      // archive_nomfichier: 'domaine.test_20200101.tar',
    }

    const resultat = await processFichiersBackup.traiterBackupQuotidien(mq, pathConsignation, catalogue)
    console.debug("backup horaire -> quotidien : %O", resultat)
    console.debug("Messages : %O", appels_transmettreEnveloppeTransaction)
    console.debug("Fichiers horaires : %O", resultat.catalogue.fichiers_horaire)
  })

  it('', async ()=>{
    await sampleCreation.creerBackupQuotidien(new Date("2020-02-01"), {rep: path.join(tmpdir.name, 'quotidien')})

    const resultat = await processFichiersBackup.trouverArchivesQuotidiennes(tmpdir.name)
    console.debug("backup quotidien -> annuel : %O", resultat)
    expect(resultat['20200201']).toBeDefined()
  })

  it('backup quotidien -> annuel', async ()=>{
    await sampleCreation.creerBackupQuotidien(new Date("2020-02-01"), {rep: path.join(tmpdir.name)})

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
      },
      emettreEvenement: (message, domaine)=>{
        appels_transmettreEnveloppeTransaction.push({message, domaine})
        return ''
      }
    }

    const catalogue = {
      annee: new Date("2020-01-01").getTime()/1000,
      domaine: 'domaine.test',
      securite: '1.public',
      fichiers_quotidien: {},
    }

    const resultat = await processFichiersBackup.genererBackupAnnuel(mq, pathConsignation, catalogue)
    // console.debug("backup quotidien -> annuel resultat : %O", resultat)
    // console.debug("Messages : %O", appels_transmettreEnveloppeTransaction)

    const catalogueMessage = appels_transmettreEnveloppeTransaction[1]
    // console.debug("Fichiers : %O", catalogueMessage.fichiers_quotidien)

    expect(appels_transmettreEnveloppeTransaction.length).toBe(4)
    expect(catalogueMessage.fichiers_quotidien['20200201']).toBeDefined()
  })

})

describe('operations copie de fichiers', ()=>{

  beforeEach(()=>{
    // Creer repertoire temporaire
    tmpdir = tmp.dirSync()
    pathConsignation.consignationPathBackup = tmpdir.name
  })

  afterEach(()=>{
    // Nettoyer repertoire temporaire
    // fs.rmdir(tmpdir.name, {recursive: true}, err=>{
    //   if(err) console.error("Erreur suppression %s", tmpdir.name)
    // })
  })

  it('rsyncBackupVersCopie nouvelle', async ()=>{
    // Creer du contenu de repertoire source
    const pathSource = path.join(tmpdir.name, 'domaine.test')
    const pathDestination = path.join(tmpdir.name, 'destination')
    fs.mkdirSync(path.join(pathSource, 'horaire'), {recursive: true})
    fs.mkdirSync(pathDestination)

    await creerFichierDummy(path.join(pathSource, 'fichier1.txt'), 'fichier 1')
    await creerFichierDummy(path.join(pathSource, 'fichier2.txt'), 'fichier 2')
    await creerFichierDummy(path.join(pathSource, 'fichier3.txt'), 'fichier 3')
    await creerFichierDummy(path.join(pathSource, 'horaire/fichier1.txt'), 'fichier 4')

    // Override path pour test
    pathConsignation.trouverPathBackupDomaine = () => {return pathSource}

    const resultat = await processFichiersBackup.rsyncBackupVersCopie(pathConsignation, 'domaine.test', pathDestination)
    // console.debug('rsyncBackupVersCopie nouvelle resultat %O', resultat)

    // Verifier que les fichiers ont ete copies
    expect(fs.statSync(path.join(pathDestination, 'domaine.test/fichier1.txt'))).toBeDefined()
    expect(fs.statSync(path.join(pathDestination, 'domaine.test/fichier2.txt'))).toBeDefined()
    expect(fs.statSync(path.join(pathDestination, 'domaine.test/fichier3.txt'))).toBeDefined()
    expect(fs.statSync(path.join(pathDestination, 'domaine.test/horaire/fichier1.txt'))).toBeDefined()
  })

  it('rsyncBackupVersCopie cpoie manquant', async ()=>{
    // Creer du contenu de repertoire source
    const pathSource = path.join(tmpdir.name, 'domaine.test')
    const pathDestination = path.join(tmpdir.name, 'destination')
    fs.mkdirSync(path.join(pathSource, 'horaire'), {recursive: true})
    fs.mkdirSync(pathDestination)

    await creerFichierDummy(path.join(pathSource, 'fichier1.txt'), 'fichier 1')
    await creerFichierDummy(path.join(pathSource, 'fichier2.txt'), 'fichier 2')
    await creerFichierDummy(path.join(pathDestination, 'fichier1.txt'), 'fichier 1')

    // Override path pour test
    pathConsignation.trouverPathBackupDomaine = () => {return pathSource}

    const resultat = await processFichiersBackup.rsyncBackupVersCopie(pathConsignation, 'domaine.test', pathDestination)
    // console.debug('rsyncBackupVersCopie nouvelle resultat %O', resultat)

    // Verifier que les fichiers ont ete copies
    expect(fs.statSync(path.join(pathDestination, 'domaine.test/fichier2.txt'))).toBeDefined()
  })

  it('rsyncBackupVersCopie supprime fichier', async ()=>{
    // Creer du contenu de repertoire source
    const pathSource = path.join(tmpdir.name, 'domaine.test')
    const pathDestination = path.join(tmpdir.name, 'destination')
    fs.mkdirSync(path.join(pathSource, 'horaire'), {recursive: true})
    fs.mkdirSync(pathDestination)

    await creerFichierDummy(path.join(pathSource, 'fichier2.txt'), 'fichier 2')
    await creerFichierDummy(path.join(pathDestination, 'fichier1.txt'), 'fichier 1')
    await creerFichierDummy(path.join(pathDestination, 'fichier2.txt'), 'fichier 2')

    // Override path pour test
    pathConsignation.trouverPathBackupDomaine = () => {return pathSource}

    const resultat = await processFichiersBackup.rsyncBackupVersCopie(pathConsignation, 'domaine.test', pathDestination)
    // console.debug('rsyncBackupVersCopie nouvelle resultat %O', resultat)

    // Verifier que les fichiers ont ete copies
    expect(()=>fs.statSync(path.join(pathDestination, 'domaine.test/fichier1.txt'))).toThrow()
  })

})

async function creerFichierDummy(pathFichier, contenu, opts) {
  opts = opts || {}

  fichiersTmp.push(pathFichier)

  if(opts.lzma) return processFichiersBackup.sauvegarderLzma(pathFichier, contenu)

  fs.writeFileSync(pathFichier, contenu)
}
