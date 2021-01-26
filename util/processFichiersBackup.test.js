const processFichiersBackup = require('./processFichiersBackup')
const tmp = require('tmp')
const path = require('path')
const fs = require('fs')

var fichiersTmp = []

describe('processFichiersBackup', ()=>{

  var tmpdir
  beforeEach(()=>{
    // Creer repertoire temporaire
    tmpdir = tmp.dirSync()
    console.info("TMP dir : %s", tmpdir.name)
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

  it('traiterFichiersBackup 1 catalogue 1 fichier transaction', async ()=>{
    // Traiter un fichier de backup horaire avec catalogue et transactions

    // Creer fichier pour catalogue, hook supprimer tmp
    fichiersTmp.push(path.join(tmpdir.name, 'fichier1.txt'))
    creerFichierDummy(path.join(tmpdir.name, 'fichier1.txt.init'), 'Catalogue')
    fichiersTmp.push(path.join(tmpdir.name, 'fichier2.txt'))
    creerFichierDummy(path.join(tmpdir.name, 'fichier2.txt.init'), 'Transaction')

    const fichiersTransactions = [{
        originalname: 'fichier2.txt',
        path: path.join(tmpdir.name, 'fichier2.txt.init'),
      }],
      fichierCatalogue = {
        originalname: 'fichier1.txt',
        path: path.join(tmpdir.name, 'fichier1.txt.init'),
      },
      pathRepertoire = tmpdir.name

    expect.assertions(3)
    return processFichiersBackup.traiterFichiersBackup(fichiersTransactions, fichierCatalogue, pathRepertoire)
    .then(resultat=>{
      const infoCatalogue = fs.statSync(path.join(tmpdir.name, 'fichier1.txt'))
      expect(infoCatalogue).toBeDefined()
      const infoTransaction = fs.statSync(path.join(tmpdir.name, 'fichier2.txt'))
      expect(infoTransaction).toBeDefined()

      console.info("Resultat hachage : %O", resultat)
      expect(resultat['fichier2.txt']).toBe('sha512_b64:1+ROfpt6khAjwaJfsH274cNSZlpekgjU9iUTuyOTCM8htjm53L8eMJP7qy4v6MGx9CbU5O0z9AdBNiFW73YsVQ==')
    })
  })

  it('traiterGrosfichiers grosfichier introuvable', async () => {
    // Creation de backup, hard link grosfichier impossible (fichier introuvable)

    const pathConsignation = {
      trouverPathFuuidExistant: function() {return 'folder_dummy'}
    }
    const fuuidDict = {
      'abcd-1234-efgh-5678': {'securite': '3.protege', 'hachage': 'hachage_1', 'extension': 'txt', 'heure': '21'}
    }

    expect.assertions(1)
    return processFichiersBackup.traiterGrosfichiers(pathConsignation, tmpdir.name, fuuidDict)
    .then(resultat=>{
      // console.debug("Resultat : %O", resultat)
      expect(resultat.err).toBeDefined()
    })

  })

  it('traiterGrosfichiers 1 grosfichier', async () => {
    // Creation de backup, hard link 1 grosfichier vers sous-repertoire de backup horaire

    const pathConsignation = {
      trouverPathFuuidExistant: function(fuuid) {return {fichier: path.join(tmpdir.name, 'folder_dummy', fuuid + '.mgs1')}}
    }
    const fuuidDict = {
      'abcd-1234-efgh-5678': ''
    }

    fs.mkdirSync(path.join(tmpdir.name, 'folder_dummy'))
    creerFichierDummy(path.join(pathConsignation.trouverPathFuuidExistant('abcd-1234-efgh-5678').fichier), 'dadada')

    expect.assertions(1)
    return processFichiersBackup.traiterGrosfichiers(pathConsignation, tmpdir.name, fuuidDict)
    .then(resultat=>{
      // console.debug("Resultat process fichiers : %O", resultat)
      expect(resultat.fichiers[0].indexOf('/grosfichiers/abcd-1234-efgh-5678.mgs1')).toBeGreaterThan(0)
    })

  })

  it('traiterGrosfichiers 2 grosfichiers', async () => {
    // Creation de backup, hard link 2 grosfichier vers sous-repertoire de backup horaire

    const pathConsignation = {
      trouverPathFuuidExistant: function(fuuid) {return {fichier: path.join(tmpdir.name, 'folder_dummy', fuuid + '.mgs1')}}
    }
    const fuuidDict = {
      'abcd-1234-efgh-5678': '',
      'abcd-1234-efgh-5679': ''
    }

    fs.mkdirSync(path.join(tmpdir.name, 'folder_dummy'))
    creerFichierDummy(path.join(pathConsignation.trouverPathFuuidExistant('abcd-1234-efgh-5678').fichier), 'dadada')
    creerFichierDummy(path.join(pathConsignation.trouverPathFuuidExistant('abcd-1234-efgh-5679').fichier), 'dadada')

    expect.assertions(2)
    return processFichiersBackup.traiterGrosfichiers(pathConsignation, tmpdir.name, fuuidDict)
    .then(resultat=>{
      // console.debug("Resultat process fichiers : %O", resultat)
      expect(resultat.fichiers[0].indexOf('/grosfichiers/abcd-1234-efgh-5678.mgs1')).toBeGreaterThan(0)
      expect(resultat.fichiers[1].indexOf('/grosfichiers/abcd-1234-efgh-5679.mgs1')).toBeGreaterThan(0)
    })

  })

  it('sauvegarderFichiersApplication', async () => {
    // Tester la sauvegarde du catalogue (dict) et transfere le fichier
    // d'archive vers le repertoire de backup d'application

    const pathApplication = path.join(tmpdir.name, 'application.txt'),
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
          },
          transactionCatalogue = {
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

  it('genererBackupQuotidien2', async () => {

  })

  it('genererBackupQuotidien', async () => {

  })

  it('genererBackupAnnuel2', async () => {

  })

  it('genererBackupAnnuel', async () => {

  })

})

function creerFichierDummy(pathFichier, contenu) {
  fichiersTmp.push(pathFichier)
  fs.writeFileSync(pathFichier, contenu)
}
