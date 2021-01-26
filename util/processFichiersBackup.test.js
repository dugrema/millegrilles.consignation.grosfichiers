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

    const pathConsignation = {
      trouverPathFuuidExistant: function(fuuid) {return {fichier: path.join(tmpdir.name, 'folder_dummy', fuuid + '.mgs1')}}
    }
    const fuuidDict = {
      'abcd-1234-efgh-5678': {'securite': '3.protege', 'hachage': 'hachage_1', 'extension': 'txt', 'heure': '21'}
    }

    fs.mkdirSync(path.join(tmpdir.name, 'folder_dummy'))
    creerFichierDummy(path.join(pathConsignation.trouverPathFuuidExistant('abcd-1234-efgh-5678').fichier), 'dadada')

    expect.assertions(1)
    return processFichiersBackup.traiterGrosfichiers(pathConsignation, tmpdir.name, fuuidDict)
    .then(resultat=>{
      // console.debug("Resultat process fichiers : %O", resultat)
      expect(resultat.fichier.indexOf('/grosfichiers/abcd-1234-efgh-5678.mgs1')).toBeGreaterThan(0)
    })

  })

  it('rotationArchiveApplication', async () => {

  })

  it('sauvegarderFichiersApplication', async () => {

  })

  it('traiterFichiersApplication', async () => {

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
