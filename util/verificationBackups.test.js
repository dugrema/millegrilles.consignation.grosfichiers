const fs = require('fs')
const path = require('path')
const verificationBackups = require('./verificationBackups')

const BASE_SAMPLE = '/tmp/mg-verificationbackups'

function pathConsignation(repertoire) {
  return {
    trouverPathBackupDomaine: domaine=>{return repertoire},
    trouverPathBackupHoraire: domaine=>{return path.join(repertoire, 'horaire')},
    consignationPathBackup: repertoire,
    trouverPathFuuidExistant: fuuid=>{return path.join(repertoire, fuuid + '.mgs1')}
  }
}

describe('VerificationBackups integration logique', ()=>{

  // Creation des samples (uniquement invoque si repertoire n'existe pas)
  it('generer archives horaire test', async ()=>{
    try {
      fs.statSync(BASE_SAMPLE)
    } catch(err) {
      console.info("Creation des archives de test de VerificationBackups")
      const sampleCreation = require('./sampleCreation')
      await sampleCreation()
    }
  })

  // it('parcourirBackupsHoraire', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample1')
  //
  //   const cb = async function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     expect(catalogue.heure).toBeDefined()
  //   }
  //
  //   expect.assertions(10)
  //   const resultat = await verificationBackups.parcourirBackupsHoraire(pathConsignation(repertoireSample), 'domaine.test', cb)
  //   console.debug("parcourirBackupsHoraire resultat : %O", resultat)
  //   expect(Object.keys(resultat.dateHachageEntetes).length).toBe(4)
  //   expect(resultat.hachagesTransactions.length).toBe(0)
  // })

  it('parcourirBackupsHoraire hachage', async() =>{
    const repertoireSample = path.join(BASE_SAMPLE, 'sample1')

    const cb = async function(catalogue, cataloguePath) {
      // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
      expect(cataloguePath).toBeDefined()
      expect(catalogue.heure).toBeDefined()
    }

    const resultat = await verificationBackups.parcourirBackupsHoraire(
      pathConsignation(repertoireSample), 'domaine.test', cb, {hachage: true}
    )
    console.debug("parcourirBackupsHoraire hachage resultat : %O", resultat)
  })

  // it('parcourir 1 backup quotidien', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample2')
  //
  //   const cb = function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     expect(catalogue.heure||catalogue.jour).toBeDefined()
  //   }
  //
  //   expect.assertions(8)
  //   await verificationBackups.parcourirArchivesBackup(pathConsignation(repertoireSample), 'domaine.test', cb)
  // })
  //
  // it('parcourir 1 backup annuel', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample4')
  //
  //   const cb = function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     expect(catalogue.heure||catalogue.jour||catalogue.annee).toBeDefined()
  //   }
  //
  //   expect.assertions(42)
  //   await verificationBackups.parcourirArchivesBackup(pathConsignation(repertoireSample), 'domaine.test', cb)
  // })

  // it('parcourir mix de backups annuel/quotidiens/horaire', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample5')
  //
  //   const cb = function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     expect(catalogue.heure||catalogue.jour||catalogue.annee).toBeDefined()
  //   }
  //
  //   expect.assertions(124)
  //   await verificationBackups.parcourirDomaine(pathConsignation(repertoireSample), 'domaine.test', cb)
  // })
  //
  // it('output catalogues horaires vers stream', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample6')
  //
  //   const outputstream = fs.createWriteStream('/tmp/outcats.txt')
  //
  //   const cb = function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     if(catalogue.heure) {
  //       outputstream.write(JSON.stringify(catalogue))
  //       outputstream.write('\n')
  //     }
  //   }
  //
  //   expect.assertions(124)
  //   await verificationBackups.parcourirDomaine(pathConsignation(repertoireSample), 'domaine.test', cb)
  // })

})

describe("Verification backups load test", ()=>{
  // it('parcourir 1 backup annuel complet (365 jours avec 24 heures chaque)', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample6')
  //
  //   const cb = function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     expect(catalogue.heure||catalogue.jour||catalogue.annee).toBeDefined()
  //   }
  //
  //   expect.assertions(18252)
  //   jest.setTimeout(120000)
  //   await verificationBackups.parcourirArchivesBackup(pathConsignation(repertoireSample), 'domaine.test', cb)
  // })
})
