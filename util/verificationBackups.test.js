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
      jest.setTimeout(300000)  // Donner 5 minutes pour creer les samples
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
  //   expect.assertions(12)
  //   const resultat = await verificationBackups.parcourirBackupsHoraire(pathConsignation(repertoireSample), 'domaine.test', cb)
  //   console.debug("parcourirBackupsHoraire resultat : %O", resultat)
  //   expect(resultat.dateHachageEntetes).toBeNull()
  //   expect(resultat.hachagesTransactions).toBeNull()
  //   expect(resultat.erreursHachage).toBeNull()
  //   expect(resultat.erreursCatalogues).toBeNull()
  // })
  //
  // it('parcourirBackupsHoraire hachage', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample1')
  //
  //   const cb = async function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     expect(catalogue.heure).toBeDefined()
  //   }
  //
  //   expect.assertions(12)
  //   const resultat = await verificationBackups.parcourirBackupsHoraire(
  //     pathConsignation(repertoireSample), 'domaine.test', cb, {verification_hachage: true, verification_enchainement: true}
  //   )
  //   console.debug("parcourirBackupsHoraire hachage resultat : %O", resultat)
  //   expect(Object.keys(resultat.dateHachageEntetes).length).toBe(4)
  //   expect(Object.keys(resultat.hachagesTransactions).length).toBe(4)
  //   expect(resultat.erreursHachage.length).toBe(0)
  //   expect(resultat.erreursCatalogues.length).toBe(0)
  // })
  //
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
  //   const resultat = await verificationBackups.parcourirArchivesBackup(pathConsignation(repertoireSample), 'domaine.test', cb)
  //   console.debug('parcourir 1 backup quotidien resultat : %O', resultat)
  // })
  //
  // it('parcourir 1 backup quotidien verification enchainement', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample2')
  //
  //   const cb = (catalogue, cataloguePath) => {
  //     // Rien a faire
  //   }
  //
  //   expect.assertions(2)
  //   const info = await verificationBackups.parcourirArchivesBackup(pathConsignation(
  //     repertoireSample), 'domaine.test', cb, {verification_enchainement: true})
  //   console.debug("parcourir 1 backup quotidien verification enchainement: resultat %O", info)
  //   expect(Object.keys(info.erreursCatalogues).length).toBe(0)
  //   expect(info.erreursHachage).toBeNull()
  // })
  //
  // it('parcourir 1 backup quotidien verification hachage', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample2')
  //
  //   const cb = (catalogue, cataloguePath) => {
  //     // Rien a faire
  //   }
  //
  //   expect.assertions(2)
  //   const info = await verificationBackups.parcourirArchivesBackup(
  //     pathConsignation(repertoireSample),
  //     'domaine.test',
  //     cb,
  //     {verification_hachage: true}
  //   )
  //   console.debug("parcourir 1 backup quotidien hachage: resultat %O", info)
  //   expect(info.erreursCatalogues).toBeNull()
  //   expect(Object.keys(info.erreursHachage).length).toBe(0)
  // })
  //
  // it('parcourir 3 backups quotidiens verification enchainement', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample3')
  //
  //   const cb = (catalogue, cataloguePath) => {
  //     // Rien a faire
  //   }
  //
  //   expect.assertions(2)
  //   const info = await verificationBackups.parcourirArchivesBackup(pathConsignation(
  //     repertoireSample), 'domaine.test', cb, {verification_enchainement: true})
  //   console.debug("parcourir 3 backups quotidiens verification enchainement: resultat %O", info)
  //   expect(Object.keys(info.erreursCatalogues).length).toBe(0)
  //   expect(info.erreursHachage).toBeNull()
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
  //
  // it('parcourir 1 backup annuel verifications enchainement', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample4')
  //
  //   const cb = function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     expect(catalogue.heure||catalogue.jour||catalogue.annee).toBeDefined()
  //   }
  //
  //   expect.assertions(44)
  //   const resultat = await verificationBackups.parcourirArchivesBackup(
  //     pathConsignation(repertoireSample), 'domaine.test', cb, {verification_enchainement: true})
  //   console.debug("parcourir 1 backup annuel hachage: %O", resultat)
  //
  //   expect(resultat.erreursCatalogues.length).toBe(0)
  //   expect(resultat.erreursHachage).toBeNull()
  // })
  //
  // it('parcourir 1 backup annuel verifications hachage', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE, 'sample4')
  //
  //   const cb = function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     expect(catalogue.heure||catalogue.jour||catalogue.annee).toBeDefined()
  //   }
  //
  //   expect.assertions(46)
  //   const resultat = await verificationBackups.parcourirArchivesBackup(
  //     pathConsignation(repertoireSample), 'domaine.test', cb, {verification_hachage: true})
  //   console.debug("parcourir 1 backup annuel hachage: %O", resultat)
  //   expect(resultat.dateHachageEntetes).toBeNull()
  //   expect(resultat.erreursCatalogues).toBeNull()
  //   expect(resultat.hachagesTransactions).toBeNull()
  //   expect(Object.keys(resultat.erreursHachage).length).toBeNull()
  // })
  //
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
  it('parcourir mix de backups annuel/quotidiens/horaire avec verifications', async() =>{
    const repertoireSample = path.join(BASE_SAMPLE, 'sample5')

    const cb = function(catalogue, cataloguePath) {
      // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
      expect(cataloguePath).toBeDefined()
      expect(catalogue.heure||catalogue.jour||catalogue.annee).toBeDefined()
    }

    expect.assertions(124)
    const resultat = await verificationBackups.parcourirDomaine(
      pathConsignation(repertoireSample), 'domaine.test', cb,
      {verification_hachage: true, verification_enchainement: true}
    )
    console.debug("Resultat parcourir mix de backups annuel/quotidiens/horaire avec verifications: %O", resultat)
  })

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
  //   const infoChainage = await verificationBackups.parcourirArchivesBackup(pathConsignation(repertoireSample), 'domaine.test', cb)
  //   expect(infoChainage.length).toBe(0)
  // })
})
