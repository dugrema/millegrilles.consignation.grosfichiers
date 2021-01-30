const fs = require('fs')
const path = require('path')
const verificationBackups = require('./verificationBackups')

const BASE_SAMPLE_HORAIRE = '/tmp/mg-verificationbackups/horaire'
const BASE_SAMPLE_QUOTIDIEN = '/tmp/mg-verificationbackups/quotidien'
const BASE_SAMPLE_ANNUEL = '/tmp/mg-verificationbackups/annuel'
const BASE_SAMPLE_LOAD = '/tmp/mg-verificationbackups/load'

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
      fs.statSync(BASE_SAMPLE_HORAIRE)
    } catch(err) {
      console.info("Creation des archives de test de VerificationBackups")
      const sampleCreation = require('./sampleCreation')
      jest.setTimeout(300000)  // Donner 5 minutes pour creer les samples
      await sampleCreation.creerSamplesHoraire()
    }
  })

  it('parcourirBackupsHoraire', async() =>{
    const repertoireSample = path.join(BASE_SAMPLE_HORAIRE, 'sample1')

    const cb = async function(catalogue, cataloguePath) {
      // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
      expect(cataloguePath).toBeDefined()
      expect(catalogue.heure).toBeDefined()
    }

    expect.assertions(11)
    const resultat = await verificationBackups.parcourirBackupsHoraire(pathConsignation(repertoireSample), 'domaine.test', cb)
    console.debug("parcourirBackupsHoraire resultat : %O", resultat)
    expect(resultat.chainage).toBeNull()
    expect(resultat.erreursHachage).toBeNull()
    expect(resultat.erreursCatalogues).toBeNull()
  })

  it('parcourirBackupsHoraire hachage', async() =>{
    const repertoireSample = path.join(BASE_SAMPLE_HORAIRE, 'sample1')

    const cb = async function(catalogue, cataloguePath) {
      // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
      expect(cataloguePath).toBeDefined()
      expect(catalogue.heure).toBeDefined()
    }

    expect.assertions(12)
    const resultat = await verificationBackups.parcourirBackupsHoraire(
      pathConsignation(repertoireSample), 'domaine.test', cb, {verification_hachage: true, verification_enchainement: true}
    )
    console.debug("parcourirBackupsHoraire hachage resultat : %O", resultat)
    expect(resultat.chainage.hachage_contenu).toBe('hachage-0')
    expect(resultat.chainage.uuid_transaction).toBe('uuid-0')
    expect(resultat.erreursHachage.length).toBe(0)
    expect(resultat.erreursCatalogues.length).toBe(0)
  })
})

// describe('VerificationBackups integration logique backups quotidiens', ()=>{
//
//   // Creation des samples (uniquement invoque si repertoire n'existe pas)
//   it('generer archives horaire test', async ()=>{
//     try {
//       fs.statSync(BASE_SAMPLE_QUOTIDIEN)
//     } catch(err) {
//       console.info("Creation des archives de test de VerificationBackups")
//       const sampleCreation = require('./sampleCreation')
//       jest.setTimeout(300000)  // Donner 5 minutes pour creer les samples
//       await sampleCreation.creerSamplesQuotidien()
//     }
//   })
//
//   it('parcourir 1 backup quotidien', async() =>{
//     const repertoireSample = path.join(BASE_SAMPLE_QUOTIDIEN, 'sample2')
//
//     const cb = function(catalogue, cataloguePath) {
//       // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
//       expect(cataloguePath).toBeDefined()
//       expect(catalogue.heure||catalogue.jour).toBeDefined()
//     }
//
//     expect.assertions(11)
//     const resultat = await verificationBackups.parcourirArchivesBackup(pathConsignation(repertoireSample), 'domaine.test', cb)
//     console.debug('parcourir 1 backup quotidien resultat : %O', resultat)
//     expect(resultat.chainage).toBeUndefined()
//     expect(resultat.erreursHachage).toBeNull()
//     expect(resultat.erreursCatalogues).toBeNull()
//   })
//
//   it('parcourir 1 backup quotidien verification enchainement', async() =>{
//     const repertoireSample = path.join(BASE_SAMPLE_QUOTIDIEN, 'sample2')
//
//     const cb = (catalogue, cataloguePath) => {
//       // Rien a faire
//     }
//
//     expect.assertions(2)
//     const info = await verificationBackups.parcourirArchivesBackup(pathConsignation(
//       repertoireSample), 'domaine.test', cb, {verification_enchainement: true})
//     console.debug("parcourir 1 backup quotidien verification enchainement: resultat %O", info)
//     expect(Object.keys(info.erreursCatalogues).length).toBe(0)
//     expect(info.erreursHachage).toBeNull()
//   })
//
//   it('parcourir 1 backup quotidien verification hachage', async() =>{
//     const repertoireSample = path.join(BASE_SAMPLE_QUOTIDIEN, 'sample2')
//
//     const cb = (catalogue, cataloguePath) => {
//       // Rien a faire
//     }
//
//     expect.assertions(2)
//     const info = await verificationBackups.parcourirArchivesBackup(
//       pathConsignation(repertoireSample),
//       'domaine.test',
//       cb,
//       {verification_hachage: true}
//     )
//     console.debug("parcourir 1 backup quotidien hachage: resultat %O", info)
//     expect(info.erreursCatalogues).toBeNull()
//     expect(Object.keys(info.erreursHachage).length).toBe(0)
//   })
//
//   it('parcourir 3 backups quotidiens verification enchainement', async() =>{
//     const repertoireSample = path.join(BASE_SAMPLE_QUOTIDIEN, 'sample3')
//
//     const cb = (catalogue, cataloguePath) => {
//       // Rien a faire
//     }
//
//     expect.assertions(2)
//     const info = await verificationBackups.parcourirArchivesBackup(pathConsignation(
//       repertoireSample), 'domaine.test', cb, {verification_enchainement: true})
//     console.debug("parcourir 3 backups quotidiens verification enchainement: resultat %O", info)
//     expect(Object.keys(info.erreursCatalogues).length).toBe(0)
//     expect(info.erreursHachage).toBeNull()
//   })
//
// })

describe('VerificationBackups integration logique backups annuels', ()=>{

  // // Creation des samples (uniquement invoque si repertoire n'existe pas)
  // it('generer archives annuel test', async ()=>{
  //   try {
  //     fs.statSync(BASE_SAMPLE_ANNUEL)
  //   } catch(err) {
  //     console.info("Creation des archives de test de VerificationBackups")
  //     const sampleCreation = require('./sampleCreation')
  //     jest.setTimeout(300000)  // Donner 5 minutes pour creer les samples
  //     await sampleCreation.creerSamplesAnnuel()
  //   }
  // })
  //
  // it('parcourir 1 backup annuel', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE_ANNUEL, 'sample4')
  //
  //   const cb = function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     expect(catalogue.heure||catalogue.jour||catalogue.annee).toBeDefined()
  //   }
  //
  //   expect.assertions(42)
  //   const resultat = await verificationBackups.parcourirArchivesBackup(pathConsignation(repertoireSample), 'domaine.test', cb)
  //   console.debug("parcourir 1 backup annuel resultat: %O", resultat)
  // })
  //
  // it('parcourir 1 backup annuel verifications enchainement', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE_ANNUEL, 'sample4')
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
  //   const repertoireSample = path.join(BASE_SAMPLE_ANNUEL, 'sample4')
  //
  //   const cb = function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     expect(catalogue.heure||catalogue.jour||catalogue.annee).toBeDefined()
  //   }
  //
  //   expect.assertions(45)
  //   const resultat = await verificationBackups.parcourirArchivesBackup(
  //     pathConsignation(repertoireSample), 'domaine.test', cb, {verification_hachage: true})
  //   console.debug("parcourir 1 backup annuel hachage: %O", resultat)
  //   expect(resultat.erreursHachage.length).toBe(0)
  //   expect(resultat.erreursCatalogues).toBeNull()
  //   expect(resultat.chainage).toBeUndefined()
  // })
  //
  // it('parcourir mix de backups annuel/quotidiens/horaire', async() =>{
  //   const repertoireSample = path.join(BASE_SAMPLE_ANNUEL, 'sample5')
  //
  //   const cb = function(catalogue, cataloguePath) {
  //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
  //     expect(cataloguePath).toBeDefined()
  //     expect(catalogue.heure||catalogue.jour||catalogue.annee).toBeDefined()
  //   }
  //
  //   expect.assertions(169)
  //   const resultat = await verificationBackups.parcourirDomaine(pathConsignation(repertoireSample), 'domaine.test', cb)
  //   console.debug("parcourir mix de backups annuel/quotidiens/horaire: %O", resultat)
  //   expect(resultat.erreursHachage).toBeNull()
  //   expect(resultat.erreursCatalogues).toBeNull()
  //   expect(resultat.chainage).toBeNull()
  // })

  it('parcourir mix de backups annuel/quotidiens/horaire avec verifications', async() =>{
    const repertoireSample = path.join(BASE_SAMPLE_ANNUEL, 'sample5')

    const cb = function(catalogue, cataloguePath) {
      // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
      expect(cataloguePath).toBeDefined()
      expect(catalogue.heure||catalogue.jour||catalogue.annee).toBeDefined()
    }

    expect.assertions(169)
    const resultat = await verificationBackups.parcourirDomaine(
      pathConsignation(repertoireSample), 'domaine.test', cb,
      {verification_hachage: true, verification_enchainement: true}
    )
    console.debug("Resultat parcourir mix de backups annuel/quotidiens/horaire avec verifications: %O", resultat)
    expect(resultat.erreursHachage.length).toBe(0)
    expect(resultat.erreursCatalogues.length).toBe(0)
    expect(resultat.chainage).toBeDefined()

  })

//   // it('output catalogues horaires vers stream', async() =>{
//   //   const repertoireSample = path.join(BASE_SAMPLE_ANNUEL, 'sample6')
//   //
//   //   const outputstream = fs.createWriteStream('/tmp/outcats.txt')
//   //
//   //   const cb = function(catalogue, cataloguePath) {
//   //     // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
//   //     expect(cataloguePath).toBeDefined()
//   //     if(catalogue.heure) {
//   //       outputstream.write(JSON.stringify(catalogue))
//   //       outputstream.write('\n')
//   //     }
//   //   }
//   //
//   //   expect.assertions(124)
//   //   await verificationBackups.parcourirDomaine(pathConsignation(repertoireSample), 'domaine.test', cb)
//   // })

})

describe("Verification backups load test", ()=>{

  // Creation des samples (uniquement invoque si repertoire n'existe pas)
  it('generer archives annuel test', async ()=>{
    try {
      fs.statSync(BASE_SAMPLE_LOAD)
    } catch(err) {
      console.info("Creation des archives de test de VerificationBackups")
      const sampleCreation = require('./sampleCreation')
      jest.setTimeout(300000)  // Donner 5 minutes pour creer les samples
      await sampleCreation.creerSamplesLoad()
    }
  })

//   it('parcourir 1 backup annuel complet (365 jours avec 24 heures chaque)', async() =>{
//     const repertoireSample = path.join(BASE_SAMPLE_LOAD, 'sample6')
//
//     const cb = function(catalogue, cataloguePath) {
//       // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
//       expect(cataloguePath).toBeDefined()
//       expect(catalogue.heure||catalogue.jour||catalogue.annee).toBeDefined()
//     }
//
//     expect.assertions(18252)
//     jest.setTimeout(120000)
//     const resultat = await verificationBackups.parcourirArchivesBackup(
//       pathConsignation(repertoireSample), 'domaine.test', cb,
//       {verification_hachage: true, verification_enchainement: true}
//     )
//     console.debug("parcourir 1 backup annuel complet (365 jours avec 24 heures chaque), resultat\n%O", resultat)
//
//     // expect(resultat.length).toBe(0)
//   })
})
