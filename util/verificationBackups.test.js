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

describe('VerificationBackups', ()=>{

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

  it('parcourirBackupsHoraire', async() =>{
    const repertoireSample = path.join(BASE_SAMPLE, 'sample1')

    const cb = async function(catalogue, cataloguePath) {
      // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
      expect(cataloguePath).toBeDefined()
      expect(catalogue.heure).toBeDefined()
    }

    expect.assertions(8)
    await verificationBackups.parcourirBackupsHoraire(pathConsignation(repertoireSample), 'domaine.test', cb)
  })

  it('parcourirBackupsQuotidiens', async() =>{
    const repertoireSample = path.join(BASE_SAMPLE, 'sample2')

    const cb = function(catalogue, cataloguePath) {
      // console.debug("Catalogue path: %s, catalogue: %O", cataloguePath, catalogue)
      expect(cataloguePath).toBeDefined()
      expect(catalogue.heure||catalogue.jour).toBeDefined()
    }

    expect.assertions(8)
    await verificationBackups.parcourirBackupsQuotidiens(pathConsignation(repertoireSample), 'domaine.test', cb)
  })

})
