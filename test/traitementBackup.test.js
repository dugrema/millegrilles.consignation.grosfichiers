const fs = require('fs')
const path = require('path')

const traitementBackup = require('./traitementBackup')

PATH_SAMPLES = '/tmp/mg-traitementbackup'

describe('TraitementFichierBackup', ()=>{

  beforeAll(()=>{
    try {
      fs.statSync(PATH_SAMPLES)
    } catch(err) {
      fs.mkdirSync(path.join(PATH_SAMPLES, 'domaine1.main'), {recursive:true})
      fs.mkdirSync(path.join(PATH_SAMPLES, 'domaine2.sous1'))
      fs.mkdirSync(path.join(PATH_SAMPLES, 'domaine2.sous2'))
      fs.mkdirSync(path.join(PATH_SAMPLES, 'domaine3.main'))
      fs.mkdirSync(path.join(PATH_SAMPLES, 'domaine4.main'))
      fs.mkdirSync(path.join(PATH_SAMPLES, 'domaine5.main'))
    }
  })

  it("Test get liste domaines", async () => {
    const resultat = await traitementBackup.identifierDomaines(PATH_SAMPLES)
    console.debug("Test get liste domaines : %O", resultat)
    expect(resultat.length).toBe(6)
    expect(resultat[0]).toBe('domaine1.main')
  })

});
