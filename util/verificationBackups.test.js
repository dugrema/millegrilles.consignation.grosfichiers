const fs = require('fs')

describe('VerificationBackups', ()=>{

  it('generer archives horaire test', async ()=>{
    try {
      fs.statSync('/tmp/mg-verificationbackups')
    } catch(err) {
      console.info("Creation des archives de test de VerificationBackups")
      const sampleCreation = require('./sampleCreation')
      await sampleCreation()
    }
  })

})
