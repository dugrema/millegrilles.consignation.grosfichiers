// const MilleGrillesAmqpDAO = require('@dugrema/millegrilles.common/lib/amqpdao')
const TraitementFichierBackup = require('./traitementBackup').TraitementFichierBackup

// Mock de amqpdao (RabbitMQ)
jest.mock('@dugrema/millegrilles.common/lib/amqpdao')
const MilleGrillesAmqpDAO = require('@dugrema/millegrilles.common/lib/amqpdao')
MilleGrillesAmqpDAO.mockImplementation(()=>{

  var pki = {
    idmg: "DUMMY"
  }

  return {
    pki,
  }
})

describe('TraitementFichierBackup', ()=>{

  var traitementFichierBackup;

  beforeAll( ()=>{
    rabbitMQMock = new MilleGrillesAmqpDAO()
    traitementFichierBackup = new TraitementFichierBackup(rabbitMQMock)
    return traitementFichierBackup
  })

  it('devrait etre importe', ()=>{
    expect(TraitementFichierBackup).toBeInstanceOf(Object);
    // expect(MilleGrillesAmqpDAO).toBeInstanceOf(Object);
  });

  // it("executer traiterPutBackup", async () => {
  //   expect.assertions(1)
  //   const req = {
  //     body: 'allo',
  //     autorisationMillegrille: {idmg: "DUMMY"},
  //     files: {transactions: 'allo', catalogue: 'toi'}
  //   }
  //   await traitementFichierBackup.traiterPutBackup(req).then(resultat=>{
  //     console.info("Resultat - %O", resultat)
  //     expect(resultat).toBeTruthy(resultat)
  //   })
  // })

});
