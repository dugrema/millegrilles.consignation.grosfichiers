const debug = require('debug')('consignation:store:root')
const StoreConsignationLocal = require('./storeConsignationLocal')

let _storeConsignation = null,
    _storeConsignationLocal = null

async function init(opts) {
    opts = opts || {}
    // const {typeStore} = opts

    // Toujours initialiser le type local - utilise pour stocker/charger la configuration
    _storeConsignationLocal = StoreConsignationLocal
    _storeConsignationLocal.init(opts)
    const configuration = await _storeConsignationLocal.chargerConfiguration(opts)
    const typeStore = configuration.typeStore

    const params = {...configuration, ...opts}  // opts peut faire un override de la configuration

    await changerStoreConsignation(typeStore, params)
}

async function changerStoreConsignation(typeStore, params, opts) {
    opts = opts || {}
    params = params || {}
    typeStore = typeStore?typeStore.toLowerCase():'local'
    debug("changerStoreConsignation type: %s, params: %O", typeStore, params)

    switch(typeStore) {
        case 'sftp': throw new Error('todo'); break
        case 'awss3': throw new Error('todo'); break
        case 'local': _storeConsignation = _storeConsignationLocal; break
        default: _storeConsignation = _storeConsignationLocal
    }

    await _storeConsignationLocal.modifierConfiguration({...params, typeStore})
}

async function chargerConfiguration(opts) {
    opts = opts || {}
    return await _storeConsignationLocal.chargerConfiguration(opts)
}

async function modifierConfiguration(params, opts) {
    if(params.typeStore) {
        return await changerStoreConsignation(params.typeStore, params, opts)
    }
    return await _storeConsignationLocal.modifierConfiguration(params, opts)
}

module.exports = { init, changerStoreConsignation, chargerConfiguration, modifierConfiguration }
