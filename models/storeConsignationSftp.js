const debugLib = require('debug')
const debug = debugLib('consignation:store:sftp')
// const debugSftp = debugLib('consignation:store:sftpTrace')
const fs = require('fs')
const path = require('path')
const readdirp = require('readdirp')
// const {Client} = require('ssh2')
const { Readable } = require('stream')
const lzma = require('lzma-native')

const { Hacheur, VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')
// const { writer } = require('repl')
// const hachage = require('@dugrema/millegrilles.nodejs/src/hachage')
// const { sauvegarderBackupTransactions, rotationBackupTransactions } = require('./storeConsignationLocal')
const { chargerConfiguration, modifierConfiguration } = require('./storeConsignationLocal')
const SftpDao = require('./sftpDao')

// Charger les cles privees utilisees pour se connecter par sftp
// Ed25519 est prefere, RSA comme fallback
const _privateEd25519KeyPath = process.env.SFTP_ED25519_KEY || '/run/secrets/sftp.ed25519.key.pem'
const _privateEd25519Key = fs.readFileSync(_privateEd25519KeyPath)
const _privateRsaKeyPath = process.env.SFTP_RSA_KEY || '/run/secrets/sftp.rsa.key.pem'
const _privateRsaKey = fs.readFileSync(_privateRsaKeyPath)

// Creer un pool de connexions a reutiliser
// const CONNEXION_TIMEOUT = 10 * 60 * 1000  // 10 minutes
const CHUNK_SIZE = 32768 - 29  // Packet ssh est 32768 sur hostgator, 29 bytes overhead

const CONSIGNATION_PATH = process.env.MG_CONSIGNATION_PATH || '/var/opt/millegrilles/consignation'
const PATH_BACKUP_TRANSACTIONS_DIR = path.join(CONSIGNATION_PATH, 'backup', 'transactions')
// const PATH_CONFIG_DIR = path.join(CONSIGNATION_PATH, 'config')
// const PATH_CONFIG_FICHIER = path.join(PATH_CONFIG_DIR, 'store.json')

// let _intervalEntretienConnexion = null,
//     _connexionSsh = null,
//     _channelSftp = null,
//     _supporteSshExtensions = true,
//     _connexionError = null

const _sftpDao = new SftpDao()

// let _hostname = null,
//     _port = 22,
//     _username = null,
//     _urlDownload = null,
//     _remotePath = '.',
//     _keyType = 'ed25519'

let _urlDownload = null,
    _remotePath = '.'

async function init(params) {
    params = params || {}

    // const {hostname_sftp, username_sftp, url_download, port_sftp, remote_path_sftp, key_type_sftp} = params
    // if(!hostname_sftp) throw new Error("Parametre hostname manquant")
    // if(!username_sftp) throw new Error("Parametre username manquant")

    const { remote_path_sftp, url_download } = params
    if(!url_download) throw new Error("Parametre urlDownload manquant")

    // _hostname = hostname_sftp
    // _username = username_sftp
    _urlDownload = new URL(''+url_download).href
    // _keyType = key_type_sftp || 'ed25519'
    
    // _port = port_sftp || _port
    _remotePath = remote_path_sftp || _remotePath

    // if(!_intervalEntretienConnexion) {
    //    _intervalEntretienConnexion = setInterval(entretienConnexion, CONNEXION_TIMEOUT/2)
    // }

    // debug("Init, connecter ssh sur %s@%s:%d", _username, _hostname, _port)
    // return connecterSSH(_hostname, _port, _username, params)
    _sftpDao.fermer()  // Si connexion deja ouverte (config change)
    return _sftpDao.init(params)
}

function fermer() {
    // if(_intervalEntretienConnexion) clearInterval(_intervalEntretienConnexion)
    // entretienConnexion({closeAll: true}).catch(err=>console.error("ERROR %O Erreur fermeture connexion sftp : %O", new Date(), err))
    return _sftpDao.fermer()
}

// async function entretienConnexion(opts) {
//     opts = opts || {}
//     if(opts.closeAll) {
//         debug("entretientConnexion closeAll")
//         if(_connexionSsh) _connexionSsh.end()
//     }
// }

function getPathFichier(fuuid) {
    return path.join(_remotePath, 'local', fuuid)
}

function getPathWork(fuuid) {
    if(fuuid) {
        return path.join(_remotePath, 'work', fuuid)
    } else {
        return path.join(_remotePath, 'work')
    }
}

async function getInfoFichier(fuuid) {
    debug("getInfoFichier %s", fuuid)
    const url = new URL(_urlDownload)
    url.pathname = path.join(url.pathname, fuuid)
    const fileRedirect = url.href
    debug("File redirect : %O", fileRedirect)
    return { fileRedirect }
}

async function recoverFichierSupprime(fuuid) {
    debug("recoverFichierSupprime %s", fuuid)
    const filePath = getPathFichier(fuuid)
    const filePathCorbeille = filePath + '.corbeille'
    try {
        // var sftp = await sftpClient()
        await _sftpDao.stat(filePathCorbeille)
        await _sftpDao.rename(filePathCorbeille, filePath)
        const statInfo = await _sftpDao.stat(filePath)
        return { stat: statInfo, filePath }
    } catch(err) {
        debug("Erreur recoverFichierSupprime %s : %O", fuuid, err)
        return null
    } finally {
        try {
            debug("recoverFichierSupprime fermer sftp apres %s", fuuid)
            // sftp.end(err=>console.debug("SFTP end, resultat : %O", err))
        } catch(err) {
            debug("ERR sftp.end: %O", err)
        }
    }
}

async function consignerFichier(pathFichierStaging, fuuid) {
    debug("Consigner fichier fuuid %s", fuuid)
    // let sftp = await sftpClient()
    await _sftpDao.mkdir(getPathWork())

    const pathFichier = getPathWork(fuuid)

    // Lire toutes les parts et combiner dans la destination

    const promiseReaddirp = readdirp(pathFichierStaging, {
        type: 'files',
        fileFilter: '*.part',
        depth: 1,
    })

    // Creer hacheur en sha2-256 pour verifier avec sha256sums via ssh apres l'upload
    const hacheur = new Hacheur({encoding: 'base16', hash: 'sha2-256'})
    // Verifier contenu local uploade
    const verificateurHachage = new VerificateurHachage(fuuid)

    let positionVerif = 0
    const callbackVerif = async (chunk, position) => {
        if(positionVerif === position) {  // Handle retry
            await hacheur.update(chunk)
            await verificateurHachage.update(chunk)
            positionVerif += chunk.length
        } else if(positionVerif > position) {
            debug("Verification, skip position previous (retry?): %d < %d", position, positionVerif)
        } else {
            debug("Erreur hachage, position out of sync (%d != %d)", position, positionVerif)
        }
    }

    try {
        var writeHandle = await _sftpDao.open(pathFichier, 'w', 0o644)

        const listeParts = []
        for await (const entry of promiseReaddirp) {
            const fichierPart = entry.basename
            const position = Number(fichierPart.split('.').shift())
            listeParts.push({position, fullPath: entry.fullPath})
        }
        listeParts.sort((a,b)=>{return a.position-b.position})

        let retryCount = 0,
            fileSize = 0
        debug("consignerFichier Conserver %s avec parts %O", fuuid, listeParts)
        for await (const entry of listeParts) {
            while(retryCount++ < 3) {
                try {
                    const { total } = await _sftpDao.putFile(fuuid, entry, writeHandle, callbackVerif)

                    // Succes, break boucle retry
                    fileSize = total  // Total est la position finale a l'ecriture
                    retryCount = 0
                    break
                } catch(err) {
                    if(retryCount < 3) {
                        debug("Erreur putfile %s, reessayer. Detail : %O", fuuid, err)
                        // if(!_connexionSsh) {
                        //     // Reconnecter, restaurer etat ecriture
                        //     await connecterSSH()
                        //     sftp = await sftpClient()
                            writeHandle = await _sftpDao.open(pathFichier, 'r+', 0o644)
                        // }
                    }
                }
            }
        }

        // await writer.close()
        const multiHachageSha256 = await hacheur.finalize()
        const hachageSha256 = multiHachageSha256.slice(5)  // Retirer 5 premiers chars (f multibase, multihash)
        debug("Fichier %s sftp upload, sha-256 local = %s, taille %s", fuuid, hachageSha256, fileSize)
        await verificateurHachage.verify()
        debug("Fichier %s transfere avec succes vers consignation sftp, sha-256 local = %s", fuuid, hachageSha256)

        // Attendre que l'ecriture du fichier soit terminee (fs sync)
        // const handle = await open(pathFichier, 'r')
        let infoFichier = null
        debug("Attente sync du fichier %s", pathFichier)

        debug("Execution fstat sur %s", fuuid)
        infoFichier = await _sftpDao.fstat(writeHandle)
        debug("Stat fichier initial avant attente : %O", infoFichier)

        debug("Info fichier : %O", infoFichier)

        if(infoFichier.size !== fileSize) {
            const err = new Error(`Taille du fichier est differente sur sftp : ${infoFichier.size} != ${fileSize}`)
            debug("Erreur taille fichier: %O", err)
            throw err
        }

        debug("Information fichier sftp : %O", infoFichier)

        // Renommer de work/fuuid a local/fuuid
        await _sftpDao.rename(pathFichier, getPathFichier(fuuid))

    } catch(err) {
        try {
            await _sftpDao.unlink(pathFichier)
        } catch(err) {
            console.error("Erreur unlink fichier sftp %s : %O", pathFichier, err)
        }
        throw err
    } finally {
        try { await _sftpDao.close(writeHandle) }
        catch(err) { debug("Erreur fermeture writeHandle %s : %O", fuuid, err)}
        try { 
            debug("consignerFichier fermer sftp apres %s", fuuid)
            // sftp.end(err=>debug("Fermeture sftp : %O", err)) 
        }
        catch(err) { debug("ERR fermerture sftp : %O", err)}
    }

}

// async function putFile(sftp, fuuid, entry, writeHandle, callback) {
//     const {position, fullPath} = entry

//     // debug("Entry path : %O", entry);
//     debug("Traiter consignation pour item %s position %d", fuuid, position)
//     const streamReader = fs.createReadStream(fullPath, {highWaterMark: CHUNK_SIZE})

//     const resultat = await writeStream(sftp, streamReader, writeHandle, callback, {position})

//     debug("Bytes fichier %s ecits : %d", fuuid, resultat.total - position)
//     return resultat
// }

// async function writeStream(sftp, streamReader, writeHandle, callback, opts) {
//     opts = opts || {}
//     let filePosition = opts.position || 0

//     // debug("Entry path : %O", entry);
//     debug("putStream position %d", filePosition)

//     const promise = new Promise((resolve, reject)=>{
//         let termine = false,
//             ecritureEnCours = false
//         streamReader.on('end', _=>{
//             termine = true
//             if(ecritureEnCours) {
//                 debug("end appele, ecriture en cours : %s", ecritureEnCours)
//             } else {
//                 resolve({total: filePosition})
//             }
//         })
//         streamReader.on('error', err=>reject(err))
//         streamReader.on('data', async chunk => {
//             ecritureEnCours = true
//             if(termine) return reject("Stream / promise resolved avant fin ecriture")

//             // Verifier hachage
//             streamReader.pause()
//             // verificateurHachage.update(chunk)

//             // Ecrire le contenu du chunk
//             debugSftp("Ecrire position %d (offset %d, len: %d)", filePosition, 0, chunk.length)
//             try {
//                 // await new Promise(resolve=>setTimeout(resolve, 20))  // Throttle, aide pour hostgator
//                 await write(sftp, writeHandle, chunk, 0, chunk.length, filePosition)
//                 if(callback) await callback(chunk, filePosition)
//             } catch(err) {
//                 streamReader.close()
//                 return reject(err)
//             }

//             // Incrementer position globale
//             filePosition += chunk.length

//             debugSftp("Ecriture rendu position %d (offset %d)", filePosition)

//             ecritureEnCours = false
//             if(termine) return resolve({total: filePosition})

//             streamReader.resume()
//         })
//     })

//     const resultat = await promise

//     debug("Resultat ecriture stream ", resultat)

//     return resultat
// }

// async function connecterSSH(host, port, username, opts) {
//     opts = opts || {}
//     host = host || _hostname
//     port = port || _port
//     username = username || _username
//     const connexionName = username + '@' + host + ':' + port
  
//     debug("Connecter SSH sur %s (opts: %O)", connexionName, opts)
  
//     // await new Promise(resolve=>setTimeout(resolve, 3000))  // Delai connexion

//     var privateKey = _privateEd25519Key
//     const keyType = opts.keyType || _keyType || 'ed25519'
//     if(keyType === 'rsa') {
//         privateKey = _privateRsaKey
//     }
  
//     const conn = new Client()

//     _connexionError = null
//     return new Promise((resolve, reject)=>{
//         conn.on('ready', async _ => {
//             debug("Connexion ssh ready")
//             _connexionSsh = conn
//             try {
//                 _channelSftp = await sftpClient()
//                 resolve(conn)
//             } catch(err) {
//                 debug("Erreur ouverture sftp : %O", err)
//                 reject(err)
//             }
//         })
//         conn.on('error', err=>{
//             debug("Erreur connexion SFTP : %O", err)
//             _connexionError = err
//             _channelSftp = null
//             _connexionSsh = null
//             reject(err)
//         })
//         conn.on('end', _=>{
//             debug("connecterSSH.end Connexion %s fermee, nettoyage pool", connexionName)
//             _channelSftp = null
//             _connexionSsh = null
//         })

//         conn.connect({
//             host, port, username, privateKey,
//             readyTimeout: 60000,  // 60s, regle probleme sur login hostgator
//             debug: debugSftp,
//         })
//     })
// }

// async function sftpClient() {
//     if(_channelSftp) return _channelSftp

//     debug("!!! sftpClient ")
//     if(!_connexionSsh) await connecterSSH()
//     return new Promise(async (resolve, reject)=>{
//         try {
//             debug("Ouverture sftp")
//             _connexionSsh.sftp((err, sftp)=>{
//                 debug("Sftp ready, err?: %O", err)
//                 if(err) return reject(err)

//                 sftp.on('end', ()=>{
//                     debug("Channel SFTP end")
//                     _channelSftp = null
//                 })
//                 sftp.on('error', ()=>{
//                     debug("ERROR Channel SFTP : %O", err)
//                 })

//                 resolve(sftp)
//             })
//         } catch(err) {
//             reject(err)
//         }
//     })
// }

async function marquerSupprime(fuuid) {
    try {
        // var sftp = await sftpClient()
        const pathFichier = getPathFichier(fuuid)
        const pathFichierSupprime = pathFichier + '.corbeille'
        await _sftpDao.rename(pathFichier, pathFichierSupprime)
    } finally {
        // sftp.end(err=>debug("SFTP end : %O", err))
    }
}

async function parcourirFichiers(callback, opts) {
    debug("Parcourir fichiers")
    try {
        // var sftp = await sftpClient()
        const pathLocal = getRemotePathFichiers()
        debug("sftp.parcourirFichiers Path local %s", pathLocal)
        await _sftpDao.parcourirFichiersRecursif(pathLocal, callback, opts)
        await callback()  // Dernier appel avec aucune valeur (fin traitement)
    } finally {
        debug("parcourirFichiers fermer sftp")
        // sftp.end(err=>debug("SFTP end : %O", err))
    }
}

function getRemotePathFichiers() {
    return path.join(_remotePath, 'local')
}

function getRemotePathBackup() {
    return path.join(_remotePath, 'backup')
}

function getRemotePathBackupTransactions() {
    return path.join(_remotePath, 'backup', 'transactions')
}

async function parcourirBackup(callback, opts) {
    debug("Parcourir backup")
    
    try {
        // var sftp = await sftpClient()
        const pathLocal = getRemotePathBackup()
        await _sftpDao.parcourirFichiersRecursif(pathLocal, callback, opts)
        await callback()  // Dernier appel avec aucune valeur (fin traitement)
    } finally {
        debug("parcourirBackup fermer sftp")
        // sftp.end(err=>debug("SFTP end : %O", err))
    }
}

// async function parcourirFichiersRecursif(sftp, repertoire, callback, opts) {
//     opts = opts || {}
//     debug("parcourirFichiers %s", repertoire)

//     const handleDir = await opendir(sftp, repertoire)

//     if(opts.direntry === true) {
//         callback({directory: repertoire})
//     }

//     try {
//         let liste = await readdir(sftp, handleDir)
//         while(liste) {
//             // Filtrer la liste, conserver uniquement les fuuids (fichiers, enlever extension)
//             // Appeler callback sur chaque item
//             var infoFichiers = liste.filter(item=>{
//                 let isFile = item.attrs.isFile()
//                 if(opts.filtre) {
//                     const data = { filename: item.filename, directory: repertoire, modified: item.attrs.mtime }
//                     return isFile && opts.filtre(data)
//                 }
//                 return isFile
//             })
//             if(infoFichiers && infoFichiers.length > 0) {
//                 for(let fichier of infoFichiers) {
//                     // debug("parcourirFichiersRecursif Fichier ", fichier)
//                     const data = { 
//                         filename: fichier.filename, 
//                         directory: repertoire, 
//                         modified: fichier.attrs.mtime, 
//                         size: fichier.attrs.size 
//                     }
//                     await callback(data)
//                 }
//             }
        
//             // Parcourir recursivement tous les repertoires
//             const directories = liste.filter(item=>item.attrs.isDirectory())
//             for await (const directory of directories) {
//                 const subDirectory = path.join(repertoire, directory.filename)
//                 await parcourirFichiersRecursif(sftp, subDirectory, callback, opts)
//             }

//             try {
//                 liste = await readdir(sftp, handleDir)
//             } catch (err) {
//                 liste = false
//             }
//         }

//     } finally {
//         close(sftp, handleDir)
//     }
// }

async function sauvegarderBackupTransactions(message) {

    const { domaine, partition, date_transactions_fin } = message
    const { uuid_transaction } = message['en-tete']

    const dateFinBackup = new Date(date_transactions_fin * 1000)
    debug("Sauvegarde du backup %s date %O", domaine, dateFinBackup)

    // Formatter le nom du fichier avec domaine_partition_DATE
    const dateFinString = dateFinBackup.toISOString().replaceAll('-', '').replaceAll(':', '')
    const nomFichierList = [domaine]
    if(partition) nomFichierList.push(partition)
    nomFichierList.push(dateFinString)
    nomFichierList.push(uuid_transaction.slice(0,8))  // Ajouter valeur "random" avec uuid_transaction
    
    const nomFichier = nomFichierList.join('_') + '.json.xz'
    const pathFichier = path.join(domaine, nomFichier)

    // Compresser en lzma et conserver
    const messageCompresse = await lzma.compress(JSON.stringify(message), 9)

    await pipeBackupTransactionStream(pathFichier, toStream(messageCompresse))

    debug("Backup %s date %O sauvegarde sous %O", domaine, dateFinBackup, pathFichier)
}

function toStream(bytes) {
    const byteReader = new Readable()
    byteReader._read = () => {}
    
    byteReader.push(bytes)
    byteReader.push(null)

    return byteReader
}

async function rotationBackupTransactions(message) {

    const { domaine, partition } = message
    debug("rotationBackupTransactions", domaine, partition)

    const maxArchives = 1

    const pathDomaine = path.join(getRemotePathBackupTransactions(), domaine)
    // const sftp = await sftpClient()
    await _sftpDao.rmdir(pathDomaine)

    console.warn(" !!! NOT IMPLEMENTED : storeConsignationSftp.rotationBackupTransactions !!! ")
}

async function getFichiersBackupTransactionsCourant(mq, replyTo) {
    throw new Error('not implemented')

    // Parcourir repertoire
    const promiseReaddirp = readdirp(PATH_BACKUP_TRANSACTIONS_DIR, {
        type: 'files',
        fileFilter: '*.json.xz',
        depth: 2,
    })

    let clesAccumulees = {}
    let countCles = 0

    for await (const entry of promiseReaddirp) {
        debug("Fichier backup transactions : %O", entry)
        const nomFichier = entry.path
        let contenu = await fsPromises.readFile(entry.fullPath)
        contenu = await lzma.decompress(contenu)
        debug("Contenu archive str : %O", contenu)
        contenu = JSON.parse(contenu)
        debug("Contenu archive : %O", contenu)

        const cle = contenu.cle
        clesAccumulees[nomFichier] = cle
        countCles++

        if(countCles >= 1000) {
            debug("Emettre message %d cles (batch)", countCles)
            await emettreMessageCles(mq, replyTo, clesAccumulees, false)

            // Clear
            clesAccumulees = {}
            countCles = 0
        }
    }

    debug("Emettre message %d cles (final)", countCles)
    await emettreMessageCles(mq, replyTo, clesAccumulees, true)

    return {ok: true}
}

async function emettreMessageCles(mq, replyTo, cles, complet) {
    const reponse = { ok: true, cles, complet }
    await mq.transmettreReponse(reponse, replyTo, 'cles')
}

async function getBackupTransaction(pathBackupTransaction) {
    const readStream = getRemotePathBackupTransactions(pathBackupTransaction)
    contenu = await lzma.decompress(readStream)

    debug("Contenu archive str : %O", contenu)
    contenu = JSON.parse(contenu)

    debug("Contenu archive : %O", contenu)
    return contenu
}

async function getBackupTransactionStream(pathBackupTransaction) {
    const pathDomaine = path.join(getRemotePathBackupTransactions(), pathBackupTransaction)
    // const sftp = await sftpClient()
    return _sftpDao.createReadStream(pathDomaine)
}

async function pipeBackupTransactionStream(pathFichier, stream) {
    
    // const sftp = await sftpClient()

    const pathFichierParsed = path.parse(pathFichier)
    debug("pipeBackupTransactionStream ", pathFichierParsed)
    const dirBackup = getRemotePathBackupTransactions()
    await _sftpDao.mkdir(dirBackup)
    const dirFichier = path.join(dirBackup, pathFichierParsed.dir)
    await _sftpDao.mkdir(dirFichier)
    
    const pathFichierComplet = path.join(dirBackup, pathFichier)
    
    const writeHandle = await _sftpDao.open(pathFichierComplet, 'w', 0o644)
    const resultat = await _sftpDao.writeStream(stream, writeHandle)

    return resultat
}

async function deleteBackupTransaction(pathBackupTransaction) {
    const dirBackup = getRemotePathBackupTransactions()
    const pathFichier = path.join(dirBackup, pathBackupTransaction)
    // const sftp = await sftpClient()
    await _sftpDao.unlink(pathFichier)
}

// function mkdir(sftp, pathRepertoire) {
//     return new Promise(async (resolve, reject)=>{
//         debug("mkdir ", pathRepertoire)
//         sftp.mkdir(pathRepertoire, (err, info)=>{
//             if(err) {
//                 if(err.code === 4) {
//                     // Ok, repertoire existe deja
//                 } else {
//                     return reject(err)
//                 }
//             }
//             resolve(info)
//         })
//     })
// }

// async function rmdir(sftp, pathRepertoire, opts) {
//     opts = opts || {}
//     debug("rmdir ", pathRepertoire)

//     const liste = []

//     const callback = async info => {
//         liste.push(info)
//     }

//     await parcourirFichiersRecursif(sftp, pathRepertoire, callback, {direntry: true})

//     // Inverser la liste, supprimer sous-repertoires en premier
//     liste.reverse()
    
//     for await (let info of liste) {
//         if(info.filename) {
//             const nomFichier = path.join(info.directory, info.filename)
//             await unlink(sftp, nomFichier)
//         } else {
//             // directory, suppression
//             debug("Supprimer repertoire ", info.directory)
//             await new Promise((resolve, reject)=>{
//                 sftp.rmdir(info.directory, (err, info)=>{
//                     if(err) {
//                         if(err.code === 2) {
//                             // OK, n'existe pas
//                         } else {
//                             return reject(err)
//                         }
//                     }
//                     resolve(info)
//                 })
//             })
//         }
//     }
// }

// function opendir(sftp, pathRepertoire) {
//     return new Promise(async (resolve, reject)=>{
//         sftp.opendir(pathRepertoire, (err, info)=>{
//             if(err) return reject(err)
//             resolve(info)
//         })
//     })
// }

// function readdir(sftp, pathRepertoire) {
//     return new Promise(async (resolve, reject)=>{
//         sftp.readdir(pathRepertoire, (err, info)=>{
//             if(err) return reject(err)
//             resolve(info)
//         })
//     })
// }

// function rename(sftp, srcPath, destPath) {
//     return new Promise(async (resolve, reject)=>{
//         sftp.rename(srcPath, destPath, err=>{
//             if(err) return reject(err)
//             resolve()
//         })
//     })
// }

// function stat(sftp, pathFichier) {
//     return new Promise(async (resolve, reject)=>{
//         sftp.stat(pathFichier, (err, info)=>{
//             if(err) return reject(err)
//             resolve(info)
//         })
//     })
// }

// function unlink(sftp, pathFichier) {
//     return new Promise(async (resolve, reject)=>{
//         debug("unlink fichier ", pathFichier)
//         sftp.unlink(pathFichier, (err, info)=>{
//             if(err) return reject(err)
//             resolve(info)
//         })
//     })
// }

// function open(sftp, pathFichier, flags, mode) {
//     mode = mode || 0o644
//     return new Promise(async (resolve, reject)=>{
//         sftp.open(pathFichier, flags, mode, (err, info)=>{
//             if(err) return reject(err)
//             resolve(info)
//         })
//     })
// }

// function write(sftp, handle, buffer, offset, length, position) {
//     return new Promise((resolve, reject)=>{
//         sftp.write(handle, buffer, offset, length, position, err=>{
//             if(err) return reject(err)
//             resolve()
//         })
//     })
// }

// function sync(sftp, handle) {
//     if(!_supporteSshExtensions) return 
//     return new Promise(async (resolve, reject)=>{
//         try {
//             sftp.ext_openssh_fsync(handle, err=>{
//                 if(err) {
//                     debug("Erreur SYNC (1), on assume manque de support de openssh extensions : %O", err)
//                     _supporteSshExtensions = false  // toggle support extensions a false
//                     // return reject(err)
//                     resolve(false)
//                 }
//                 resolve(true)
//             })
//         } catch(err) {
//             debug("Erreur SYNC (2), on assume manque de support de openssh extensions : %O", err)
//             _supporteSshExtensions = false  // toggle support extensions a false
//             resolve(false)
//         }
//     })
// }

// function fstat(sftp, handle) {
//     return new Promise(async (resolve, reject)=>{
//         sftp.fstat(handle, (err, info)=>{
//             if(err) return reject(err)
//             resolve(info)
//         })
//     })
// }

// function close(sftp, handle) {
//     return new Promise(async (resolve, reject)=>{
//         sftp.close(handle, (err, info)=>{
//             if(err) return reject(err)
//             resolve(info)
//         })
//     })
// }

module.exports = {
    init, fermer,
    chargerConfiguration, modifierConfiguration,
    getInfoFichier, consignerFichier, 
    marquerSupprime, recoverFichierSupprime,
    parcourirFichiers,

    // Backup
    parcourirBackup,
    sauvegarderBackupTransactions, rotationBackupTransactions,
    getFichiersBackupTransactionsCourant, getBackupTransaction,
    getBackupTransactionStream, pipeBackupTransactionStream, deleteBackupTransaction,
}

// module.exports = {
//     init, 
//     chargerConfiguration, modifierConfiguration,
//     getInfoFichier, consignerFichier,
//     marquerSupprime, recoverFichierSupprime, 
//     parcourirFichiers, parcourirBackup,
//     sauvegarderBackupTransactions, rotationBackupTransactions,
//     getFichiersBackupTransactionsCourant, getBackupTransaction,
//     getBackupTransactionStream, pipeBackupTransactionStream, deleteBackupTransaction,
// }
