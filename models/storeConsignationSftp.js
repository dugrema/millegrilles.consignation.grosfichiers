const debug = require('debug')('consignation:store:sftp')
const fs = require('fs')
const path = require('path')
const readdirp = require('readdirp')
const {Client} = require('ssh2')

const { VerificateurHachage } = require('@dugrema/millegrilles.nodejs/src/hachage')
const { writer } = require('repl')

// Charger les cles privees utilisees pour se connecter par sftp
// Ed25519 est prefere, RSA comme fallback
const _privateEd25519KeyPath = process.env.SFTP_ED25519_KEY || '/run/secrets/sftp.ed25519.key.pem'
const _privateEd25519Key = fs.readFileSync(_privateEd25519KeyPath)
const _privateRsaKeyPath = process.env.SFTP_RSA_KEY || '/run/secrets/sftp.rsa.key.pem'
const _privateRsaKey = fs.readFileSync(_privateRsaKeyPath)

// Creer un pool de connexions a reutiliser
const CONNEXION_TIMEOUT = 10 * 60 * 1000  // 10 minutes

let _intervalEntretienConnexion = null,
    _connexionSsh = null,
    _connexionSftp = null,
    _supporteSshExtensions = true

let _hostname = null,
    _port = 22,
    _username = null,
    _urlDownload = null,
    _remotePath = '.',
    _keyType = 'ed25519'

async function init(params) {
    params = params || {}
    const {hostnameSftp, usernameSftp, urlDownload, portSftp, remotePathSftp, keyTypeSftp} = params
    if(!hostnameSftp) throw new Error("Parametre hostname manquant")
    if(!usernameSftp) throw new Error("Parametre username manquant")
    if(!urlDownload) throw new Error("Parametre urlDownload manquant")

    _hostname = hostnameSftp
    _username = usernameSftp
    _urlDownload = new URL(''+urlDownload).href
    _keyType = keyTypeSftp || 'ed25519'
    
    _port = portSftp || _port
    _remotePath = remotePathSftp || _remotePath

    if(!_intervalEntretienConnexion) {
        _intervalEntretienConnexion = setInterval(entretienConnexion, CONNEXION_TIMEOUT/2)
    }

    debug("Init, connecter ssh sur %s@%s:%d", _username, _hostname, _port)
    return connecterSSH(_hostname, _port, _username, params)
}

function fermer() {
    if(_intervalEntretienConnexion) clearInterval(_intervalEntretienConnexion)
    entretienConnexion({closeAll: true}).catch(err=>console.error("ERROR %O Erreur fermeture connexion sftp : %O", new Date(), err))
}

async function entretienConnexion(opts) {
    opts = opts || {}
    if(opts.closeAll) {
        if(_connexionSsh) _connexionSsh.end()
    }
}

function getPathFichier(fuuid) {
    return path.join(_remotePath, fuuid)
}

async function getInfoFichier(fuuid) {
    const url = new URL(_urlDownload)
    url.pathname = path.join(url.pathname, fuuid)
    const fileRedirect = url.href
    debug("File redirect : %O", fileRedirect)
    return { fileRedirect }
}

async function recoverFichierSupprime(fuuid) {
    const filePath = getPathFichier(fuuid)
    const filePathCorbeille = filePath + '.corbeille'
    try {
        await stat(filePathCorbeille)
        await rename(filePathCorbeille, filePath)
        const statInfo = await stat(filePath)
        return { stat: statInfo, filePath }
    } catch(err) {
        debug("Erreur recoverFichierSupprime %s : %O", fuuid, err)
        return null
    }
}

async function consignerFichier(pathFichierStaging, fuuid) {

    if(!_connexionSsh) await connecterSSH(_hostname, _port, _username, params)

    const pathFichier = getPathFichier(fuuid)

    // Lire toutes les parts et combiner dans la destination
    const dirFichier = path.dirname(pathFichier)
    // await fsPromises.mkdir(dirFichier, {recursive: true})
    // const writer = fs.createWriteStream(pathFichier)
    // const writer = _connexionSftp.createWriteStream(pathFichier, {flags: 'w', mode: 0o644, autoClose: true})
    const writeHandle = await open(pathFichier, 'w', 0o644)

    const promiseReaddirp = readdirp(pathFichierStaging, {
        type: 'files',
        fileFilter: '*.part',
        depth: 1,
    })

    const verificateurHachage = new VerificateurHachage(fuuid)
    try {
        const listeParts = []
        for await (const entry of promiseReaddirp) {
            const fichierPart = entry.basename
            const position = Number(fichierPart.split('.').shift())
            listeParts.push({position, fullPath: entry.fullPath})
        }
        listeParts.sort((a,b)=>{return a.position-b.position})

        let total = 0
        for await (const entry of listeParts) {
            const {position, fullPath} = entry
            // debug("Entry path : %O", entry);
            // const fichierPart = entry.basename
            // const position = Number(fichierPart.split('.').shift())
            debug("Traiter consignation pour item %s position %d", fuuid, position)
            const streamReader = fs.createReadStream(fullPath)
            
            streamReader.on('data', async chunk => {
                // Verifier hachage
                streamReader.pause()
                verificateurHachage.update(chunk)
                // writer.write(chunk)
                await write(writeHandle, chunk, 0, chunk.length, total)
                total += chunk.length
                streamReader.resume()
            })

            const promise = new Promise((resolve, reject)=>{
                streamReader.on('end', _=>resolve())
                streamReader.on('error', err=>reject(err))
            })

            await promise

            debug("Taille fichier %s : %d", pathFichier, total)
        }

        // await writer.close()
        await verificateurHachage.verify()
        debug("Fichier %s transfere avec succes vers consignation sftp", fuuid)

        // Attendre que l'ecriture du fichier soit terminee (fs sync)
        // const handle = await open(pathFichier, 'r')
        let infoFichier = null
        try {
            debug("Attente sync du fichier %s", pathFichier)
            // if(_supporteSshExtensions) {
            //     try {
            //         const ok = await sync(handle)
            //         if(ok === false) {
            //             // Echec du sync, on utilise poll
            //             debug("Echec sync, on va faire du polling")
            //         }
            //     } catch(err) {
            //         debug("Erreur sync(3)", err)
            //     }
            // }

            debug("Execution fstat sur %s", fuuid)
            infoFichier = await fstat(writeHandle)
            let tailleStat = infoFichier.size, 
                compteur = 0,
                delai = 750
            debug("Stat fichier initial avant attente : %O", infoFichier)

            // while(tailleStat < total && compteur++ < 10) {
            //     debug("Attente fichier %s ms, boucle compteur: %d", delai, compteur)
            //     await new Promise((resolve, reject)=>{
            //         setTimeout(async ()=>{
            //             try {
            //                 infoFichier = await fstat(handle)
            //                 if(tailleStat === infoFichier.size) {
            //                     reject(`Fichier ${fuuid} bloque a taille ${tailleStat}`)
            //                 }
            //                 tailleStat = infoFichier.size
            //                 delai = delai * 1.5  // Augmenter delai attente
            //                 resolve()
            //             } catch(err) {
            //                 reject(err)
            //             }
            //         }, delai)
            //     })
            // }

            debug("Info fichier : %O", infoFichier)
        } finally {
            close(writeHandle)
        }

        if(infoFichier.size !== total) {
            const err = new Error(`Taille du fichier est differente sur sftp : ${infoFichier.size} != ${total}`)
            debug("Erreur taille fichier: %O", err)
            return reject(err)
        }

        debug("Information fichier sftp : %O", infoFichier)

    } catch(err) {
        try {
            await unlink(pathFichier)
        } catch(err) {
            console.error("Erreur unlink fichier sftp %s : %O", pathFichier, err)
        }
        throw err
    }

}

async function connecterSSH(host, port, username, opts) {
    opts = opts || {}
    host = host || _hostname
    port = port || _port
    username = username || _username
    const connexionName = username + '@' + host + ':' + port
  
    debug("Connecter SSH sur %s (opts: %O)", connexionName, opts)
  
    var privateKey = _privateEd25519Key
    const keyType = opts.keyType || _keyType || 'ed25519'
    if(keyType === 'rsa') {
        privateKey = _privateRsaKey
    }
  
    const conn = new Client()
    return new Promise((resolve, reject)=>{
        conn.on('ready', _=>{
            debug("Connexion ssh ready")
            _connexionSsh = conn
            conn.sftp((err, sftp)=>{
                if(err) return reject(err)
                _connexionSftp = sftp
                resolve(conn)
            })
        })
        conn.on('error', err=>{
            reject(err)
        })
        conn.on('end', _=>{
            debug("Connexion %s fermee, nettoyage pool", connexionName)
            _connexionSftp = null
            _connexionSsh = null
        })

        conn.connect({
            host, port, username, privateKey,
            readyTimeout: 60000,  // 60s, regle probleme sur login hostgator
            // debug,
        })
    })
}

async function marquerSupprime(fuuid) {
    const pathFichier = getPathFichier(fuuid)
    const pathFichierSupprime = pathFichier + '.corbeille'
    await rename(pathFichier, pathFichierSupprime)
}

async function parourirFichiers(callback, opts) {
    await parourirFichiersRecursif(_remotePath, callback, opts)
    await callback()  // Dernier appel avec aucune valeur (fin traitement)
}

async function parourirFichiersRecursif(repertoire, callback, opts) {
    opts = opts || {}
    debug("parourirFichiers %s", repertoire)
  
    const handleDir = await opendir(repertoire)
    try {
        let liste = await readdir(handleDir)
        while(liste) {
            // Filtrer la liste, conserver uniquement les fuuids (fichiers, enlever extension)
            // Appeler callback sur chaque item
            var infoFichiers = liste.filter(item=>{
                let isFile = item.attrs.isFile()
                if(opts.filtre) {
                    const data = { filename: item.filename, directory: repertoire, modified: item.attrs.mtime }
                    return isFile && opts.filtre(data)
                }
                return isFile
            })
            if(infoFichiers && infoFichiers.length > 0) {
                for(let fichier of infoFichiers) {
                    const data = { filename: fichier.filename, directory: repertoire, modified: fichier.attrs.mtime }
                    await callback(data)
                }
            }
        
            // Parcourir recursivement tous les repertoires
            const directories = liste.filter(item=>item.attrs.isDirectory())
            for await (const directory of directories) {
                const subDirectory = path.join(repertoire, directory.filename)
                await parourirFichiersRecursif(subDirectory, callback, opts)
            }

            try {
                liste = await readdir(handleDir)
            } catch (err) {
                liste = false
            }
        }
    } finally {
        close(handleDir)
    }
}

function opendir(pathRepertoire) {
    return new Promise(async (resolve, reject)=>{
        if(!_connexionSsh) await connecterSSH()

        _connexionSftp.opendir(pathRepertoire, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

function readdir(pathRepertoire) {
    return new Promise(async (resolve, reject)=>{
        if(!_connexionSsh) await connecterSSH()

        _connexionSftp.readdir(pathRepertoire, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

function rename(srcPath, destPath) {
    return new Promise(async (resolve, reject)=>{
        if(!_connexionSsh) await connecterSSH()

        _connexionSftp.rename(srcPath, destPath, err=>{
            if(err) return reject(err)
            resolve()
        })
    })
}

function stat(pathFichier) {
    return new Promise(async (resolve, reject)=>{
        if(!_connexionSsh) await connecterSSH()

        _connexionSftp.stat(pathFichier, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

function unlink(pathFichier) {
    return new Promise(async (resolve, reject)=>{
        if(!_connexionSsh) await connecterSSH()

        _connexionSftp.unlink(pathFichier, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

function open(pathFichier, flags, mode) {
    mode = mode || 0o644
    return new Promise(async (resolve, reject)=>{
        if(!_connexionSsh) await connecterSSH()

        _connexionSftp.open(pathFichier, flags, mode, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

function write(handle, buffer, offset, length, position) {
    return new Promise(async (resolve, reject)=>{
        if(!_connexionSsh) await connecterSSH()

        _connexionSftp.write(handle, buffer, offset, length, position, err=>{
            if(err) return reject(err)
            resolve()
        })
    })
}

function sync(handle) {
    if(!_supporteSshExtensions) return 
    return new Promise(async (resolve, reject)=>{
        if(!_connexionSsh) await connecterSSH()

        try {
            _connexionSftp.ext_openssh_fsync(handle, err=>{
                if(err) {
                    debug("Erreur SYNC (1), on assume manque de support de openssh extensions : %O", err)
                    _supporteSshExtensions = false  // toggle support extensions a false
                    // return reject(err)
                    resolve(false)
                }
                resolve(true)
            })
        } catch(err) {
            debug("Erreur SYNC (2), on assume manque de support de openssh extensions : %O", err)
            _supporteSshExtensions = false  // toggle support extensions a false
            resolve(false)
        }
    })
}

function fstat(handle) {
    return new Promise(async (resolve, reject)=>{
        if(!_connexionSsh) await connecterSSH()

        _connexionSftp.fstat(handle, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

function close(handle) {
    return new Promise(async (resolve, reject)=>{
        if(!_connexionSsh) await connecterSSH()

        _connexionSftp.close(handle, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

module.exports = {
    init, fermer,
    getInfoFichier, consignerFichier, marquerSupprime, recoverFichierSupprime,
    parourirFichiers,
}
