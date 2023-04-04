const debugLib = require('debug')
const debug = debugLib('sftpDao')
const debugSftp = debugLib('sftpDaoTrace')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const {Client} = require('ssh2')

// Creer un pool de connexions a reutiliser
const CONNEXION_TIMEOUT = 10 * 60 * 1000  // 10 minutes
const CHUNK_SIZE = 32768 - 29  // Packet ssh est 32768 sur hostgator, 29 bytes overhead

function SftpDao() {
    this.intervalEntretienConnexion = null
    this.connexionSsh = null
    this.channelSftp = null
    this.supporteSshExtensions = true
    this.connexionError = null

    this.hostname = null
    this.port = 22
    this.username = null
    this.keyType = 'ed25519'
}

SftpDao.prototype.init = async function(params) {
    params = params || {}
    const {hostname_sftp, username_sftp, port_sftp, key_type_sftp} = params
    if(!hostname_sftp) throw new Error("Parametre hostname manquant")
    if(!username_sftp) throw new Error("Parametre username manquant")

    this.hostname = hostname_sftp
    this.username = username_sftp
    this.keyType = key_type_sftp || 'ed25519'
    
    this.port = port_sftp || this.port

    if(!this.intervalEntretienConnexion) {
        this.intervalEntretienConnexion = setInterval(this.entretienConnexion, CONNEXION_TIMEOUT/2)
    }

    // Charger les cles privees utilisees pour se connecter par sftp
    // Ed25519 est prefere, RSA comme fallback
    const privateEd25519KeyPath = process.env.SFTP_ED25519_KEY || '/run/secrets/sftp.ed25519.key.pem'
    this.privateEd25519Key = await fsPromises.readFile(privateEd25519KeyPath)
    const privateRsaKeyPath = process.env.SFTP_RSA_KEY || '/run/secrets/sftp.rsa.key.pem'
    this.privateRsaKey = await fsPromises.readFile(privateRsaKeyPath)    

    if(this.channelSftp) {
        await this.fermer()
    }

    debug("Init, connecter ssh sur %s@%s:%d", this.username, this.hostname, this.port)
    return this.connecterSSH(this.hostname, this.port, this.username, params)
}

SftpDao.prototype.fermer = function () {
    if(this.intervalEntretienConnexion) clearInterval(this.intervalEntretienConnexion)
    return this.entretienConnexion({closeAll: true})
        .catch(err=>console.error("ERROR %O Erreur fermeture connexion sftp : %O", new Date(), err))
}

SftpDao.prototype.entretienConnexion = async function(opts) {
    opts = opts || {}
    if(opts.closeAll) {
        debug("entretientConnexion closeAll")
        if(this.connexionSsh) this.connexionSsh.end()
    }
}

SftpDao.prototype.putFile = async function(fuuid, entry, writeHandle, callback) {
    const {position, fullPath} = entry

    const sftp = await this.sftpClient()

    // debug("Entry path : %O", entry);
    debug("Traiter consignation pour item %s position %d", fuuid, position)
    const streamReader = fs.createReadStream(fullPath, {highWaterMark: CHUNK_SIZE})

    const resultat = await this.writeStream(streamReader, writeHandle, callback, {position})

    debug("Bytes fichier %s ecits : %d", fuuid, resultat.total - position)
    return resultat
}

SftpDao.prototype.createReadStream = async function(pathFichier) {
    const sftp = await this.sftpClient()
    return sftp.createReadStream(pathFichier)
}

SftpDao.prototype.writeStream = async function(streamReader, writeHandle, callback, opts) {
    opts = opts || {}
    let filePosition = opts.position || 0

    const sftp = await this.sftpClient()

    // debug("Entry path : %O", entry);
    debug("putStream position %d", filePosition)

    const promise = new Promise((resolve, reject)=>{
        let termine = false,
            ecritureEnCours = false
        streamReader.on('end', ()=>{
            termine = true
            if(ecritureEnCours) {
                debug("end appele, ecriture en cours : %s", ecritureEnCours)
            } else {
                resolve({total: filePosition})
            }
        })
        streamReader.on('error', err=>reject(err))
        streamReader.on('data', async chunk => {
            ecritureEnCours = true
            if(termine) return reject("Stream / promise resolved avant fin ecriture")

            // Verifier hachage
            streamReader.pause()
            // verificateurHachage.update(chunk)

            // Ecrire le contenu du chunk
            debugSftp("Ecrire position %d (offset %d, len: %d)", filePosition, 0, chunk.length)
            try {
                // await new Promise(resolve=>setTimeout(resolve, 20))  // Throttle, aide pour hostgator
                await this.write(writeHandle, chunk, 0, chunk.length, filePosition)
                if(callback) await callback(chunk, filePosition)
            } catch(err) {
                streamReader.close()
                return reject(err)
            }

            // Incrementer position globale
            filePosition += chunk.length

            debugSftp("Ecriture rendu position %d (offset %d)", filePosition)

            ecritureEnCours = false
            if(termine) return resolve({total: filePosition})

            streamReader.resume()
        })
    })

    const resultat = await promise

    debug("Resultat ecriture stream ", resultat)

    return resultat
}

SftpDao.prototype.writeFileStream = async function(pathFichier, streamReader) {
    var writeHandle = await this.open(pathFichier, 'w', 0o644)
    try {
        await this.writeStream(streamReader, writeHandle)
    } finally {
        await this.close(writeHandle)
    }
}

SftpDao.prototype.connecterSSH = async function(host, port, username, opts) {
    opts = opts || {}
    host = host || this.hostname
    port = port || this.port
    readyTimeout = opts.readyTimeout || 15_000
    username = username || this.username
    const connexionName = username + '@' + host + ':' + port
  
    debug("Connecter SSH sur %s (opts: %O)", connexionName, opts)
  
    // await new Promise(resolve=>setTimeout(resolve, 3000))  // Delai connexion

    var privateKey = this.privateEd25519Key
    const keyType = opts.keyType || this.keyType || 'ed25519'
    if(keyType === 'rsa') {
        privateKey = this.privateRsaKey
    }
  
    const conn = new Client()

    this.connexionError = null
    return new Promise((resolve, reject)=>{
        conn.on('ready', async () => {
            debug("Connexion ssh ready")
            this.connexionSsh = conn
            try {
                this.channelSftp = await this.sftpClient()
                resolve(conn)
            } catch(err) {
                debug("Erreur ouverture sftp : %O", err)
                reject(err)
            }
        })

        conn.on('error', err=>{
            console.error(new Date() + " sftpDao.connecterSSH Erreur connexion SFTP : %O", err)
            this.connexionError = err
            this.channelSftp = null
            this.connexionSsh = null
            reject(err)
        })

        conn.on('end', ()=>{
            debug("connecterSSH.end Connexion %s fermee, nettoyage pool", connexionName)
            this.channelSftp = null
            this.connexionSsh = null
        })

        conn.connect({
            host, port, username, privateKey,
            readyTimeout,
            debug: debugSftp,
        })
    })
}

SftpDao.prototype.sftpClient = async function() {
    if(this.channelSftp) return this.channelSftp

    if(!this.connexionSsh) await connecterSSH()
    return new Promise(async (resolve, reject)=>{
        try {
            debug("Ouverture sftp")
            this.connexionSsh.sftp((err, sftp)=>{
                debug("Sftp ready, err?: %O", err)
                if(err) return reject(err)

                sftp.on('end', ()=>{
                    debug("Channel SFTP end")
                    this.channelSftp = null
                })

                sftp.on('error', err=>{
                    console.error(new Date() + " sftpDao.sftpClient ERROR Channel SFTP : %O", err)
                })

                resolve(sftp)
            })
        } catch(err) {
            reject(err)
        }
    })
}

SftpDao.prototype.parcourirFichiersRecursif = async function(repertoire, callback, opts) {
    opts = opts || {}
    debug("parcourirFichiers %s", repertoire)

    const sftp = await this.sftpClient()

    const handleDir = await this.opendir(repertoire)

    if(opts.direntry === true) {
        callback({directory: repertoire})
    }

    try {
        let liste = await this.readdir(handleDir)
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
                    // debug("parcourirFichiersRecursif Fichier ", fichier)
                    const data = { 
                        filename: fichier.filename, 
                        directory: repertoire, 
                        modified: fichier.attrs.mtime, 
                        size: fichier.attrs.size 
                    }
                    await callback(data)
                }
            }
        
            // Parcourir recursivement tous les repertoires
            const directories = liste.filter(item=>item.attrs.isDirectory())
            for await (const directory of directories) {
                const subDirectory = path.join(repertoire, directory.filename)
                await this.parcourirFichiersRecursif(subDirectory, callback, opts)
            }

            try {
                liste = await this.readdir(handleDir)
            } catch (err) {
                liste = false
            }
        }

    } finally {
        this.close(handleDir)
    }
}

SftpDao.prototype.mkdir = async function(pathRepertoire) {
    
    const sftp = await this.sftpClient()

    return new Promise(async (resolve, reject)=>{
        debug("mkdir ", pathRepertoire)
        sftp.mkdir(pathRepertoire, (err, info)=>{
            if(err) {
                if(err.code === 4) {
                    // Ok, repertoire existe deja
                } else {
                    return reject(err)
                }
            }
            resolve(info)
        })
    })
}

SftpDao.prototype.rmdir = async function(pathRepertoire, opts) {
    opts = opts || {}
    debug("rmdir ", pathRepertoire)

    const sftp = await this.sftpClient()

    const liste = []

    const callback = async info => {
        liste.push(info)
    }

    await this.parcourirFichiersRecursif(pathRepertoire, callback, {direntry: true})

    // Inverser la liste, supprimer sous-repertoires en premier
    liste.reverse()
    
    for await (let info of liste) {
        if(info.filename) {
            const nomFichier = path.join(info.directory, info.filename)
            await this.unlink(nomFichier)
        } else {
            // directory, suppression
            debug("Supprimer repertoire ", info.directory)
            await new Promise((resolve, reject)=>{
                sftp.rmdir(info.directory, (err, info)=>{
                    if(err) {
                        if(err.code === 2) {
                            // OK, n'existe pas
                        } else {
                            return reject(err)
                        }
                    }
                    resolve(info)
                })
            })
        }
    }

}

SftpDao.prototype.opendir = async function(pathRepertoire) {
    const sftp = await this.sftpClient()

    return new Promise(async (resolve, reject)=>{
        sftp.opendir(pathRepertoire, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

SftpDao.prototype.readdir = async function(pathRepertoire) {
    const sftp = await this.sftpClient()

    return new Promise(async (resolve, reject)=>{
        sftp.readdir(pathRepertoire, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

SftpDao.prototype.rename = async function(srcPath, destPath) {
    const sftp = await this.sftpClient()

    return new Promise(async (resolve, reject)=>{
        sftp.rename(srcPath, destPath, err=>{
            if(err) return reject(err)
            resolve()
        })
    })
}

SftpDao.prototype.touch = async function(srcPath) {
    const sftp = await this.sftpClient()

    // const stat = await this.stat(srcPath)
    // console.debug("!!! STAT ", stat)

    const newDate = new Date()
    return new Promise(async (resolve, reject)=>{
        sftp.utimes(srcPath, newDate, newDate, err=>{
            if(err) return reject(err)
            resolve()
        })
    })
}

SftpDao.prototype.stat = async function(pathFichier) {
    const sftp = await this.sftpClient()

    return new Promise(async (resolve, reject)=>{
        sftp.stat(pathFichier, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

SftpDao.prototype.unlink = async function(pathFichier) {
    const sftp = await this.sftpClient()

    return new Promise(async (resolve, reject)=>{
        debug("unlink fichier ", pathFichier)
        sftp.unlink(pathFichier, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

SftpDao.prototype.open = async function(pathFichier, flags, mode) {
    mode = mode || 0o644
    const sftp = await this.sftpClient()

    return new Promise(async (resolve, reject)=>{
        sftp.open(pathFichier, flags, mode, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

SftpDao.prototype.write = async function(handle, buffer, offset, length, position) {
    const sftp = await this.sftpClient()

    return new Promise((resolve, reject)=>{
        sftp.write(handle, buffer, offset, length, position, err=>{
            if(err) return reject(err)
            resolve()
        })
    })
}

SftpDao.prototype.sync = async function(handle) {
    if(!this.supporteSshExtensions) return 
    const sftp = await this.sftpClient()

    return new Promise(async (resolve, reject)=>{
        try {
            sftp.ext_openssh_fsync(handle, err=>{
                if(err) {
                    debug("Erreur SYNC (1), on assume manque de support de openssh extensions : %O", err)
                    this.supporteSshExtensions = false  // toggle support extensions a false
                    // return reject(err)
                    resolve(false)
                }
                resolve(true)
            })
        } catch(err) {
            debug("Erreur SYNC (2), on assume manque de support de openssh extensions : %O", err)
            this.supporteSshExtensions = false  // toggle support extensions a false
            resolve(false)
        }
    })
}

SftpDao.prototype.fstat = async function(handle) {
    const sftp = await this.sftpClient()

    return new Promise(async (resolve, reject)=>{
        sftp.fstat(handle, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

SftpDao.prototype.close = async function(handle) {
    const sftp = await this.sftpClient()

    return new Promise(async (resolve, reject)=>{
        sftp.close(handle, (err, info)=>{
            if(err) return reject(err)
            resolve(info)
        })
    })
}

module.exports = SftpDao
