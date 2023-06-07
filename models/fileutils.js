const fs = require('fs')
const readline = require('readline')
const { exec } = require('child_process')

async function chargerFuuidsListe(pathFichier, cb) {
    const readStreamFichiers = fs.createReadStream(pathFichier)
    const rlFichiers = readline.createInterface({input: readStreamFichiers, crlfDelay: Infinity})
    for await (let fuuid of rlFichiers) {
        // Detecter fichiers manquants localement par espaces vide au debut de la ligne
        fuuid = fuuid.trim()
        if(!fuuid) continue  // Ligne vide
        try {
            await cb(fuuid)
        } catch(err) {
            if(err.overflow === true) return  // SKIP le reste, overflow Q
            else throw err
        }
    }
}

async function sortFile(src, dest, opts) {
    opts = opts || {}
    const gzip = opts.gzip || false

    let command = null
    if(src.endsWith('.gz') || opts.gzipsrc ) {
        command = `zcat ${src} | sort -u -o ${dest}`
    } else {
        command = `sort -u -o ${dest} ${src}`
    }
    if(gzip) command += ` && gzip -9fk ${dest}`

    await new Promise((resolve, reject)=>{
        exec(command, error=>{
            if(error) return reject(error)
            else resolve()
        })
    })
}

async function combinerSortFiles(srcList, dest, opts) {
    opts = opts || {}
    const gzip = opts.gzip || false,
          gzipsrc = opts.gzipsrc || false

    let command = null
    if(gzipsrc) {
        command = `zcat ${srcList.join(' ')} | sort -u -o ${dest}`
    } else {
        command = `cat ${srcList.join(' ')} | sort -u -o ${dest}`
    }
    if(gzip) command += ` && gzip -9fk ${dest}`

    await new Promise((resolve, reject)=>{
        exec(command, error=>{
            if(error) return reject(error)
            else resolve()
        })
    })
}

module.exports = { chargerFuuidsListe, sortFile, combinerSortFiles }