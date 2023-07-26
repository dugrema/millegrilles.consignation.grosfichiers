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

/** Trouve les fichiers qui sont manquants dans src2 compare a src1 */
async function trouverManquants(src1, src2, dest) {
    try {
        // Faire la liste des fuuids inconnus (reclames mais pas dans actifs / archives)
        await new Promise((resolve, reject)=>{
            exec(`comm -13 ${src1} ${src2} > ${dest} && gzip -9fk ${dest}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })
    } catch(err) {
        console.error(new Date() + " trouverManquants ERROR Traitement fichiers manquants : ", err)
        return
    }
}

/** Conserve les fichiers qui sont uniquement presents dans src1 */
async function trouverUniques(src1, src2, dest) {
    try {
        await new Promise((resolve, reject)=>{
            exec(`comm -23 ${src1} ${src2} > ${dest}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })
    } catch(err) {
        console.error(new Date() + " trouverManquants ERROR Traitement fichiers manquants : ", err)
        return
    }
}

module.exports = { chargerFuuidsListe, sortFile, combinerSortFiles, trouverManquants, trouverUniques}
