const fs = require('fs')
const fsPromises = require('fs/promises')
const readline = require('readline')
const { exec } = require('child_process')
const zlib = require('zlib')

async function chargerFuuidsListe(pathFichier, cb) {
    const readStreamFichiers = fs.createReadStream(pathFichier)
    const rlFichiers = readline.createInterface({input: readStreamFichiers, crlfDelay: Infinity})
    let count = 0
    for await (let fuuid of rlFichiers) {
        // Detecter fichiers manquants localement par espaces vide au debut de la ligne
        fuuid = fuuid.trim()
        if(!fuuid) continue  // Ligne vide
        count ++
        try {
            await cb(fuuid)
        } catch(err) {
            if(err.overflow === true) return  // SKIP le reste, overflow Q
            else throw err
        }
    }
    return count
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
    // Trier fichiers
    const src1Sorted = src1 + '.sorted',
          src2Sorted = src2 + '.sorted'
    // Faire la liste des fuuids inconnus (reclames mais pas dans actifs / archives)
    try {
        await sortFile(src1, src1Sorted)
        await sortFile(src2, src2Sorted)

        await new Promise((resolve, reject)=>{
            exec(`comm -13 ${src1Sorted} ${src2Sorted} > ${dest} && gzip -9fk ${dest}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })
    } finally {
        fsPromises.unlink(src1Sorted).catch(err=>console.info("trouverManquants Erreur supprimer src1.sorted (%s) : ", src1Sorted, err))
        fsPromises.unlink(src2Sorted).catch(err=>console.info("trouverManquants Erreur supprimer src2.sorted (%s) : ", src2Sorted, err))
    }
}

/** Conserve les fichiers qui sont uniquement presents dans src1 */
async function trouverUniques(src1, src2, dest) {
    // Trier fichiers
    const src1Sorted = src1 + '.sorted',
          src2Sorted = src2 + '.sorted'
    try {
        await sortFile(src1, src1Sorted)
        await sortFile(src2, src2Sorted)

        await new Promise((resolve, reject)=>{
            exec(`comm -23 ${src1Sorted} ${src2Sorted} > ${dest}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })
    } finally {
        fsPromises.unlink(src1Sorted).catch(err=>console.info("trouverUniques Erreur supprimer src1.sorted (%s) : ", src1Sorted, err))
        fsPromises.unlink(src2Sorted).catch(err=>console.info("trouverUniques Erreur supprimer src2.sorted (%s) : ", src2Sorted, err))
    }
}

/** Conserve les fichiers qui sont uniquement presents dans src1 */
async function trouverPresentsTous(src1, src2, dest) {
    // Trier fichiers
    const src1Sorted = src1 + '.sorted',
          src2Sorted = src2 + '.sorted'
    try {
        await sortFile(src1, src1Sorted)
        await sortFile(src2, src2Sorted)
  
        await new Promise((resolve, reject)=>{
            exec(`comm -12 ${src1Sorted} ${src2Sorted} > ${dest}`, error=>{
                if(error) return reject(error)
                else resolve()
            })
        })
    } finally {
        fsPromises.unlink(src1Sorted).catch(err=>console.info("trouverPresentsTous Erreur supprimer src1.sorted (%s) : ", src1Sorted, err))
        fsPromises.unlink(src2Sorted).catch(err=>console.info("trouverPresentsTous Erreur supprimer src2.sorted (%s) : ", src2Sorted, err))
    }
}

async function gzipFile(src, dest) {
    await new Promise((resolve, reject)=>{
        const readStream = fs.createReadStream(src)
        readStream.on('error', reject)

        const writeStream = fs.createWriteStream(dest)
        writeStream.on('error', reject)
        writeStream.on('close', resolve)

        const gzip = zlib.createGzip()
        gzip.pipe(writeStream)
        readStream.pipe(gzip)
    })
}

module.exports = { chargerFuuidsListe, sortFile, combinerSortFiles, trouverManquants, trouverUniques, trouverPresentsTous, gzipFile }
