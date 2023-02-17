const debug = require('debug')('backupSftp')

const INTERVALLE_BACKUP = 8 * 3_600_000

function BackupSftp(mq, storeConsignation) {
    this.amqpdao = mq
    this.storeConsignation = storeConsignation

    this.queueItems = []
    this.timerBackup = null
    this.backupEnCours = false
}

BackupSftp.prototype.threadBackup = async function() {
    try {
        debug("Run threadBackup")
        if(this.timerBackup) clearTimeout(this.timerBackup)
        this.timerBackup = null
        this.backupEnCours = new Date()

        const configuration = await this.storeConsignation.chargerConfiguration()
        if(configuration.type_backup === 'sftp') {
            // Run backup
            await this.runBackup(configuration)
        } else {
            debug("Type backup !== sftp, pas de backup")
        }

    } catch(err) {
        console.error(new Date() + ' TransfertPrimaire.threadBackup Erreur execution cycle : %O', err)
    } finally {
        this.backupEnCours = false
        debug("threadBackup Fin execution cycle, attente %s ms", INTERVALLE_BACKUP)
        // Redemarrer apres intervalle
        this.timerBackup = setTimeout(()=>{
            this.timerBackup = null
            this.threadBackup()
                .catch(err=>console.error(new Date() + " BackupSftp Erreur run threadBackup: %O", err))
        }, INTERVALLE_BACKUP)

    }

}

BackupSftp.prototype.runBackup = async function() {
    debug("runBackup Debut")

    debug("runBackup Fin backup")
}

module.exports = BackupSftp
