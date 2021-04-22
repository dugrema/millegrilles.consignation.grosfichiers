#!/usr/bin/env bash

CERT_FOLDER=/home/mathieu/mgdev/certs

export HOST=`uname -n`

export MG_MQ_CAFILE=$CERT_FOLDER/pki.millegrille.cert
export MG_MQ_CERTFILE=$CERT_FOLDER/pki.fichiers.cert
export MG_MQ_KEYFILE=$CERT_FOLDER/pki.fichiers.key
export SFTP_KEY=$CERT_FOLDER/pki.fichiers.sftp
export WEB_CERT=$MG_MQ_CERTFILE
export WEB_KEY=$MG_MQ_KEYFILE
export MG_MQ_URL=amqps://$HOST:5673

export PORT=3021

# export DEBUG=millegrilles:fichiers:transformationsVideo,millegrilles:fichiers:cryptoUtils,millegrilles:fichiers:uploadFichier
# export DEBUG=millegrilles:messages:*,millegrilles:fichiers:traitementMedia
# export DEBUG=millegrilles:fichiers:transformationsVideo,millegrilles:messages:*,millegrilles:fichiers:traitementMedia
export DEBUG=millegrilles:fichiers:publication,millegrilles:fichiers:ssh,millegrilles:fichiers:ipfs

# export SERVER_TYPE=https

# Desactiver validation usager locale
# export IDMG=z2xMUPJHXDgkLEgdziA21EuA4BCvtWtKLuSnyqVJyvexgxj8yPsMRW
# export DISABLE_SSL_AUTH=1

npm start
