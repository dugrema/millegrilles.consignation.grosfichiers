#!/usr/bin/env bash

CERT_FOLDER=/home/mathieu/mgdev/certs

export HOST=`uname -n`

export MG_MQ_CAFILE=$CERT_FOLDER/pki.millegrille.cert
export MG_MQ_CERTFILE=$CERT_FOLDER/pki.fichiers.cert
export MG_MQ_KEYFILE=$CERT_FOLDER/pki.fichiers.key
export SFTP_ED25519_KEY=$CERT_FOLDER/pki.fichiers.sftp.ed25519
export SFTP_RSA_KEY=$CERT_FOLDER/pki.fichiers.sftp.rsa
export WEB_CERT=$MG_MQ_CERTFILE
export WEB_KEY=$MG_MQ_KEYFILE
export MG_MQ_URL=amqps://$HOST:5673
export IPFS_HOST=http://192.168.2.131:5001

# Path ou les apps web (Vitrine, Place) sont copiees
export WEBAPPS_SRC_FOLDER=/var/opt/millegrilles/nginx/html

export PORT=3021

# export DEBUG=millegrilles:fichiers:transformationsVideo,millegrilles:fichiers:cryptoUtils,millegrilles:fichiers:uploadFichier
# export DEBUG=millegrilles:messages:*,millegrilles:fichiers:traitementMedia
# export DEBUG=millegrilles:fichiers:transformationsVideo,millegrilles:messages:*,millegrilles:fichiers:traitementMedia
export DEBUG=millegrilles:fichiers:publication,millegrilles:fichiers:ssh,millegrilles:fichiers:ipfs,millegrilles:fichiers:awss3,\
millegrilles:fichiers:publier

# export SERVER_TYPE=https

# Desactiver validation usager locale
# export IDMG=z2xMUPJHXDgkLEgdziA21EuA4BCvtWtKLuSnyqVJyvexgxj8yPsMRW
# export DISABLE_SSL_AUTH=1

npm start
