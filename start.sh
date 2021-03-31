#!/usr/bin/env bash

CERT_FOLDER=/home/mathieu/mgdev/certs

export HOST=`uname -n`

export MG_MQ_CAFILE=$CERT_FOLDER/pki.millegrille.cert
export MG_MQ_CERTFILE=$CERT_FOLDER/pki.fichiers.cert
export MG_MQ_KEYFILE=$CERT_FOLDER/pki.fichiers.key
export WEB_CERT=$MG_MQ_CERTFILE
export WEB_KEY=$MG_MQ_KEYFILE
export MG_MQ_URL=amqps://$HOST:5673

export PORT=3021

export DEBUG=millegrilles:fichiers:transformationsVideo,millegrilles:fichiers:cryptoUtils

# export SERVER_TYPE=https

# Desactiver validation usager locale
export IDMG=z2xMUPJHXDgkLEgdziA21EuA4BCvtWtKLuSnyqVJyvexgxj8yPsMRW
export DISABLE_SSL_AUTH=1

npm start
