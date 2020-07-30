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

export DEBUG=millegrilles.*

# export SERVER_TYPE=https

export IDMG=JjP8L3xKtE1yAyQGMU5TSNmVjBzxu5wTGpU23CMHbXUT
# export DISABLE_SSL_AUTH=1

npm start
