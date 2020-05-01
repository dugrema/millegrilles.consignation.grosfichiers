#!/usr/bin/env bash

CERT_FOLDER=/home/mathieu/mgdev/certs

export MG_MQ_CAFILE=$CERT_FOLDER/pki.millegrille.cert
export HEB_CERTFILE=$CERT_FOLDER/pki.heb_fichiers.cert
export HEB_KEYFILE=$CERT_FOLDER/pki.heb_fichiers.key
export WEB_CERT=$HEB_CERTFILE
export WEB_KEY=$HEB_KEYFILE
export MG_MQ_URL=amqps://mg-dev3:5673

export PORT=3003

export TRANSMISSION_PASSWORD='bwahahah1202'

npm run-script hebergement
