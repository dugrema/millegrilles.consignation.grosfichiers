#!/usr/bin/env bash

CERT_FOLDER=/home/mathieu/mgdev/certs
export MG_NOM_MILLEGRILLE=dev3

# export COUPDOEIL_SESSION_TIMEOUT=15000
export MG_MQ_CAFILE=$CERT_FOLDER/pki.ca.root.cert
export MG_MQ_CERTFILE=$CERT_FOLDER/pki.fichiers.fullchain
export MG_MQ_KEYFILE=$CERT_FOLDER/pki.fichiers.key
export CERT=$MG_MQ_CERTFILE
export PRIVKEY=$MG_MQ_KEYFILE
export MG_MQ_URL=amqps://mg-dev3.local:5673/$MG_NOM_MILLEGRILLE

export PORT=3003

export MG_HTTPPROXY_SECURE=false

export TRANSMISSION_PASSWORD='bwahahah1202'

npm start
