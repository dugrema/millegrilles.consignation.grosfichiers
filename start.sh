#!/usr/bin/env bash

CERT_FOLDER=/home/mathieu/mgdev/certs
export MG_IDMG=vPXTaPjpUErFjV5d8pKrAHHqKhFUr7GSEruCL7
export MG_CONSIGNATION_PATH=/var/opt/millegrilles/$MG_IDMG/mounts/consignation

# export COUPDOEIL_SESSION_TIMEOUT=15000
export MG_MQ_CAFILE=$CERT_FOLDER/pki.millegrille.cert
export MG_MQ_CERTFILE=$CERT_FOLDER/pki.fichiers.cert
export MG_MQ_KEYFILE=$CERT_FOLDER/pki.fichiers.key
export CERT=$MG_MQ_CERTFILE
export PRIVKEY=$MG_MQ_KEYFILE
export MG_MQ_URL=amqps://mg-dev3:5673/$MG_IDMG

export PORT=3003

export MG_HTTPPROXY_SECURE=false

export TRANSMISSION_PASSWORD='bwahahah1202'

npm start
