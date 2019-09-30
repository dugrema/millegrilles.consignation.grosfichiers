#!/usr/bin/env bash

CERT_FOLDER=/opt/millegrilles/test1/pki/deployeur
export MG_NOM_MILLEGRILLE=test1

# export COUPDOEIL_SESSION_TIMEOUT=15000
export MG_MQ_CAFILE=$CERT_FOLDER/pki.ca.fullchain.pem
export MG_MQ_CERTFILE=$CERT_FOLDER/deployeur.cert.pem
export MG_MQ_KEYFILE=$CERT_FOLDER/deployeur.key.pem
export CERT=$MG_MQ_CERTFILE
export PRIVKEY=$MG_MQ_KEYFILE
export MG_MQ_URL=amqps://mg-dev3.local:5673/$MG_NOM_MILLEGRILLE

export PORT=3003

export MG_HTTPPROXY_SECURE=false

npm start
