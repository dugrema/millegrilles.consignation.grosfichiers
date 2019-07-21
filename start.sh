#!/usr/bin/env bash

CERT_FOLDER=/home/mathieu/certificates

# export COUPDOEIL_SESSION_TIMEOUT=15000
export MG_MQ_CAFILE=$CERT_FOLDER/millegrilles/pki.millegrilles.ssl.CAchain
export MG_MQ_CERTFILE=$CERT_FOLDER/millegrilles/certs/dev2.maple.mdugre.info.cert.pem
export MG_MQ_KEYFILE=$CERT_FOLDER/millegrilles/privkeys/dev2.maple.mdugre.info.pem
export CERT=$CERT_FOLDER/dev2.maple.mdugre.info/fullchain.pem
export PRIVKEY=$CERT_FOLDER/dev2.maple.mdugre.info/privkey.pem
export MG_MQ_URL=amqps://dev2:5673/dev2
export PORT=3003

export MG_HTTPPROXY_SECURE=false

npm start

