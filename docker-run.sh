#!/bin/bash

source image_info.txt
ARCH=`uname -m`

# Override version (e.g. pour utiliser x86_64_...)
# VERSION=x86_64_1.29.3
IMAGE_DOCKER=$REPO/${NAME}:${ARCH}_${VERSION}

echo Image docker : $IMAGE_DOCKER

# MQ
export HOST=mg-dev4.maple.maceroc.com
export HOST_MQ=mg-dev4

CERT_FOLDER=/home/mathieu/mgdev/certs
export MG_MQ_CAFILE=/certs/pki.millegrille.cert
export MG_MQ_CERTFILE=/certs/pki.fichiers.cert
export MG_MQ_KEYFILE=/certs/pki.fichiers.key
export MG_MQ_URL=amqps://$HOST_MQ:5673
export PORT=3021

# export DEBUG=millegrilles:fichiers:transformationsVideo,millegrilles:messages:*,millegrilles:fichiers:traitementMedia
export DEBUG=millegrilles:fichiers:*

docker run --rm -it \
  --network host \
  -v /home/mathieu/mgdev/certs:/certs \
  -v /var/opt/millegrilles/consignation/grosfichiers:/var/opt/millegrilles/consignation/grosfichiers \
  -e MG_MQ_CAFILE -e MG_MQ_CERTFILE -e MG_MQ_KEYFILE \
  -e MG_MQ_URL -e HOST -e PORT \
  -e DEBUG \
  $IMAGE_DOCKER
