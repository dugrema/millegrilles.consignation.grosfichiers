#!/bin/bash
set -e

source image_info.txt

echo "Nom build : $NAME"

echo "S'assurer que toutes les dependances sont presentes"
rm -rf node_modules/millegrilles.common
npm i --production
