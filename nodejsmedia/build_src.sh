#!/bin/bash
set -e

# Faire lien vers package.json de consignationfichiers
ln -sf ../package.json

# Nettoyager package existants
rm -rf node_modules

# Installer dependances
npm i --production