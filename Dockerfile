FROM docker.maceroc.com/nodejsmedia:12_0

# Create app directory
WORKDIR /usr/src/app

# Volume pour le staging des fichiers uploades via coupdoeil
VOLUME /opt/millegrilles/consignation

ENV PORT=443 \
    HOST=fichiers

COPY ./package*.json ./

RUN npm install

# Bundle app source
# Api est l'application node back-end et front-end est l'application react
COPY ./ ./
