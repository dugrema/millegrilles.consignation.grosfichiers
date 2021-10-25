FROM node:16

# Create app directory
WORKDIR /usr/src/app

# Volume pour le staging des fichiers uploades via coupdoeil
VOLUME /opt/millegrilles/consignation

ENV PORT=443 \
    HOST=fichiers \
    NODE_ENV=production

COPY ./package*.json ./

# Bundle app source
# Api est l'application node back-end et front-end est l'application react
COPY ./ ./

RUN npm i --production && \
    rm -rf /root/.npm
