FROM node:16

# Create app directory
WORKDIR /usr/src/app

# Volume pour le staging des fichiers uploades via coupdoeil
VOLUME /var/opt/millegrilles

ENV NODE_ENV=production

EXPOSE 443

COPY ./package*.json ./

# Installer ffmpeg, imagemagick pour conversion video et images
RUN npm i --production && \
    apt update && \
    apt install ffmpeg imagemagick-6.q16 rsync -y && \
    apt clean && \
    rm -rf /var/cache/apt

# COPY node_modules/ node_modules/

CMD [ "npm", "start" ]
