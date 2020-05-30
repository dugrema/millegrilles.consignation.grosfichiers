FROM node:12

# Create app directory
WORKDIR /usr/src/app

# Volume pour le staging des fichiers uploades via coupdoeil
VOLUME /opt/millegrilles/consignation

ENV NODE_ENV=production

EXPOSE 443

CMD [ "npm", "start" ]

# Installer ffmpeg, imagemagick pour conversion video et images
RUN apt update && \
    apt install ffmpeg imagemagick-6.q16 -y && \
    apt clean && \
    rm -rf /var/cache/apt
