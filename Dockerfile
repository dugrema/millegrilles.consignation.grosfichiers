FROM node:16

# Create app directory
WORKDIR /usr/src/app

# Volume pour le staging des fichiers uploades via coupdoeil
VOLUME /var/opt/millegrilles

ENV PORT=443 \
    HOST=fichiers \
    NODE_ENV=production \
    SERVER_TYPE=https \
    MG_MQ_URL=amqps://mq:5673 \
    MG_REDIS_HOST=redis \
    MG_REDIS_PORT=6379 \
    MG_MQ_CERTFILE=/run/secrets/cert.pem \
    MG_MQ_KEYFILE=/run/secrets/key.pem \
    MG_MQ_CAFILE=/run/secrets/millegrille.cert.pem \
    WEB_CERT=/run/secrets/cert.pem \
    WEB_KEY=/run/secrets/key.pem \
    SFTP_KEY=/run/secrets/sftp.key.pem \
    MG_CONSIGNATION_PATH=/var/opt/millegrilles/consignation \
    WEBAPPS_SRC_FOLDER=/var/opt/millegrilles/nginx/html 

EXPOSE 443

COPY ./ ./

RUN npm i --production && \
    rm -rf /root/.npm

CMD [ "npm", "start" ]
