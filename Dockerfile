FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY server.js ./
COPY public ./public

RUN mkdir -p /app/data
VOLUME ["/app/data"]

ENV PORT=3000
EXPOSE 3000

CMD ["node", "server.js"]
