FROM oven/bun:1.3.10-alpine

WORKDIR /app

RUN apk add --no-cache --update ca-certificates git tzdata

COPY package.json bun.lock .
RUN bun install --frozen-lockfile --production --ignore-scripts

COPY src src

EXPOSE 3000

ENV DATA_DIR=/mnt
VOLUME ["/mnt"]

ENTRYPOINT ["bun", "start"]
