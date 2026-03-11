FROM oven/bun:1-alpine
WORKDIR /app
RUN apk add --no-cache --update ca-certificates git tzdata
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production --ignore-scripts
COPY src/ ./src/
VOLUME ["/data"]
EXPOSE 3000
ENTRYPOINT ["bun", "src/index.ts"]
