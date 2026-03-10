FROM oven/bun:1-alpine
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production
COPY src/ ./src/
VOLUME ["/data"]
EXPOSE 3000
ENTRYPOINT ["bun", "src/index.ts"]
