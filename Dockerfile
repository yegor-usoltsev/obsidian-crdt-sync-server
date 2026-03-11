FROM oven/bun:1.3.10-alpine

WORKDIR /app

RUN apk add --no-cache --update ca-certificates git tzdata

COPY package.json bun.lock .
RUN bun install --frozen-lockfile --production --ignore-scripts

COPY src src

EXPOSE 3000

ENV DATA_DIR=/data/db
ENV BACKUP_GIT_WORKTREE_DIR=/data/git
VOLUME ["/data"]

ENTRYPOINT ["bun", "start"]
