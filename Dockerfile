FROM oven/bun:1.3.12-alpine

WORKDIR /app

RUN apk add --no-cache --update ca-certificates git tini tzdata

COPY main.js .

EXPOSE 3000

ENV DATA_DIR=/data/db
ENV BACKUP_GIT_WORKTREE_DIR=/data/git
VOLUME ["/data"]

ENTRYPOINT ["tini", "--"]
CMD ["bun", "main.js"]
