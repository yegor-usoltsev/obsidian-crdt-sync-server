# obsidian-crdt-sync-server

[![CI](https://github.com/yegor-usoltsev/obsidian-crdt-sync-server/actions/workflows/ci.yml/badge.svg)](https://github.com/yegor-usoltsev/obsidian-crdt-sync-server/actions/workflows/ci.yml)
[![Docker](https://img.shields.io/badge/docker-ghcr.io-2496ED?logo=docker&logoColor=white)](https://github.com/yegor-usoltsev/obsidian-crdt-sync-server/pkgs/container/obsidian-crdt-sync-server)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> ⚠️ **Early development** — This server is in active early development. Use at your own risk.

A self-hosted WebSocket server for the [obsidian-crdt-sync](https://github.com/yegor-usoltsev/obsidian-crdt-sync) Obsidian plugin. Persists vault state in SQLite and optionally backs up to a remote Git repository.

## How it works

- Clients authenticate over the WebSocket handshake using a shared `AUTH_TOKEN`. The token is checked with a constant-time comparison to prevent timing attacks. IPs that fail authentication more than 5 times per minute are rate-limited.
- Vault content (text and binary files) is stored as [Yjs](https://github.com/yjs/yjs) documents in a SQLite database. Each document is persisted to disk and loaded into memory on first access.
- File metadata (paths, renames, deletes) is managed through a server-authoritative ordered event log. The server validates every metadata operation, rejects invalid or conflicting operations, and broadcasts commits to all connected clients.
- The optional Git backup job periodically materializes the full vault to a local worktree and pushes commits to a remote HTTPS repository.

## Usage

### Docker Compose (recommended)

Copy `.env.example` to `.env` and set `AUTH_TOKEN`:

```sh
cp .env.example .env
# edit .env and set AUTH_TOKEN
```

Then start the server:

```sh
docker compose up -d
```

Generate a token with:

```sh
openssl rand -base64 32
```

### Docker

```sh
docker run -d \
  -p 3000:3000 \
  -v ./data:/data \
  -e AUTH_TOKEN="your-random-secret-at-least-32-chars" \
  -e DATA_DIR=/data \
  ghcr.io/yegor-usoltsev/obsidian-crdt-sync-server:latest
```

### From source

```sh
bun install
bun src/index.ts
```

Set environment variables via a `.env` file or export them before running. See `.env.example` for all options.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `AUTH_TOKEN` | ✅ | — | Shared secret for WebSocket auth (min 32 chars) |
| `PORT` | | `3000` | Port to listen on |
| `DATA_DIR` | | `./data` | Directory for the SQLite database |
| `BACKUP_GIT_INTERVAL_MINUTES` | | — | Enable periodic Git backup (positive integer) |
| `BACKUP_GIT_URL` | when backup enabled | — | HTTPS remote URL |
| `BACKUP_GIT_USERNAME` | when backup enabled | — | Git HTTPS username |
| `BACKUP_GIT_PASSWORD` | when backup enabled | — | Git HTTPS password or token |
| `BACKUP_GIT_BRANCH` | | `main` | Branch to push backups to |
| `BACKUP_GIT_WORKTREE_DIR` | | `<DATA_DIR>-git-backup` | Local worktree directory |
| `BACKUP_GIT_AUTHOR_NAME` | | `obsidian-crdt-sync-server` | Git author name for backup commits |
| `BACKUP_GIT_AUTHOR_EMAIL` | | `backup@localhost` | Git author email for backup commits |

## Health Check

`GET /health` returns `{"status":"ok"}` with HTTP 200.

## Security

- **TLS**: The server does not terminate TLS itself. Put it behind a reverse proxy (e.g. nginx, Caddy, Traefik) with a valid TLS certificate. Never expose it on a public network without TLS.
- **Auth token**: The shared token is the only authentication mechanism. Use a strong random value and rotate it if compromised.
- **Data at rest**: Vault data is stored unencrypted in SQLite. Secure the host and the `DATA_DIR` accordingly.
- **Git backup credentials**: `BACKUP_GIT_PASSWORD` is used as a plain HTTPS password or personal access token. Use a token with minimal required permissions.

## License

[MIT](LICENSE) © [Yegor Usoltsev](https://github.com/yegor-usoltsev)
