# obsidian-crdt-sync-server

[![Build Status](https://github.com/yegor-usoltsev/obsidian-crdt-sync-server/actions/workflows/ci.yml/badge.svg)](https://github.com/yegor-usoltsev/obsidian-crdt-sync-server/actions)
[![GitHub Release](https://img.shields.io/github/v/release/yegor-usoltsev/obsidian-crdt-sync-server?sort=semver)](https://github.com/yegor-usoltsev/obsidian-crdt-sync-server/releases)
[![Docker Image (docker.io)](https://img.shields.io/docker/v/yusoltsev/obsidian-crdt-sync-server?label=docker.io&sort=semver)](https://hub.docker.com/r/yusoltsev/obsidian-crdt-sync-server)
[![Docker Image (ghcr.io)](https://img.shields.io/docker/v/yusoltsev/obsidian-crdt-sync-server?label=ghcr.io&sort=semver)](https://github.com/yegor-usoltsev/obsidian-crdt-sync-server/pkgs/container/obsidian-crdt-sync-server)
[![Docker Image Size](https://img.shields.io/docker/image-size/yusoltsev/obsidian-crdt-sync-server?sort=semver&arch=amd64)](https://hub.docker.com/r/yusoltsev/obsidian-crdt-sync-server/tags)

> ⚠️ **Early development** — This server is in active early development. Use at your own risk.

A self-hosted WebSocket server for the [obsidian-crdt-sync](https://github.com/yegor-usoltsev/obsidian-crdt-sync) Obsidian plugin. Persists vault state in SQLite via Bun's native SQLite APIs and optionally backs up to a remote Git repository.

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

Generate a token with:

```sh
openssl rand -base64 32
```

Then start the server:

```sh
docker compose up -d
```

### Docker

```sh
docker run -d \
  -p 3000:3000 \
  -v ./data:/data \
  -e AUTH_TOKEN="your-random-secret-at-least-32-chars" \
  -e DATA_DIR=/data/db \
  -e BACKUP_GIT_WORKTREE_DIR=/data/git \
  yusoltsev/obsidian-crdt-sync-server:latest
```

### From source

```sh
bun install
bun src/index.ts
```

Set environment variables via a `.env` file or export them before running. See `.env.example` for all options.

## Environment Variables

| Variable                      | Required            | Default                     | Description                                     |
| ----------------------------- | ------------------- | --------------------------- | ----------------------------------------------- |
| `AUTH_TOKEN`                  | ✅                  | —                           | Shared secret for WebSocket auth (min 32 chars) |
| `PORT`                        |                     | `3000`                      | Port to listen on                               |
| `DATA_DIR`                    |                     | `./data/db`                 | Directory for the SQLite database               |
| `BACKUP_GIT_INTERVAL_MINUTES` |                     | —                           | Enable periodic Git backup (positive integer)   |
| `BACKUP_GIT_URL`              | when backup enabled | —                           | HTTPS remote URL                                |
| `BACKUP_GIT_USERNAME`         | when backup enabled | —                           | Git HTTPS username                              |
| `BACKUP_GIT_PASSWORD`         | when backup enabled | —                           | Git HTTPS password or token                     |
| `BACKUP_GIT_BRANCH`           |                     | `main`                      | Branch to push backups to                       |
| `BACKUP_GIT_WORKTREE_DIR`     |                     | `./data/git`                | Local worktree directory                        |
| `BACKUP_GIT_AUTHOR_NAME`      |                     | `obsidian-crdt-sync-server` | Git author name for backup commits              |
| `BACKUP_GIT_AUTHOR_EMAIL`     |                     | `backup@localhost`          | Git author email for backup commits             |

## Health Check

`GET /health` returns `{"status":"ok"}` with HTTP 200.

## Security

- **TLS**: The server does not terminate TLS itself. Put it behind a reverse proxy (e.g. nginx, Caddy, Traefik) with a valid TLS certificate. Never expose it on a public network without TLS.
- **Auth token**: The shared token is the only authentication mechanism. Use a strong random value and rotate it if compromised.
- **Data at rest**: Vault data is stored unencrypted in SQLite. Secure the host and the `DATA_DIR` accordingly.
- **Git backup credentials**: `BACKUP_GIT_PASSWORD` is used as a plain HTTPS password or personal access token. Use a token with minimal required permissions.

## Docker Images

This server is published as a multi-platform Docker image for `linux/amd64` and `linux/arm64`.

Images are available from:

- [yusoltsev/obsidian-crdt-sync-server](https://hub.docker.com/r/yusoltsev/obsidian-crdt-sync-server)
- [ghcr.io/yegor-usoltsev/obsidian-crdt-sync-server](https://github.com/yegor-usoltsev/obsidian-crdt-sync-server/pkgs/container/obsidian-crdt-sync-server)

## Releasing

Create a release tag from a clean `main` branch:

```sh
bun run release patch
# or: bun run release minor
# or: bun run release major
```

The local release script computes the next semantic version from existing Git tags, creates the new `vX.Y.Z` tag, and pushes it to GitHub.

The GitHub Actions release workflow then runs [GoReleaser](https://goreleaser.com/) on that tag. GoReleaser creates the GitHub release and builds/publishes the Docker image to both Docker Hub and GHCR using the repository `Dockerfile`. The release pipeline packages the application source tree into the Docker build context rather than compiling a standalone Bun executable.

For a local dry run of the release pipeline:

```sh
goreleaser release --snapshot --clean
```

For a plain local image build without GoReleaser:

```sh
docker build -t obsidian-crdt-sync-server:test .
```

## Versioning

This project uses [Semantic Versioning](https://semver.org). Release tags use the `vX.Y.Z` format and drive both GitHub releases and Docker image tags.

## Contributing

Pull requests are welcome. For larger changes, open an issue first, especially if the change affects the wire protocol, on-disk storage format, or release packaging.

## License

[MIT](https://github.com/yegor-usoltsev/obsidian-crdt-sync-server/blob/main/LICENSE)
