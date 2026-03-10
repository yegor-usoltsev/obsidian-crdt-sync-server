import { Database as BunSQLite } from "bun:sqlite";
import { SQLite } from "@hocuspocus/extension-sqlite";
import { Server } from "@hocuspocus/server";
import { join } from "pathe";
import { MetadataExtension } from "./extensions/metadata.ts";

export interface SyncServerConfig {
  authToken: string;
  dataDir: string;
  maxStatelessPayloadBytes?: number;
  maxDocumentPayloadBytes?: number;
}

const DEFAULT_MAX_STATELESS_PAYLOAD_BYTES = 1 * 1024 * 1024; // 1 MB
const DEFAULT_MAX_DOCUMENT_PAYLOAD_BYTES = 90 * 1024 * 1024; // 90 MiB
const SQLITE_AUTO_VACUUM_FULL = 1;
const SQLITE_SCHEMA = `CREATE TABLE IF NOT EXISTS "documents" (
  "name" varchar(255) NOT NULL,
  "data" blob NOT NULL,
  UNIQUE(name)
)`;

export async function createSyncServer({
  authToken,
  dataDir,
  maxStatelessPayloadBytes = DEFAULT_MAX_STATELESS_PAYLOAD_BYTES,
  maxDocumentPayloadBytes = DEFAULT_MAX_DOCUMENT_PAYLOAD_BYTES,
}: SyncServerConfig): Promise<Server> {
  const marker = join(dataDir, `.bun-${Bun.randomUUIDv7()}`);
  await Bun.write(marker, "");
  await Bun.file(marker).delete();
  const database = join(dataDir, "sync.db");
  const sqlite = new BunSQLite(database);
  if (
    (
      sqlite.query("PRAGMA auto_vacuum").get() as {
        auto_vacuum?: number;
      } | null
    )?.auto_vacuum !== SQLITE_AUTO_VACUUM_FULL
  ) {
    sqlite.run("PRAGMA auto_vacuum = FULL");
    sqlite.run("VACUUM");
  }
  sqlite.run(SQLITE_SCHEMA);
  sqlite.close();

  return new Server(
    {
      name: "obsidian-crdt-sync",
      quiet: true,
      extensions: [
        new SQLite({ database }),
        new MetadataExtension({ authToken, maxStatelessPayloadBytes }),
      ],
      async onRequest({ request, response }) {
        if (
          new URL(request.url ?? "/", "http://localhost").pathname !== "/health"
        ) {
          return;
        }

        response.writeHead(200, {
          "Content-Type": "application/json",
          "X-Content-Type-Options": "nosniff",
        });
        response.end(JSON.stringify({ status: "ok" }));
        return Promise.reject(null);
      },
    },
    {
      maxPayload: maxDocumentPayloadBytes,
    },
  );
}
