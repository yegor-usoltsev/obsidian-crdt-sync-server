/**
 * CRDT Sync Server - Main entry point.
 */

import { join } from "node:path";
import {
  createGitBackupJob,
  readGitBackupConfig,
  validateWorktreeOverlap,
} from "./backup/git-backup";
import { createBlobStore } from "./blob-store/blob-store";
import { createHistoryStore } from "./history/history-store";
import { createMetadataRegistry } from "./metadata-registry/registry";
import { createSettingsStore } from "./settings-store/settings-store";
import { openDatabase } from "./shared/database";
import { log } from "./shared/log";
import { createTextDocService } from "./text-doc-service/text-doc-service";
import { validateTokenConfig } from "./transport/auth";
import { createSyncServer } from "./transport/server";

const PORT = Number(process.env.PORT ?? 3000);
const AUTH_TOKEN = process.env.AUTH_TOKEN ?? "";
const DATA_DIR = process.env.DATA_DIR ?? "./data";

// Validate auth token
const tokenError = validateTokenConfig(AUTH_TOKEN);
if (tokenError) {
  log("error", `Invalid AUTH_TOKEN: ${tokenError}`);
  process.exit(1);
}

// Initialize database
const dbPath = join(DATA_DIR, "sync.db");
const db = openDatabase(dbPath);

// Initialize subsystems in order: database → registry → blobStore → settingsStore → historyStore → textDocService
const registry = createMetadataRegistry(db);
const blobStore = await createBlobStore(db, DATA_DIR);
const settingsStore = createSettingsStore(db);
const historyStore = createHistoryStore(db, blobStore, settingsStore);
// Deferred broadcast - wired after server creation
let broadcastFn: (msg: import("./transport/messages").ControlResponse) => void =
  () => {};
const textDocService = createTextDocService({
  db,
  authToken: AUTH_TOKEN,
  registry,
  broadcast: (msg) => broadcastFn(msg),
});

// Create and start server with all subsystems
const server = createSyncServer({
  port: PORT,
  authToken: AUTH_TOKEN,
  dataDir: DATA_DIR,
  db,
  registry,
  historyStore,
  blobStore,
  settingsStore,
  textDocService,
});

// Wire deferred broadcast
broadcastFn = (msg) => server.broadcast(msg);

await server.start();
log("info", "Server ready", { port: PORT, dataDir: DATA_DIR });

// Optional Git backup
const backupConfig = readGitBackupConfig();
let backupJob: ReturnType<typeof createGitBackupJob> | null = null;
if (backupConfig) {
  const overlapError = validateWorktreeOverlap(
    backupConfig.worktreePath,
    DATA_DIR,
  );
  if (overlapError) {
    log("error", overlapError);
  } else {
    backupJob = createGitBackupJob(
      backupConfig,
      registry,
      db,
      DATA_DIR,
      blobStore,
      settingsStore,
    );
    backupJob.start();
  }
}

// Graceful shutdown: tear down in reverse order
async function shutdown() {
  log("info", "Shutting down...");
  backupJob?.stop();
  await server.stop();
  db.close();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
process.on("unhandledRejection", (err) => {
  log("error", "Unhandled rejection", {
    error: err instanceof Error ? err.message : String(err),
  });
});
