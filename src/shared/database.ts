/**
 * SQLite database initialization and schema management.
 */

import { Database } from "bun:sqlite";
import { log } from "./log";

/**
 * Open and initialize the server database.
 * Creates all tables if they don't exist and enables auto-vacuum.
 */
export function openDatabase(dbPath: string): Database {
  const db = new Database(dbPath);

  // Enable auto-vacuum so deleted state reclaims disk space
  db.run("PRAGMA auto_vacuum = INCREMENTAL");
  db.run("PRAGMA journal_mode = WAL");
  db.run("PRAGMA foreign_keys = ON");

  // Metadata registry
  db.run(`
		CREATE TABLE IF NOT EXISTS files (
			file_id TEXT PRIMARY KEY,
			path TEXT NOT NULL,
			kind TEXT NOT NULL CHECK (kind IN ('text', 'binary', 'directory')),
			deleted INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			content_digest TEXT,
			content_size INTEGER,
			content_mod_time INTEGER,
			content_anchor INTEGER NOT NULL DEFAULT 0
		)
	`);

  // Case-insensitive path index for collision checks
  db.run(`
		CREATE INDEX IF NOT EXISTS idx_files_path_lower
		ON files (LOWER(path)) WHERE deleted = 0
	`);

  // Append-only history
  db.run(`
		CREATE TABLE IF NOT EXISTS history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_id TEXT NOT NULL,
			operation_type TEXT NOT NULL,
			path TEXT NOT NULL,
			kind TEXT NOT NULL,
			content_digest TEXT,
			content_size INTEGER,
			content_anchor INTEGER NOT NULL,
			client_id TEXT NOT NULL,
			operation_id TEXT NOT NULL,
			timestamp INTEGER NOT NULL,
			revision INTEGER NOT NULL,
			epoch TEXT NOT NULL,
			intent_fingerprint TEXT
		)
	`);

  // Operation deduplication index
  db.run(`
		CREATE UNIQUE INDEX IF NOT EXISTS idx_history_operation
		ON history (operation_id)
	`);

  // Blob metadata
  db.run(`
		CREATE TABLE IF NOT EXISTS blobs (
			file_id TEXT NOT NULL,
			digest TEXT NOT NULL,
			size INTEGER NOT NULL,
			content_anchor INTEGER NOT NULL,
			stored_at INTEGER NOT NULL,
			PRIMARY KEY (file_id, content_anchor)
		)
	`);

  // Content-addressed blob reuse
  db.run(`
		CREATE INDEX IF NOT EXISTS idx_blobs_digest
		ON blobs (digest)
	`);

  // Settings snapshots (legacy table kept for migration safety)
  db.run(`
		CREATE TABLE IF NOT EXISTS settings_snapshots (
			config_path TEXT NOT NULL,
			digest TEXT NOT NULL,
			size INTEGER NOT NULL,
			content_anchor INTEGER NOT NULL,
			stored_at INTEGER NOT NULL,
			PRIMARY KEY (config_path, content_anchor)
		)
	`);

  // Epoch / revision tracking
  db.run(`
		CREATE TABLE IF NOT EXISTS server_state (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)
	`);

  // Initialize epoch and revision if not present
  const epochRow = db
    .query("SELECT value FROM server_state WHERE key = 'epoch'")
    .get() as { value: string } | null;
  if (!epochRow) {
    const epoch = crypto.randomUUID();
    db.run("INSERT INTO server_state (key, value) VALUES ('epoch', ?)", [
      epoch,
    ]);
    db.run("INSERT INTO server_state (key, value) VALUES ('revision', '0')");
    log("info", "Initialized server state", { epoch });
  }

  log("info", "Database initialized", { path: dbPath });
  return db;
}

/** Get current epoch. */
export function getEpoch(db: Database): string {
  const row = db
    .query("SELECT value FROM server_state WHERE key = 'epoch'")
    .get() as { value: string };
  return row.value;
}

/** Get and increment revision. */
export function nextRevision(db: Database): number {
  const row = db
    .query("SELECT value FROM server_state WHERE key = 'revision'")
    .get() as { value: string };
  const next = Number.parseInt(row.value, 10) + 1;
  db.run("UPDATE server_state SET value = ? WHERE key = 'revision'", [
    String(next),
  ]);
  return next;
}

/** Get current revision. */
export function getRevision(db: Database): number {
  const row = db
    .query("SELECT value FROM server_state WHERE key = 'revision'")
    .get() as { value: string };
  return Number.parseInt(row.value, 10);
}
