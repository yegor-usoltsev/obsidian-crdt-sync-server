/**
 * Settings snapshot storage: versioned blobs with digest and coordination anchors.
 */

import type { Database } from "bun:sqlite";
import type { SettingsSnapshot } from "../shared/types";

export interface SettingsStore {
  /** Store a settings snapshot. */
  store(
    configPath: string,
    content: Uint8Array,
    digest: string,
    contentAnchor: number,
  ): SettingsSnapshot;
  /** Retrieve the latest settings snapshot for a config path. */
  getLatest(
    configPath: string,
  ): { content: Uint8Array; metadata: SettingsSnapshot } | null;
  /** List all tracked settings paths. */
  listPaths(): string[];
}

export function createSettingsStore(
  db: Database,
  _dataDir: string,
): SettingsStore {
  // Settings blobs stored inline in SQLite since they're small
  db.run(`
		CREATE TABLE IF NOT EXISTS settings_blobs (
			config_path TEXT NOT NULL,
			content_anchor INTEGER NOT NULL,
			data BLOB NOT NULL,
			digest TEXT NOT NULL,
			size INTEGER NOT NULL,
			stored_at INTEGER NOT NULL,
			PRIMARY KEY (config_path, content_anchor)
		)
	`);

  return {
    store(
      configPath: string,
      content: Uint8Array,
      digest: string,
      contentAnchor: number,
    ): SettingsSnapshot {
      const snapshot: SettingsSnapshot = {
        configPath,
        digest,
        size: content.byteLength,
        contentAnchor,
        storedAt: Date.now(),
      };

      db.run(
        `INSERT OR REPLACE INTO settings_blobs (config_path, content_anchor, data, digest, size, stored_at)
				 VALUES (?, ?, ?, ?, ?, ?)`,
        [
          configPath,
          contentAnchor,
          Buffer.from(content),
          digest,
          snapshot.size,
          snapshot.storedAt,
        ],
      );

      // Also update the settings_snapshots metadata table
      db.run(
        `INSERT OR REPLACE INTO settings_snapshots (config_path, digest, size, content_anchor, stored_at)
				 VALUES (?, ?, ?, ?, ?)`,
        [configPath, digest, snapshot.size, contentAnchor, snapshot.storedAt],
      );

      return snapshot;
    },

    getLatest(
      configPath: string,
    ): { content: Uint8Array; metadata: SettingsSnapshot } | null {
      const row = db
        .query(
          `SELECT * FROM settings_blobs WHERE config_path = ?
					 ORDER BY content_anchor DESC LIMIT 1`,
        )
        .get(configPath) as RawSettingsBlobRow | null;

      if (!row) return null;

      return {
        content: new Uint8Array(row.data),
        metadata: {
          configPath: row.config_path,
          digest: row.digest,
          size: row.size,
          contentAnchor: row.content_anchor,
          storedAt: row.stored_at,
        },
      };
    },

    listPaths(): string[] {
      const rows = db
        .query("SELECT DISTINCT config_path FROM settings_snapshots")
        .all() as { config_path: string }[];
      return rows.map((r) => r.config_path);
    },
  };
}

interface RawSettingsBlobRow {
  config_path: string;
  content_anchor: number;
  data: Buffer;
  digest: string;
  size: number;
  stored_at: number;
}
