/**
 * Settings snapshot storage: versioned blobs with digest and coordination anchors.
 * Keyed by fileId (from metadata registry) instead of config_path.
 */

import type { Database } from "bun:sqlite";
import type { FileId, SettingsSnapshot } from "../shared/types";

export interface SettingsStore {
  /** Store a settings snapshot keyed by fileId. */
  store(
    fileId: FileId,
    content: Uint8Array,
    digest: string,
    contentAnchor: number,
  ): SettingsSnapshot;
  /** Retrieve the latest settings snapshot by fileId. */
  getLatest(
    fileId: FileId,
  ): { content: Uint8Array; metadata: SettingsSnapshot } | null;
  /** List all tracked fileIds. */
  listFileIds(): FileId[];
}

export function createSettingsStore(
  db: Database,
  _dataDir: string,
): SettingsStore {
  // Settings blobs stored inline in SQLite since they're small
  db.run(`
		CREATE TABLE IF NOT EXISTS settings_blobs (
			file_id TEXT NOT NULL,
			content_anchor INTEGER NOT NULL,
			data BLOB NOT NULL,
			digest TEXT NOT NULL,
			size INTEGER NOT NULL,
			stored_at INTEGER NOT NULL,
			PRIMARY KEY (file_id, content_anchor)
		)
	`);

  return {
    store(
      fileId: FileId,
      content: Uint8Array,
      digest: string,
      contentAnchor: number,
    ): SettingsSnapshot {
      const snapshot: SettingsSnapshot = {
        fileId,
        configPath: "", // kept for backwards compatibility in type
        digest,
        size: content.byteLength,
        contentAnchor,
        storedAt: Date.now(),
      };

      db.run(
        `INSERT OR REPLACE INTO settings_blobs (file_id, content_anchor, data, digest, size, stored_at)
				 VALUES (?, ?, ?, ?, ?, ?)`,
        [
          fileId,
          contentAnchor,
          Buffer.from(content),
          digest,
          snapshot.size,
          snapshot.storedAt,
        ],
      );

      return snapshot;
    },

    getLatest(
      fileId: FileId,
    ): { content: Uint8Array; metadata: SettingsSnapshot } | null {
      const row = db
        .query(
          `SELECT * FROM settings_blobs WHERE file_id = ?
					 ORDER BY content_anchor DESC LIMIT 1`,
        )
        .get(fileId) as RawSettingsBlobRow | null;

      if (!row) return null;

      return {
        content: new Uint8Array(row.data),
        metadata: {
          fileId: row.file_id,
          configPath: "",
          digest: row.digest,
          size: row.size,
          contentAnchor: row.content_anchor,
          storedAt: row.stored_at,
        },
      };
    },

    listFileIds(): FileId[] {
      const rows = db
        .query("SELECT DISTINCT file_id FROM settings_blobs")
        .all() as { file_id: string }[];
      return rows.map((r) => r.file_id);
    },
  };
}

interface RawSettingsBlobRow {
  file_id: string;
  content_anchor: number;
  data: Buffer;
  digest: string;
  size: number;
  stored_at: number;
}
