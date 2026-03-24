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
  /** Retrieve a settings snapshot at a specific content anchor. */
  getByAnchor(
    fileId: FileId,
    contentAnchor: number,
  ): { content: Uint8Array; metadata: SettingsSnapshot } | null;
}

export function createSettingsStore(db: Database): SettingsStore {
  return {
    store(
      fileId: FileId,
      content: Uint8Array,
      digest: string,
      contentAnchor: number,
    ): SettingsSnapshot {
      const snapshot: SettingsSnapshot = {
        fileId,
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

    getByAnchor(
      fileId: FileId,
      contentAnchor: number,
    ): { content: Uint8Array; metadata: SettingsSnapshot } | null {
      const row = db
        .query(
          "SELECT * FROM settings_blobs WHERE file_id = ? AND content_anchor = ?",
        )
        .get(fileId, contentAnchor) as RawSettingsBlobRow | null;

      if (!row) return null;

      return {
        content: new Uint8Array(row.data),
        metadata: {
          fileId: row.file_id,
          digest: row.digest,
          size: row.size,
          contentAnchor: row.content_anchor,
          storedAt: row.stored_at,
        },
      };
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
