/**
 * Append-only history storage, restore semantics, and compaction contracts.
 */

import type { Database } from "bun:sqlite";
import { getEpoch, nextRevision } from "../shared/database";
import { log } from "../shared/log";
import type { FileId, FileKind, HistoryEntry } from "../shared/types";

export interface HistoryStore {
  /** Get history entries for a file. */
  getFileHistory(fileId: FileId): HistoryEntry[];
  /** Get all history entries since a revision. */
  getHistorySince(revision: number): HistoryEntry[];
  /** Restore a file to a specific history entry, creating a new head. */
  restore(
    fileId: FileId,
    historyEntryId: number,
    clientId: string,
  ): HistoryEntry | null;
  /** Signal compaction: bump epoch so clients rebootstrap safely. */
  compactBefore(revision: number): { deletedEntries: number; newEpoch: string };
  /** Get total history entry count. */
  getEntryCount(): number;
}

export function createHistoryStore(db: Database): HistoryStore {
  return {
    getFileHistory(fileId: FileId): HistoryEntry[] {
      const rows = db
        .query("SELECT * FROM history WHERE file_id = ? ORDER BY revision ASC")
        .all(fileId) as RawHistoryRow[];
      return rows.map(rowToHistoryEntry);
    },

    getHistorySince(revision: number): HistoryEntry[] {
      const rows = db
        .query("SELECT * FROM history WHERE revision > ? ORDER BY revision ASC")
        .all(revision) as RawHistoryRow[];
      return rows.map(rowToHistoryEntry);
    },

    restore(
      fileId: FileId,
      historyEntryId: number,
      clientId: string,
    ): HistoryEntry | null {
      const row = db
        .query("SELECT * FROM history WHERE id = ? AND file_id = ?")
        .get(historyEntryId, fileId) as RawHistoryRow | null;

      if (!row) return null;

      const revision = nextRevision(db);
      const epoch = getEpoch(db);
      const operationId = crypto.randomUUID();
      const now = Date.now();

      // Create a new canonical head from the restored state (not in-place mutation)
      db.run(
        `UPDATE files SET
					path = ?, kind = ?, deleted = 0, updated_at = ?,
					content_digest = ?, content_size = ?,
					content_anchor = content_anchor + 1
				 WHERE file_id = ?`,
        [row.path, row.kind, now, row.content_digest, row.content_size, fileId],
      );

      // Record the restore in history (append, never mutate)
      db.run(
        `INSERT INTO history (file_id, operation_type, path, kind, content_digest, content_size,
				  content_anchor, client_id, operation_id, timestamp, revision, epoch)
				 VALUES (?, 'restore', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          fileId,
          row.path,
          row.kind,
          row.content_digest,
          row.content_size,
          row.content_anchor,
          clientId,
          operationId,
          now,
          revision,
          epoch,
        ],
      );

      return {
        id: 0,
        fileId,
        operationType: "restore",
        path: row.path,
        kind: row.kind as FileKind,
        contentDigest: row.content_digest,
        contentSize: row.content_size,
        contentAnchor: row.content_anchor,
        clientId,
        operationId,
        timestamp: now,
        revision,
        epoch,
      };
    },

    compactBefore(revision: number): {
      deletedEntries: number;
      newEpoch: string;
    } {
      // Count entries to be deleted
      const countRow = db
        .query("SELECT COUNT(*) as cnt FROM history WHERE revision <= ?")
        .get(revision) as { cnt: number };

      if (countRow.cnt === 0) {
        return { deletedEntries: 0, newEpoch: getEpoch(db) };
      }

      // Delete old history entries
      db.run("DELETE FROM history WHERE revision <= ?", [revision]);

      // Bump epoch — clients with cursors referencing compacted revisions
      // must rebootstrap safely
      const newEpoch = crypto.randomUUID();
      db.run("UPDATE server_state SET value = ? WHERE key = 'epoch'", [
        newEpoch,
      ]);

      // Run incremental vacuum to reclaim space
      db.run("PRAGMA incremental_vacuum");

      log("info", "History compacted", {
        beforeRevision: revision,
        deletedEntries: countRow.cnt,
        newEpoch,
      });

      return { deletedEntries: countRow.cnt, newEpoch };
    },

    getEntryCount(): number {
      const row = db.query("SELECT COUNT(*) as cnt FROM history").get() as {
        cnt: number;
      };
      return row.cnt;
    },
  };
}

interface RawHistoryRow {
  id: number;
  file_id: string;
  operation_type: string;
  path: string;
  kind: string;
  content_digest: string | null;
  content_size: number | null;
  content_anchor: number;
  client_id: string;
  operation_id: string;
  timestamp: number;
  revision: number;
  epoch: string;
}

function rowToHistoryEntry(row: RawHistoryRow): HistoryEntry {
  return {
    id: row.id,
    fileId: row.file_id,
    operationType: row.operation_type as HistoryEntry["operationType"],
    path: row.path,
    kind: row.kind as FileKind,
    contentDigest: row.content_digest,
    contentSize: row.content_size,
    contentAnchor: row.content_anchor,
    clientId: row.client_id,
    operationId: row.operation_id,
    timestamp: row.timestamp,
    revision: row.revision,
    epoch: row.epoch,
  };
}
