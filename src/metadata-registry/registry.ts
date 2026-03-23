/**
 * Canonical metadata registry: file identity, paths, structural validation,
 * epochs, case-insensitive collision checks, and operation processing.
 */

import type { Database } from "bun:sqlite";
import { getEpoch, nextRevision } from "../shared/database";
import type {
  FileId,
  FileKind,
  FileMetadata,
  MetadataCommit,
  MetadataIntent,
  MetadataReject,
} from "../shared/types";

export interface MetadataRegistry {
  /** Process a metadata intent. Returns commit or reject. */
  processIntent(intent: MetadataIntent): MetadataCommit | MetadataReject;
  /** Update advisory content metadata and advance content anchor. */
  updateContentMetadata(
    fileId: FileId,
    digest: string,
    size: number,
    clientId: string,
  ): FileMetadata | null;
  /** Get a file by ID. */
  getFile(fileId: FileId): FileMetadata | null;
  /** Get a file by path (case-insensitive). */
  getFileByPath(path: string): FileMetadata | null;
  /** List all active (non-deleted) files. */
  listActiveFiles(): FileMetadata[];
  /** Get all files including deleted. */
  listAllFiles(): FileMetadata[];
  /** Get current epoch and revision. */
  getState(): { epoch: string; revision: number };
}

export function createMetadataRegistry(db: Database): MetadataRegistry {
  return {
    processIntent(intent: MetadataIntent): MetadataCommit | MetadataReject {
      // Check for operation replay (idempotent deduplication)
      const existingRow = db
        .query("SELECT * FROM history WHERE operation_id = ?")
        .get(intent.operationId) as RawHistoryRow | null;

      if (existingRow) {
        // Compare payload fingerprint if stored
        if (existingRow.intent_fingerprint) {
          const incomingFingerprint = computeIntentFingerprint(intent);
          if (incomingFingerprint !== existingRow.intent_fingerprint) {
            return {
              operationId: intent.operationId,
              reason: "operation ID reused with different payload",
            };
          }
        }

        // Replay: return original result without new commit
        return {
          operationId: existingRow.operation_id,
          fileId: existingRow.file_id,
          path: existingRow.path,
          kind: existingRow.kind as FileKind,
          deleted: existingRow.operation_type === "delete",
          contentAnchor: existingRow.content_anchor,
          revision: existingRow.revision,
          epoch: existingRow.epoch,
        };
      }

      switch (intent.type) {
        case "create":
          return processCreate(db, intent);
        case "rename":
        case "move":
          return processRename(db, intent);
        case "delete":
          return processDelete(db, intent);
        default:
          return {
            operationId: intent.operationId,
            reason: "unknown intent type",
          };
      }
    },

    updateContentMetadata(
      fileId: FileId,
      digest: string,
      size: number,
      clientId: string,
    ): FileMetadata | null {
      const file = db
        .query("SELECT * FROM files WHERE file_id = ? AND deleted = 0")
        .get(fileId) as RawFileRow | null;

      if (!file) return null;

      const now = Date.now();
      const newAnchor = file.content_anchor + 1;

      db.run(
        `UPDATE files SET
					content_digest = ?, content_size = ?, content_mod_time = ?,
					content_anchor = ?, updated_at = ?
				 WHERE file_id = ?`,
        [digest, size, now, newAnchor, now, fileId],
      );

      // Record content update in history
      const revision = nextRevision(db);
      const epoch = getEpoch(db);

      db.run(
        `INSERT INTO history (file_id, operation_type, path, kind, content_digest, content_size,
				  content_anchor, client_id, operation_id, timestamp, revision, epoch)
				 VALUES (?, 'content-update', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          fileId,
          file.path,
          file.kind,
          digest,
          size,
          newAnchor,
          clientId,
          crypto.randomUUID(),
          now,
          revision,
          epoch,
        ],
      );

      return this.getFile(fileId);
    },

    getFile(fileId: FileId): FileMetadata | null {
      const row = db
        .query("SELECT * FROM files WHERE file_id = ?")
        .get(fileId) as RawFileRow | null;
      return row ? rowToMetadata(row) : null;
    },

    getFileByPath(path: string): FileMetadata | null {
      const row = db
        .query(
          "SELECT * FROM files WHERE LOWER(path) = LOWER(?) AND deleted = 0",
        )
        .get(path) as RawFileRow | null;
      return row ? rowToMetadata(row) : null;
    },

    listActiveFiles(): FileMetadata[] {
      const rows = db
        .query("SELECT * FROM files WHERE deleted = 0")
        .all() as RawFileRow[];
      return rows.map(rowToMetadata);
    },

    listAllFiles(): FileMetadata[] {
      const rows = db.query("SELECT * FROM files").all() as RawFileRow[];
      return rows.map(rowToMetadata);
    },

    getState() {
      const epoch = getEpoch(db);
      const row = db
        .query("SELECT value FROM server_state WHERE key = 'revision'")
        .get() as { value: string };
      return { epoch, revision: Number.parseInt(row.value, 10) };
    },
  };
}

// --- Intent processors ---

function processCreate(
  db: Database,
  intent: MetadataIntent,
): MetadataCommit | MetadataReject {
  if (!intent.path) {
    return {
      operationId: intent.operationId,
      reason: "path is required for create",
    };
  }
  if (!intent.kind) {
    return {
      operationId: intent.operationId,
      reason: "kind is required for create",
    };
  }

  // Validate path safety
  const pathError = validatePath(intent.path);
  if (pathError) {
    return { operationId: intent.operationId, reason: pathError };
  }

  // Case-insensitive collision check
  const collision = db
    .query(
      "SELECT file_id, path FROM files WHERE LOWER(path) = LOWER(?) AND deleted = 0",
    )
    .get(intent.path) as { file_id: string; path: string } | null;

  if (collision) {
    return {
      operationId: intent.operationId,
      reason: `path collision: "${intent.path}" conflicts with existing "${collision.path}"`,
    };
  }

  const fileId = crypto.randomUUID();
  const now = Date.now();
  const revision = nextRevision(db);
  const epoch = getEpoch(db);

  db.run(
    `INSERT INTO files (file_id, path, kind, deleted, created_at, updated_at, content_anchor)
		 VALUES (?, ?, ?, 0, ?, ?, 0)`,
    [fileId, intent.path, intent.kind, now, now],
  );

  recordHistory(db, {
    fileId,
    operationType: "create",
    path: intent.path,
    kind: intent.kind,
    contentAnchor: 0,
    clientId: intent.clientId,
    operationId: intent.operationId,
    revision,
    epoch,
    intentFingerprint: computeIntentFingerprint(intent),
  });

  return {
    operationId: intent.operationId,
    fileId,
    path: intent.path,
    kind: intent.kind,
    deleted: false,
    contentAnchor: 0,
    revision,
    epoch,
  };
}

function processRename(
  db: Database,
  intent: MetadataIntent,
): MetadataCommit | MetadataReject {
  if (!intent.fileId) {
    return {
      operationId: intent.operationId,
      reason: "fileId is required for rename",
    };
  }
  if (!intent.newPath) {
    return {
      operationId: intent.operationId,
      reason: "newPath is required for rename",
    };
  }

  const pathError = validatePath(intent.newPath);
  if (pathError) {
    return { operationId: intent.operationId, reason: pathError };
  }

  const file = db
    .query("SELECT * FROM files WHERE file_id = ? AND deleted = 0")
    .get(intent.fileId) as RawFileRow | null;

  if (!file) {
    return {
      operationId: intent.operationId,
      reason: "file not found or already deleted",
    };
  }

  // Content-anchor validation: reject stale intents
  if (
    intent.contentAnchor !== undefined &&
    intent.contentAnchor < file.content_anchor
  ) {
    return {
      operationId: intent.operationId,
      reason: "stale content anchor",
    };
  }

  // Case-insensitive collision check (excluding the file being renamed)
  const collision = db
    .query(
      "SELECT file_id, path FROM files WHERE LOWER(path) = LOWER(?) AND deleted = 0 AND file_id != ?",
    )
    .get(intent.newPath, intent.fileId) as {
    file_id: string;
    path: string;
  } | null;

  if (collision) {
    return {
      operationId: intent.operationId,
      reason: `path collision: "${intent.newPath}" conflicts with existing "${collision.path}"`,
    };
  }

  // Directory: cannot move into own descendant
  if (file.kind === "directory") {
    const oldPathPrefix = `${file.path}/`;
    if (intent.newPath.startsWith(oldPathPrefix)) {
      return {
        operationId: intent.operationId,
        reason: "cannot move directory into its own descendant",
      };
    }
  }

  const now = Date.now();
  const revision = nextRevision(db);
  const epoch = getEpoch(db);

  // Update the file path
  db.run("UPDATE files SET path = ?, updated_at = ? WHERE file_id = ?", [
    intent.newPath,
    now,
    intent.fileId,
  ]);

  // If directory: update all descendant paths
  if (file.kind === "directory") {
    const oldPrefix = `${file.path}/`;
    const newPrefix = `${intent.newPath}/`;
    const descendants = db
      .query(
        "SELECT file_id, path FROM files WHERE path LIKE ? AND deleted = 0",
      )
      .all(`${file.path}/%`) as { file_id: string; path: string }[];

    for (const desc of descendants) {
      const newPath = newPrefix + desc.path.slice(oldPrefix.length);
      db.run("UPDATE files SET path = ?, updated_at = ? WHERE file_id = ?", [
        newPath,
        now,
        desc.file_id,
      ]);
    }
  }

  recordHistory(db, {
    fileId: intent.fileId,
    operationType: intent.type,
    path: intent.newPath,
    kind: file.kind as FileKind,
    contentAnchor: file.content_anchor,
    clientId: intent.clientId,
    operationId: intent.operationId,
    revision,
    epoch,
    intentFingerprint: computeIntentFingerprint(intent),
  });

  return {
    operationId: intent.operationId,
    fileId: intent.fileId,
    path: intent.newPath,
    kind: file.kind as FileKind,
    deleted: false,
    contentAnchor: file.content_anchor,
    revision,
    epoch,
  };
}

function processDelete(
  db: Database,
  intent: MetadataIntent,
): MetadataCommit | MetadataReject {
  if (!intent.fileId) {
    return {
      operationId: intent.operationId,
      reason: "fileId is required for delete",
    };
  }

  const file = db
    .query("SELECT * FROM files WHERE file_id = ? AND deleted = 0")
    .get(intent.fileId) as RawFileRow | null;

  if (!file) {
    return {
      operationId: intent.operationId,
      reason: "file not found or already deleted",
    };
  }

  // Content-anchor validation: reject stale intents
  if (
    intent.contentAnchor !== undefined &&
    intent.contentAnchor < file.content_anchor
  ) {
    return {
      operationId: intent.operationId,
      reason: "stale content anchor",
    };
  }

  const now = Date.now();
  const revision = nextRevision(db);
  const epoch = getEpoch(db);

  // Mark as deleted, record content fingerprint for non-directories
  db.run("UPDATE files SET deleted = 1, updated_at = ? WHERE file_id = ?", [
    now,
    intent.fileId,
  ]);

  // Directory: cascade delete to all descendants
  if (file.kind === "directory") {
    const descendants = db
      .query("SELECT file_id FROM files WHERE path LIKE ? AND deleted = 0")
      .all(`${file.path}/%`) as { file_id: string }[];

    for (const desc of descendants) {
      db.run("UPDATE files SET deleted = 1, updated_at = ? WHERE file_id = ?", [
        now,
        desc.file_id,
      ]);
    }
  }

  // Record history with content fingerprint for non-directories
  const deleteDigest =
    file.kind !== "directory"
      ? (intent.contentDigest ?? file.content_digest)
      : null;

  db.run(
    `INSERT INTO history (file_id, operation_type, path, kind, content_digest, content_size,
		  content_anchor, client_id, operation_id, timestamp, revision, epoch, intent_fingerprint)
		 VALUES (?, 'delete', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      intent.fileId,
      file.path,
      file.kind,
      deleteDigest,
      file.content_size,
      file.content_anchor,
      intent.clientId,
      intent.operationId,
      Date.now(),
      revision,
      epoch,
      computeIntentFingerprint(intent),
    ],
  );

  return {
    operationId: intent.operationId,
    fileId: intent.fileId,
    path: file.path,
    kind: file.kind as FileKind,
    deleted: true,
    contentAnchor: file.content_anchor,
    revision,
    epoch,
  };
}

// --- Helpers ---

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
  intent_fingerprint: string | null;
}

interface RawFileRow {
  file_id: string;
  path: string;
  kind: string;
  deleted: number;
  created_at: number;
  updated_at: number;
  content_digest: string | null;
  content_size: number | null;
  content_mod_time: number | null;
  content_anchor: number;
}

function rowToMetadata(row: RawFileRow): FileMetadata {
  return {
    fileId: row.file_id,
    path: row.path,
    kind: row.kind as FileKind,
    deleted: row.deleted === 1,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    contentDigest: row.content_digest,
    contentSize: row.content_size,
    contentModTime: row.content_mod_time,
    contentAnchor: row.content_anchor,
  };
}

function validatePath(path: string): string | null {
  if (!path) return "path is required";
  if (path.startsWith("/") || /^[A-Za-z]:/.test(path))
    return "absolute paths not allowed";
  const segments = path.split("/");
  for (const seg of segments) {
    if (seg === "..") return "path traversal not allowed";
    if (seg === ".") return "current-directory reference not allowed";
    // biome-ignore lint/suspicious/noControlCharactersInRegex: intentional control char detection
    if (/[\x00-\x1f\x7f]/.test(seg))
      return "control characters in path not allowed";
  }
  return null;
}

function computeIntentFingerprint(intent: MetadataIntent): string {
  const canonical = JSON.stringify([
    intent.type,
    intent.fileId ?? null,
    intent.path ?? null,
    intent.newPath ?? null,
    intent.kind ?? null,
    intent.contentAnchor ?? null,
    intent.contentDigest ?? null,
  ]);
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(canonical);
  return hasher.digest("hex");
}

function recordHistory(
  db: Database,
  entry: {
    fileId: string;
    operationType: string;
    path: string;
    kind: FileKind;
    contentAnchor: number;
    clientId: string;
    operationId: string;
    revision: number;
    epoch: string;
    intentFingerprint?: string;
  },
): void {
  db.run(
    `INSERT INTO history (file_id, operation_type, path, kind, content_digest, content_size,
		  content_anchor, client_id, operation_id, timestamp, revision, epoch, intent_fingerprint)
		 VALUES (?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?)`,
    [
      entry.fileId,
      entry.operationType,
      entry.path,
      entry.kind,
      entry.contentAnchor,
      entry.clientId,
      entry.operationId,
      Date.now(),
      entry.revision,
      entry.epoch,
      entry.intentFingerprint ?? null,
    ],
  );
}
