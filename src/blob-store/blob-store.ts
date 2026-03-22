/**
 * Binary blob storage: disk-backed payloads with SQLite metadata.
 * Content-addressed reuse via digest.
 */

import type { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { log } from "../shared/log";
import type { BlobRecord, FileId } from "../shared/types";

export interface BlobStore {
  /** Store a blob payload and record metadata. */
  store(
    fileId: FileId,
    content: Uint8Array,
    digest: string,
    contentAnchor: number,
  ): Promise<BlobRecord>;
  /** Retrieve a blob payload by file ID (latest version). */
  retrieve(
    fileId: FileId,
  ): Promise<{ content: Uint8Array; metadata: BlobRecord } | null>;
  /** Check if a blob with a given digest already exists. */
  existsByDigest(digest: string): boolean;
  /** Get blob metadata for a file. */
  getMetadata(fileId: FileId): BlobRecord | null;
}

export async function createBlobStore(
  db: Database,
  dataDir: string,
): Promise<BlobStore> {
  const blobDir = join(dataDir, "blobs");
  await mkdir(blobDir, { recursive: true });

  return {
    async store(
      fileId: FileId,
      content: Uint8Array,
      digest: string,
      contentAnchor: number,
    ): Promise<BlobRecord> {
      // Content-addressed storage: filename is the digest
      const blobPath = join(blobDir, digest);

      // Only write if this digest doesn't already exist on disk
      if (!(await Bun.file(blobPath).exists())) {
        await Bun.write(blobPath, content);
      }

      const record: BlobRecord = {
        fileId,
        digest,
        size: content.byteLength,
        contentAnchor,
        storedAt: Date.now(),
      };

      db.run(
        `INSERT OR REPLACE INTO blobs (file_id, digest, size, content_anchor, stored_at)
				 VALUES (?, ?, ?, ?, ?)`,
        [
          record.fileId,
          record.digest,
          record.size,
          record.contentAnchor,
          record.storedAt,
        ],
      );

      log("debug", "Stored blob", { fileId, digest, size: record.size });
      return record;
    },

    async retrieve(
      fileId: FileId,
    ): Promise<{ content: Uint8Array; metadata: BlobRecord } | null> {
      const row = db
        .query(
          "SELECT * FROM blobs WHERE file_id = ? ORDER BY content_anchor DESC LIMIT 1",
        )
        .get(fileId) as RawBlobRow | null;

      if (!row) return null;

      const blobPath = join(blobDir, row.digest);
      const file = Bun.file(blobPath);
      if (!(await file.exists())) {
        log("warn", "Blob file missing on disk", {
          fileId,
          digest: row.digest,
        });
        return null;
      }

      const content = new Uint8Array(await file.arrayBuffer());
      return {
        content,
        metadata: rowToBlobRecord(row),
      };
    },

    existsByDigest(digest: string): boolean {
      const row = db
        .query("SELECT 1 FROM blobs WHERE digest = ? LIMIT 1")
        .get(digest);
      return row !== null;
    },

    getMetadata(fileId: FileId): BlobRecord | null {
      const row = db
        .query(
          "SELECT * FROM blobs WHERE file_id = ? ORDER BY content_anchor DESC LIMIT 1",
        )
        .get(fileId) as RawBlobRow | null;
      return row ? rowToBlobRecord(row) : null;
    },
  };
}

interface RawBlobRow {
  file_id: string;
  digest: string;
  size: number;
  content_anchor: number;
  stored_at: number;
}

function rowToBlobRecord(row: RawBlobRow): BlobRecord {
  return {
    fileId: row.file_id,
    digest: row.digest,
    size: row.size,
    contentAnchor: row.content_anchor,
    storedAt: row.stored_at,
  };
}
