import type { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  type BlobStore,
  createBlobStore,
} from "../../src/blob-store/blob-store";
import { openDatabase } from "../../src/shared/database";

describe("blob-store", () => {
  let db: Database;
  let blobStore: BlobStore;
  let tempDir: string;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "blob-test-"));
    db = openDatabase(":memory:");
    blobStore = await createBlobStore(db, tempDir);
  });

  afterEach(async () => {
    db.close();
    await rm(tempDir, { recursive: true, force: true });
  });

  it("stores and retrieves a blob", async () => {
    const content = new TextEncoder().encode("binary data here");
    const digest = computeDigestSync(content);

    const record = await blobStore.store("file-1", content, digest, 1);
    expect(record.fileId).toBe("file-1");
    expect(record.digest).toBe(digest);
    expect(record.size).toBe(content.byteLength);

    const retrieved = await blobStore.retrieve("file-1");
    expect(retrieved).not.toBeNull();
    expect(new TextDecoder().decode(retrieved?.content)).toBe(
      "binary data here",
    );
  });

  it("supports content-addressed reuse", async () => {
    const content = new TextEncoder().encode("same data");
    const digest = computeDigestSync(content);

    await blobStore.store("file-1", content, digest, 1);
    await blobStore.store("file-2", content, digest, 1);

    expect(blobStore.existsByDigest(digest)).toBe(true);

    const r1 = await blobStore.retrieve("file-1");
    const r2 = await blobStore.retrieve("file-2");
    expect(r1?.metadata.digest).toBe(r2?.metadata.digest);
  });

  it("returns null for non-existent file", async () => {
    const result = await blobStore.retrieve("nonexistent");
    expect(result).toBeNull();
  });

  it("returns false for non-existent digest", () => {
    expect(blobStore.existsByDigest("nonexistent")).toBe(false);
  });

  it("stores multiple versions", async () => {
    const v1 = new TextEncoder().encode("version 1");
    const v2 = new TextEncoder().encode("version 2");
    const d1 = computeDigestSync(v1);
    const d2 = computeDigestSync(v2);

    await blobStore.store("file-1", v1, d1, 1);
    await blobStore.store("file-1", v2, d2, 2);

    // Latest version should be v2
    const result = await blobStore.retrieve("file-1");
    expect(new TextDecoder().decode(result?.content)).toBe("version 2");
    expect(result?.metadata.contentAnchor).toBe(2);
  });
});

function computeDigestSync(content: Uint8Array): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(content);
  return hasher.digest("hex");
}
