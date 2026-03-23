import type { Database } from "bun:sqlite";
import { beforeEach, describe, expect, it } from "bun:test";
import {
  createSettingsStore,
  type SettingsStore,
} from "../../src/settings-store/settings-store";
import { openDatabase } from "../../src/shared/database";

describe("settings-store", () => {
  let db: Database;
  let store: SettingsStore;

  beforeEach(() => {
    db = openDatabase(":memory:");
    store = createSettingsStore(db, "/tmp/test-settings");
  });

  it("stores and retrieves a settings snapshot by fileId", () => {
    const content = new TextEncoder().encode('{"theme": "dark"}');
    const digest = computeDigest(content);
    const fileId = "file-001";

    const snapshot = store.store(fileId, content, digest, 1);
    expect(snapshot.fileId).toBe(fileId);
    expect(snapshot.digest).toBe(digest);
    expect(snapshot.size).toBe(content.byteLength);

    const result = store.getLatest(fileId);
    expect(result).not.toBeNull();
    expect(new TextDecoder().decode(result?.content)).toBe('{"theme": "dark"}');
    expect(result?.metadata.digest).toBe(digest);
  });

  it("returns latest version", () => {
    const fileId = "file-002";
    const v1 = new TextEncoder().encode('{"v": 1}');
    const v2 = new TextEncoder().encode('{"v": 2}');

    store.store(fileId, v1, computeDigest(v1), 1);
    store.store(fileId, v2, computeDigest(v2), 2);

    const result = store.getLatest(fileId);
    expect(new TextDecoder().decode(result?.content)).toBe('{"v": 2}');
    expect(result?.metadata.contentAnchor).toBe(2);
  });

  it("returns null for non-existent fileId", () => {
    expect(store.getLatest("nonexistent")).toBeNull();
  });

  it("lists tracked fileIds", () => {
    const c1 = new TextEncoder().encode("{}");
    const c2 = new TextEncoder().encode("[]");

    store.store("file-a", c1, computeDigest(c1), 1);
    store.store("file-b", c2, computeDigest(c2), 1);

    const fileIds = store.listFileIds();
    expect(fileIds).toContain("file-a");
    expect(fileIds).toContain("file-b");
  });
});

function computeDigest(content: Uint8Array): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(content);
  return hasher.digest("hex");
}
