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

  it("stores and retrieves a settings snapshot", () => {
    const content = new TextEncoder().encode('{"theme": "dark"}');
    const digest = computeDigest(content);

    const snapshot = store.store("app.json", content, digest, 1);
    expect(snapshot.configPath).toBe("app.json");
    expect(snapshot.digest).toBe(digest);
    expect(snapshot.size).toBe(content.byteLength);

    const result = store.getLatest("app.json");
    expect(result).not.toBeNull();
    expect(new TextDecoder().decode(result?.content)).toBe('{"theme": "dark"}');
    expect(result?.metadata.digest).toBe(digest);
  });

  it("returns latest version", () => {
    const v1 = new TextEncoder().encode('{"v": 1}');
    const v2 = new TextEncoder().encode('{"v": 2}');

    store.store("app.json", v1, computeDigest(v1), 1);
    store.store("app.json", v2, computeDigest(v2), 2);

    const result = store.getLatest("app.json");
    expect(new TextDecoder().decode(result?.content)).toBe('{"v": 2}');
    expect(result?.metadata.contentAnchor).toBe(2);
  });

  it("returns null for non-existent path", () => {
    expect(store.getLatest("nonexistent.json")).toBeNull();
  });

  it("lists tracked paths", () => {
    const c1 = new TextEncoder().encode("{}");
    const c2 = new TextEncoder().encode("[]");

    store.store("app.json", c1, computeDigest(c1), 1);
    store.store("hotkeys.json", c2, computeDigest(c2), 1);

    const paths = store.listPaths();
    expect(paths).toContain("app.json");
    expect(paths).toContain("hotkeys.json");
  });
});

function computeDigest(content: Uint8Array): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(content);
  return hasher.digest("hex");
}
