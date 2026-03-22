import type { Database } from "bun:sqlite";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { openDatabase } from "../../src/shared/database";
import { createSyncServer, type SyncServer } from "../../src/transport/server";

describe("sync server", () => {
  let db: Database;
  let server: SyncServer;
  const AUTH_TOKEN = "a".repeat(32);
  let baseUrl: string;

  beforeAll(async () => {
    db = openDatabase(":memory:");
    server = createSyncServer({
      port: 0, // random port
      authToken: AUTH_TOKEN,
      dataDir: "/tmp/crdt-sync-test",
      db,
    });
    await server.start();
    baseUrl = `http://localhost:${server.port}`;
  });

  afterAll(async () => {
    await server.stop();
    db.close();
  });

  describe("health endpoint", () => {
    it("returns 200 OK", async () => {
      const res = await fetch(`${baseUrl}/health`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { status: string };
      expect(body.status).toBe("ok");
    });

    it("does not require authentication", async () => {
      const res = await fetch(`${baseUrl}/health`);
      expect(res.status).toBe(200);
    });
  });

  describe("authentication", () => {
    it("rejects unauthenticated requests", async () => {
      const res = await fetch(`${baseUrl}/blobs/test`);
      expect(res.status).toBe(401);
    });

    it("rejects invalid token", async () => {
      const res = await fetch(`${baseUrl}/blobs/test`, {
        headers: {
          Authorization: "Bearer wrong-token-that-is-long-enough-32chars",
        },
      });
      expect(res.status).toBe(401);
    });

    it("accepts valid token", async () => {
      const res = await fetch(`${baseUrl}/blobs/test`, {
        headers: { Authorization: `Bearer ${AUTH_TOKEN}` },
      });
      // Should not be 401 (might be 404 since blob doesn't exist)
      expect(res.status).not.toBe(401);
    });
  });
});
