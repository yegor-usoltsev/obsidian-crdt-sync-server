import { Database as BunSQLite } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { $ } from "bun";
import { join } from "pathe";
import * as Y from "yjs";
import {
  BACKUP_GITIGNORE,
  type GitBackupConfig,
  readGitBackupConfig,
  runGitBackupOnce,
} from "../src/git-backup.ts";
import {
  applyMetadataOp,
  CONTENT_DOCUMENT_NAME,
  initializeMetadataDoc,
  META_DOCUMENT_NAME,
} from "../src/meta-doc.ts";
import { createSyncServer } from "../src/server.ts";

const tempDirs: string[] = [];
const TEST_CONNECTION_CONFIG = {
  isAuthenticated: true,
  readOnly: false,
} as const;
const TEMP_DIR = Bun.env.TMPDIR || "/tmp";

async function createTempDir(prefix: string): Promise<string> {
  const dir = join(TEMP_DIR, `${prefix}${Bun.randomUUIDv7()}`);
  await Bun.write(join(dir, ".tmp"), "");
  await Bun.file(join(dir, ".tmp")).delete();
  tempDirs.push(dir);
  return dir;
}

async function git(
  cwd: string,
  args: string[],
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const result = await $`git ${args}`.cwd(cwd).quiet().nothrow();
  return {
    stdout: result.stdout.toString().trim(),
    stderr: result.stderr.toString().trim(),
    exitCode: result.exitCode,
  };
}

afterEach(async () => {
  for (const dir of tempDirs.splice(0)) {
    await $`rm -rf ${dir}`.quiet();
  }
});

describe("readGitBackupConfig", () => {
  test("returns null when backup is disabled", () => {
    expect(readGitBackupConfig({}, "/tmp/data/db")).toBeNull();
  });

  test("rejects non-https remotes", () => {
    expect(() => {
      readGitBackupConfig(
        {
          BACKUP_GIT_INTERVAL_MINUTES: "5",
          BACKUP_GIT_URL: "ssh://git@example.com/repo.git",
          BACKUP_GIT_USERNAME: "user",
          BACKUP_GIT_PASSWORD: "token",
        },
        "/tmp/data/db",
      );
    }).toThrow("https://");
  });

  test("rejects missing credentials when backup is enabled", () => {
    expect(() => {
      readGitBackupConfig(
        {
          BACKUP_GIT_INTERVAL_MINUTES: "5",
          BACKUP_GIT_URL: "https://example.com/repo.git",
        },
        "/tmp/data/db",
      );
    }).toThrow("BACKUP_GIT_USERNAME");
  });

  test("rejects a worktree that points at the live data directory", () => {
    expect(() => {
      readGitBackupConfig(
        {
          BACKUP_GIT_INTERVAL_MINUTES: "5",
          BACKUP_GIT_URL: "https://example.com/repo.git",
          BACKUP_GIT_USERNAME: "user",
          BACKUP_GIT_PASSWORD: "token",
          BACKUP_GIT_WORKTREE_DIR: "/tmp/data/db",
        },
        "/tmp/data/db",
      );
    }).toThrow("live DATA_DIR");
  });

  test("rejects a worktree that contains the live data directory", () => {
    expect(() => {
      readGitBackupConfig(
        {
          BACKUP_GIT_INTERVAL_MINUTES: "5",
          BACKUP_GIT_URL: "https://example.com/repo.git",
          BACKUP_GIT_USERNAME: "user",
          BACKUP_GIT_PASSWORD: "token",
          BACKUP_GIT_WORKTREE_DIR: "/tmp",
        },
        "/tmp/data/db",
      );
    }).toThrow("live DATA_DIR");
  });

  test("rejects a worktree nested inside the live data directory", () => {
    expect(() => {
      readGitBackupConfig(
        {
          BACKUP_GIT_INTERVAL_MINUTES: "5",
          BACKUP_GIT_URL: "https://example.com/repo.git",
          BACKUP_GIT_USERNAME: "user",
          BACKUP_GIT_PASSWORD: "token",
          BACKUP_GIT_WORKTREE_DIR: "/tmp/data/db/git",
        },
        "/tmp/data/db",
      );
    }).toThrow("live DATA_DIR");
  });

  test("defaults the backup worktree to ./data/git", () => {
    expect(
      readGitBackupConfig(
        {
          BACKUP_GIT_INTERVAL_MINUTES: "5",
          BACKUP_GIT_URL: "https://example.com/repo.git",
          BACKUP_GIT_USERNAME: "user",
          BACKUP_GIT_PASSWORD: "token",
        },
        "/tmp/data/db",
      ),
    ).toMatchObject({
      worktreeDir: "./data/git",
    });
  });
});

describe("runGitBackupOnce", () => {
  test("materializes the vault tree, applies the hardcoded .gitignore, and commits that file", async () => {
    const dataDir = await createTempDir("git-backup-data-");
    const remoteDir = await createTempDir("git-backup-remote-");
    const worktreeDir = await createTempDir("git-backup-worktree-");
    const cloneParentDir = await createTempDir("git-backup-clone-");
    const cloneDir = join(cloneParentDir, "repo");
    const server = await createSyncServer({
      authToken: "test-token",
      dataDir,
    });

    try {
      const metaDoc = await server.hocuspocus.createDocument(
        META_DOCUMENT_NAME,
        {},
        "backup-test-meta",
        TEST_CONNECTION_CONFIG,
      );
      const contentDoc = await server.hocuspocus.createDocument(
        CONTENT_DOCUMENT_NAME,
        {},
        "backup-test-content",
        TEST_CONNECTION_CONFIG,
      );

      metaDoc.transact(() => {
        initializeMetadataDoc(metaDoc);
        applyMetadataOp(metaDoc, {
          type: "file.create",
          fileId: "dir-1",
          path: "Notes",
          kind: "directory",
        });
        applyMetadataOp(metaDoc, {
          type: "file.create",
          fileId: "text-1",
          path: "Notes/hello.md",
          kind: "text",
        });
        applyMetadataOp(metaDoc, {
          type: "file.create",
          fileId: "bin-1",
          path: "Notes/photo.bin",
          kind: "binary",
        });
        applyMetadataOp(metaDoc, {
          type: "file.create",
          fileId: "ignored-1",
          path: ".obsidian/workspace.json",
          kind: "text",
        });
        applyMetadataOp(metaDoc, {
          type: "file.create",
          fileId: "ignored-2",
          path: ".DS_Store",
          kind: "text",
        });
        applyMetadataOp(metaDoc, {
          type: "file.create",
          fileId: "ignored-3",
          path: "Notes/hello.md.orig",
          kind: "text",
        });
        applyMetadataOp(metaDoc, {
          type: "file.create",
          fileId: "ignored-4",
          path: ".gitignore",
          kind: "text",
        });
      }, "test");

      const contentFiles = contentDoc.getMap<
        Y.Text | Uint8Array | Y.Array<Uint8Array>
      >("files");
      contentDoc.transact(() => {
        const text = new Y.Text();
        text.insert(0, "# Hello\n");
        contentFiles.set("text-1", text);

        const binary = new Y.Array<Uint8Array>();
        binary.push([new Uint8Array([1, 2, 3, 4])]);
        contentFiles.set("bin-1", binary);

        const ignored = new Y.Text();
        ignored.insert(0, '{"active":"ignored"}');
        contentFiles.set("ignored-1", ignored);

        const ignoredMac = new Y.Text();
        ignoredMac.insert(0, "ignored");
        contentFiles.set("ignored-2", ignoredMac);

        const ignoredGit = new Y.Text();
        ignoredGit.insert(0, "backup");
        contentFiles.set("ignored-3", ignoredGit);

        const ignoredReserved = new Y.Text();
        ignoredReserved.insert(0, "vault gitignore");
        contentFiles.set("ignored-4", ignoredReserved);
      }, "test");

      const liveDatabase = new BunSQLite(join(dataDir, "sync.db"));
      try {
        liveDatabase.run('DELETE FROM "documents"');
      } finally {
        liveDatabase.close();
      }

      const initRemote = await git(remoteDir, ["init", "--bare"]);
      expect(initRemote.exitCode).toBe(0);

      const config: GitBackupConfig = {
        dataDir,
        remoteUrl: remoteDir,
        branch: "main",
        worktreeDir,
        authorName: "backup-bot",
        authorEmail: "backup@example.com",
        intervalMs: 60_000,
      };

      expect(await runGitBackupOnce(config, server)).toBe("committed");
      const storedToken = await git(worktreeDir, [
        "config",
        "--local",
        "--get",
        "obsidian-crdt-sync.backuptoken",
      ]);
      expect(storedToken.exitCode).toBe(0);
      expect(storedToken.stdout.length).toBeGreaterThan(0);
      expect(await runGitBackupOnce(config, server)).toBe("noop");
      metaDoc.transact(() => {
        applyMetadataOp(metaDoc, {
          type: "file.delete",
          fileId: "bin-1",
          path: "Notes/photo.bin",
        });
      }, "test");
      contentDoc.transact(() => {
        (contentFiles.get("text-1") as Y.Text | undefined)?.insert(
          8,
          "updated\n",
        );
      }, "test");
      expect(await runGitBackupOnce(config, server)).toBe("committed");

      const cloneResult = await git(cloneParentDir, [
        "clone",
        "--branch",
        "main",
        "--single-branch",
        remoteDir,
        cloneDir,
      ]);
      expect(cloneResult.exitCode).toBe(0);

      const commitCount = await git(cloneDir, ["rev-list", "--count", "HEAD"]);
      expect(commitCount.stdout).toBe("2");

      expect(await Bun.file(join(cloneDir, "Notes/hello.md")).text()).toBe(
        "# Hello\nupdated\n",
      );
      expect(await Bun.file(join(cloneDir, "Notes/photo.bin")).exists()).toBe(
        false,
      );
      expect(await Bun.file(join(cloneDir, ".obsidian")).exists()).toBe(false);
      expect(await Bun.file(join(cloneDir, ".DS_Store")).exists()).toBe(false);
      expect(
        await Bun.file(join(cloneDir, "Notes/hello.md.orig")).exists(),
      ).toBe(false);
      expect(await Bun.file(join(cloneDir, ".gitignore")).text()).toBe(
        BACKUP_GITIGNORE,
      );
      expect(await Bun.file(join(cloneDir, "sync.db")).exists()).toBe(false);
    } finally {
      await server.destroy();
      await Bun.sleep(100);
    }
  });
});
