/**
 * Optional scheduled Git backup: canonical vault export with
 * worktree safety and redundant-backup skipping.
 */

import type { Database } from "bun:sqlite";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { $ } from "bun";
import type { MetadataRegistry } from "../metadata-registry/registry";
import { log } from "../shared/log";

export interface GitBackupConfig {
  /** Path to the Git backup worktree (must be outside data dir). */
  worktreePath: string;
  /** Remote URL for push (HTTPS with credentials). */
  remoteUrl?: string;
  /** Branch to push to. */
  branch: string;
  /** Backup interval in milliseconds. */
  intervalMs: number;
  /** Committer name. */
  committerName: string;
  /** Committer email. */
  committerEmail: string;
}

export interface GitBackupJob {
  start(): void;
  stop(): void;
  runNow(): Promise<void>;
}

export function readGitBackupConfig(): GitBackupConfig | null {
  const intervalStr = process.env.BACKUP_GIT_INTERVAL_MINUTES;
  if (!intervalStr) return null;

  const intervalMin = Number(intervalStr);
  if (!Number.isFinite(intervalMin) || intervalMin <= 0) return null;

  const worktree = process.env.BACKUP_GIT_WORKTREE_DIR ?? "./data/git";
  const remote = process.env.BACKUP_GIT_URL;

  // Build authenticated remote URL if provided
  let remoteUrl: string | undefined;
  if (remote) {
    try {
      const url = new URL(remote);
      if (!["https:", "http:"].includes(url.protocol)) {
        log("warn", "Git backup remote must use https:// or http://", {
          remote,
        });
        return null;
      }
      const username = process.env.BACKUP_GIT_USERNAME;
      const password = process.env.BACKUP_GIT_PASSWORD;
      if (username) url.username = username;
      if (password) url.password = password;
      remoteUrl = url.toString();
    } catch {
      log("warn", "Invalid Git backup remote URL", { remote });
      return null;
    }
  }

  return {
    worktreePath: worktree,
    remoteUrl,
    branch: process.env.BACKUP_GIT_BRANCH ?? "main",
    intervalMs: intervalMin * 60 * 1000,
    committerName: process.env.BACKUP_GIT_AUTHOR_NAME ?? "Obsidian Sync",
    committerEmail:
      process.env.BACKUP_GIT_AUTHOR_EMAIL ?? "obsidian-sync@localhost",
  };
}

export function createGitBackupJob(
  config: GitBackupConfig,
  registry: MetadataRegistry,
  _db: Database,
  _dataDir: string,
): GitBackupJob {
  let timer: ReturnType<typeof setInterval> | null = null;
  let lastTreeHash: string | null = null;

  async function runBackup(): Promise<void> {
    log("info", "Starting Git backup");

    try {
      const worktree = config.worktreePath;
      await mkdir(worktree, { recursive: true });

      // Initialize git repo if needed
      try {
        await $`git -C ${worktree} status`.quiet();
      } catch {
        await $`git init ${worktree}`.quiet();
        await $`git -C ${worktree} config user.name ${config.committerName}`.quiet();
        await $`git -C ${worktree} config user.email ${config.committerEmail}`.quiet();
      }

      // Export canonical vault tree
      const activeFiles = registry.listActiveFiles();

      // Write managed .gitignore
      const gitignore = [
        "# Managed by CRDT Sync backup",
        ".DS_Store",
        "Thumbs.db",
        "*.tmp",
        "",
      ].join("\n");
      await writeFile(join(worktree, ".gitignore"), gitignore);

      // Compute tree fingerprint to skip redundant backups
      const treeFingerprint = activeFiles
        .map((f) => `${f.path}:${f.contentDigest ?? ""}:${f.contentAnchor}`)
        .sort()
        .join("\n");

      const hasher = new Bun.CryptoHasher("sha256");
      hasher.update(treeFingerprint);
      const treeHash = hasher.digest("hex");

      if (treeHash === lastTreeHash) {
        log("info", "Git backup skipped: no changes since last backup");
        return;
      }

      // TODO: Materialize file content from blob store and text doc service
      // For now, just stage and commit metadata

      await $`git -C ${worktree} add -A`.quiet();

      const status = await $`git -C ${worktree} status --porcelain`.text();
      if (!status.trim()) {
        log("info", "Git backup: nothing to commit");
        lastTreeHash = treeHash;
        return;
      }

      await $`git -C ${worktree} commit -m ${`Vault backup ${new Date().toISOString()}`}`.quiet();
      lastTreeHash = treeHash;

      // Push if remote configured
      if (config.remoteUrl) {
        try {
          await $`git -C ${worktree} remote set-url origin ${config.remoteUrl}`.quiet();
        } catch {
          await $`git -C ${worktree} remote add origin ${config.remoteUrl}`.quiet();
        }
        await $`git -C ${worktree} push origin HEAD:${config.branch}`.quiet();
      }

      log("info", "Git backup complete", { files: activeFiles.length });
    } catch (err) {
      log("error", "Git backup failed", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  return {
    start() {
      // Run on startup
      runBackup();
      timer = setInterval(runBackup, config.intervalMs);
      log("info", "Git backup scheduled", {
        intervalMin: config.intervalMs / 60_000,
      });
    },

    stop() {
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
    },

    runNow: runBackup,
  };
}
