import { Database as BunSQLite } from "bun:sqlite";
import type { Server } from "@hocuspocus/server";
import { $ } from "bun";
import { basename, dirname, isAbsolute, join, normalize, resolve } from "pathe";
import * as Y from "yjs";
import { coerceFileKind } from "./file-kind.ts";
import { log } from "./log.ts";
import { CONTENT_DOCUMENT_NAME, META_DOCUMENT_NAME } from "./meta-doc.ts";
import {
  isIgnoredSyncPath,
  SYNC_IGNORE_LIST,
  SYNC_IGNORE_RESERVED_PATH,
} from "./sync-ignore.ts";

export interface GitBackupConfig {
  dataDir: string;
  remoteUrl: string;
  branch: string;
  worktreeDir: string;
  authorName: string;
  authorEmail: string;
  intervalMs: number;
  username?: string;
  password?: string;
}

export interface GitBackupJob {
  start: () => void;
  stop: () => Promise<void>;
}

const DEFAULT_BACKUP_BRANCH = "main";
const DEFAULT_BACKUP_AUTHOR_NAME = "obsidian-crdt-sync-server";
const DEFAULT_BACKUP_AUTHOR_EMAIL = "backup@localhost";
const REMOTE_NAME = "backup";
const SQLITE_DOCUMENTS_QUERY = `SELECT data FROM "documents" WHERE name = ?`;
const BACKUP_GITIGNORE_PATH = SYNC_IGNORE_RESERVED_PATH;
const BACKUP_TOKEN_VERSION = "vault-tree-v1";
const BACKUP_TOKEN_GIT_CONFIG_KEY = "obsidian-crdt-sync.backuptoken";

export const BACKUP_GITIGNORE = SYNC_IGNORE_LIST;

function hashBytes(bytes: Uint8Array | string): string {
  return new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
}

function readRequiredEnv(
  env: Record<string, string | undefined>,
  key: string,
): string {
  const value = env[key]?.trim();
  if (value) {
    return value;
  }
  throw new Error(
    `${key} environment variable is required when backup is enabled`,
  );
}

export function readGitBackupConfig(
  env: Record<string, string | undefined>,
  dataDir: string,
): GitBackupConfig | null {
  const intervalRaw = env.BACKUP_GIT_INTERVAL_MINUTES?.trim();
  if (!intervalRaw) {
    return null;
  }

  const intervalMinutes = Number(intervalRaw);
  if (!Number.isFinite(intervalMinutes) || intervalMinutes <= 0) {
    throw new Error("BACKUP_GIT_INTERVAL_MINUTES must be a positive number");
  }

  const remoteUrl = readRequiredEnv(env, "BACKUP_GIT_URL");
  let parsedRemoteUrl: URL;
  try {
    parsedRemoteUrl = new URL(remoteUrl);
  } catch {
    throw new Error("BACKUP_GIT_URL must be a valid URL");
  }
  if (parsedRemoteUrl.protocol !== "https:") {
    throw new Error("BACKUP_GIT_URL must use https://");
  }
  if (parsedRemoteUrl.username || parsedRemoteUrl.password) {
    throw new Error(
      "BACKUP_GIT_URL must not include embedded credentials; use BACKUP_GIT_USERNAME and BACKUP_GIT_PASSWORD",
    );
  }

  const resolvedDataDir = resolve(dataDir);
  const worktreeDir =
    env.BACKUP_GIT_WORKTREE_DIR?.trim() ||
    join(dirname(resolvedDataDir), `${basename(resolvedDataDir)}-git-backup`);
  const resolvedWorktreeDir = resolve(worktreeDir);
  if (
    resolvedDataDir === resolvedWorktreeDir ||
    resolvedDataDir.startsWith(`${resolvedWorktreeDir}/`) ||
    resolvedWorktreeDir.startsWith(`${resolvedDataDir}/`)
  ) {
    throw new Error(
      "BACKUP_GIT_WORKTREE_DIR must stay outside the live DATA_DIR",
    );
  }

  return {
    dataDir,
    remoteUrl,
    branch: env.BACKUP_GIT_BRANCH?.trim() || DEFAULT_BACKUP_BRANCH,
    worktreeDir,
    authorName:
      env.BACKUP_GIT_AUTHOR_NAME?.trim() || DEFAULT_BACKUP_AUTHOR_NAME,
    authorEmail:
      env.BACKUP_GIT_AUTHOR_EMAIL?.trim() || DEFAULT_BACKUP_AUTHOR_EMAIL,
    intervalMs: intervalMinutes * 60 * 1000,
    username: readRequiredEnv(env, "BACKUP_GIT_USERNAME"),
    password: readRequiredEnv(env, "BACKUP_GIT_PASSWORD"),
  };
}

function sanitizeGitOutput(output: string, authHeader: string | null): string {
  if (!authHeader || !output) {
    return output;
  }
  return output.replaceAll(authHeader, "[REDACTED]");
}

async function runGit(
  cwd: string,
  args: string[],
  config: GitBackupConfig,
  allowFailure = false,
): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const authHeader =
    config.username && config.password
      ? btoa(`${config.username}:${config.password}`)
      : null;
  const command = authHeader
    ? [
        "-c",
        "credential.helper=",
        "-c",
        `http.extraHeader=Authorization: Basic ${authHeader}`,
        ...args,
      ]
    : args;
  const result = await $`git ${command}`
    .cwd(cwd)
    .env({
      ...Bun.env,
      GIT_TERMINAL_PROMPT: "0",
    })
    .quiet()
    .nothrow();
  const stdout = sanitizeGitOutput(result.stdout.toString().trim(), authHeader);
  const stderr = sanitizeGitOutput(result.stderr.toString().trim(), authHeader);
  if (!allowFailure && result.exitCode !== 0) {
    throw new Error(stderr || stdout || `git ${args[0]} failed`);
  }
  return {
    exitCode: result.exitCode,
    stdout,
    stderr,
  };
}

async function ensureRemote(config: GitBackupConfig): Promise<void> {
  const existingRemote = await runGit(
    config.worktreeDir,
    ["remote", "get-url", REMOTE_NAME],
    config,
    true,
  );
  if (existingRemote.exitCode !== 0) {
    await runGit(
      config.worktreeDir,
      ["remote", "add", REMOTE_NAME, config.remoteUrl],
      config,
    );
    return;
  }
  if (existingRemote.stdout !== config.remoteUrl) {
    await runGit(
      config.worktreeDir,
      ["remote", "set-url", REMOTE_NAME, config.remoteUrl],
      config,
    );
  }
}

async function ensureBranch(config: GitBackupConfig): Promise<void> {
  const hasHead = await runGit(
    config.worktreeDir,
    ["rev-parse", "--verify", "HEAD"],
    config,
    true,
  );
  const remoteBranch = await runGit(
    config.worktreeDir,
    ["ls-remote", "--exit-code", "--heads", REMOTE_NAME, config.branch],
    config,
    true,
  );

  if (remoteBranch.exitCode !== 0 && remoteBranch.exitCode !== 2) {
    throw new Error(remoteBranch.stderr || "Failed to inspect remote branch");
  }

  const currentBranch = await runGit(
    config.worktreeDir,
    ["branch", "--show-current"],
    config,
    true,
  );
  if (remoteBranch.exitCode === 0) {
    await runGit(
      config.worktreeDir,
      ["fetch", REMOTE_NAME, config.branch],
      config,
    );
    if (hasHead.exitCode !== 0 || currentBranch.stdout !== config.branch) {
      const localBranch = await runGit(
        config.worktreeDir,
        ["show-ref", "--verify", `refs/heads/${config.branch}`],
        config,
        true,
      );
      if (localBranch.exitCode === 0) {
        await runGit(config.worktreeDir, ["switch", config.branch], config);
      } else {
        await runGit(
          config.worktreeDir,
          [
            "checkout",
            "--track",
            "-b",
            config.branch,
            `${REMOTE_NAME}/${config.branch}`,
          ],
          config,
        );
      }
    }

    const dirty = await runGit(
      config.worktreeDir,
      ["status", "--short"],
      config,
      true,
    );
    if (dirty.stdout.length === 0) {
      await runGit(
        config.worktreeDir,
        ["pull", "--rebase", REMOTE_NAME, config.branch],
        config,
      );
    } else {
      log("warn", "Skipping git pull because backup worktree is dirty", {
        worktreeDir: config.worktreeDir,
      });
    }
    return;
  }

  if (hasHead.exitCode !== 0 && currentBranch.stdout !== config.branch) {
    await runGit(
      config.worktreeDir,
      ["switch", "--create", config.branch],
      config,
    );
    return;
  }

  if (hasHead.exitCode === 0 && currentBranch.stdout !== config.branch) {
    const localBranch = await runGit(
      config.worktreeDir,
      ["show-ref", "--verify", `refs/heads/${config.branch}`],
      config,
      true,
    );
    if (localBranch.exitCode === 0) {
      await runGit(config.worktreeDir, ["switch", config.branch], config);
    } else {
      await runGit(
        config.worktreeDir,
        ["switch", "--create", config.branch],
        config,
      );
    }
  }
}

function cloneDoc(document: Y.Doc): Y.Doc {
  const clone = new Y.Doc();
  Y.applyUpdate(clone, Y.encodeStateAsUpdate(document));
  return clone;
}

function normalizeVaultPath(path: string): string {
  return normalize(path).split("\\").join("/");
}

function isContainedPath(basePath: string, targetPath: string): boolean {
  const resolvedTarget = resolve(targetPath);
  const resolvedBase = resolve(basePath);
  return (
    resolvedTarget === resolvedBase ||
    resolvedTarget.startsWith(`${resolvedBase}/`)
  );
}

function isSafeVaultPath(path: string): boolean {
  if (isAbsolute(path) || path.startsWith("/")) {
    return false;
  }
  return path.split("/").every((segment) => {
    return segment !== ".." && segment !== "." && segment.length > 0;
  });
}

function loadPersistedDoc(dataDir: string, documentName: string): Y.Doc {
  const sqlite = new BunSQLite(join(dataDir, "sync.db"), { readonly: true });
  try {
    const row = sqlite.query(SQLITE_DOCUMENTS_QUERY).get(documentName) as {
      data?: Uint8Array;
    } | null;
    const document = new Y.Doc();
    if (row?.data) {
      Y.applyUpdate(document, new Uint8Array(row.data));
    }
    return document;
  } finally {
    sqlite.close();
  }
}

function loadBackupDoc(
  config: GitBackupConfig,
  server: Server | undefined,
  documentName: string,
): Y.Doc {
  const liveDocument = server?.hocuspocus.documents.get(documentName);
  return liveDocument
    ? cloneDoc(liveDocument)
    : loadPersistedDoc(config.dataDir, documentName);
}

function isIgnoredVaultPath(path: string, kind: "file" | "directory"): boolean {
  if (normalizeVaultPath(path) === BACKUP_GITIGNORE_PATH) {
    return true;
  }
  return isIgnoredSyncPath(path, kind);
}

async function clearWorktreeVault(worktreeDir: string): Promise<void> {
  for (const entry of new Bun.Glob("**/*").scanSync({
    cwd: worktreeDir,
    dot: true,
  })) {
    if (!entry.startsWith(".git/")) {
      await Bun.file(join(worktreeDir, entry)).delete();
    }
  }
}

async function writeBackupGitignore(worktreeDir: string): Promise<void> {
  await Bun.write(join(worktreeDir, BACKUP_GITIGNORE_PATH), BACKUP_GITIGNORE);
}

function joinBinaryChunks(chunks: Uint8Array[]): Uint8Array {
  if (chunks.length < 2) {
    return new Uint8Array(chunks[0] ?? []);
  }

  const merged = new Uint8Array(
    chunks.reduce((sum, chunk) => sum + chunk.length, 0),
  );
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.length;
  }
  return merged;
}

function readTextContent(
  files: Y.Map<Y.Text | Uint8Array | Y.Array<Uint8Array>>,
  fileId: string,
): string {
  const value = files.get(fileId);
  if (value === undefined) {
    return "";
  }
  if (value instanceof Y.Text) {
    return value.toString();
  }
  throw new Error(`fileId "${fileId}" is not stored as text content`);
}

function readBinaryContent(
  files: Y.Map<Y.Text | Uint8Array | Y.Array<Uint8Array>>,
  fileId: string,
): Uint8Array {
  const value = files.get(fileId);
  if (value === undefined) {
    return new Uint8Array();
  }
  if (value instanceof Uint8Array) {
    return new Uint8Array(value);
  }
  if (value instanceof Y.Array) {
    return joinBinaryChunks(value.toArray());
  }
  throw new Error(`fileId "${fileId}" is not stored as binary content`);
}

function computeDocToken(
  config: GitBackupConfig,
  server: Server | undefined,
  documentName: string,
): string {
  const document = loadBackupDoc(config, server, documentName);
  return hashBytes(Y.encodeStateVector(document));
}

function computeBackupToken(
  config: GitBackupConfig,
  server: Server | undefined,
): string {
  return [
    BACKUP_TOKEN_VERSION,
    hashBytes(BACKUP_GITIGNORE),
    computeDocToken(config, server, META_DOCUMENT_NAME),
    computeDocToken(config, server, CONTENT_DOCUMENT_NAME),
  ].join(":");
}

async function readStoredBackupToken(
  config: GitBackupConfig,
): Promise<string | null> {
  const result = await runGit(
    config.worktreeDir,
    ["config", "--local", "--get", BACKUP_TOKEN_GIT_CONFIG_KEY],
    config,
    true,
  );
  return result.exitCode === 0 && result.stdout ? result.stdout : null;
}

async function writeStoredBackupToken(
  config: GitBackupConfig,
  token: string,
): Promise<void> {
  await runGit(
    config.worktreeDir,
    ["config", "--local", BACKUP_TOKEN_GIT_CONFIG_KEY, token],
    config,
  );
}

async function isWorktreeClean(config: GitBackupConfig): Promise<boolean> {
  const status = await runGit(
    config.worktreeDir,
    ["status", "--short"],
    config,
    true,
  );
  if (status.exitCode !== 0) {
    throw new Error(
      status.stderr || "Failed to inspect backup worktree status",
    );
  }
  return status.stdout.length === 0;
}

async function materializeVault(
  config: GitBackupConfig,
  server?: Server,
): Promise<void> {
  const metaDoc = loadBackupDoc(config, server, META_DOCUMENT_NAME);
  const contentDoc = loadBackupDoc(config, server, CONTENT_DOCUMENT_NAME);
  const metaFiles = metaDoc.getMap<Y.Map<unknown>>("files");
  const contentFiles = contentDoc.getMap<
    Y.Text | Uint8Array | Y.Array<Uint8Array>
  >("files");
  const entries = Array.from(metaFiles.entries())
    .flatMap(([fileId, metadata]) => {
      const path = metadata.get("path");
      const kind = coerceFileKind(metadata.get("kind")) ?? "text";
      if (
        metadata.get("deleted") === true ||
        typeof path !== "string" ||
        isIgnoredVaultPath(path, kind === "directory" ? "directory" : "file")
      ) {
        return [];
      }

      const normalizedPath = normalizeVaultPath(path);
      if (!isSafeVaultPath(normalizedPath)) {
        log("warn", "Git backup skipping entry with unsafe vault path", {
          fileId,
          path: normalizedPath,
        });
        return [];
      }

      return [
        {
          fileId,
          path: normalizedPath,
          kind,
        },
      ];
    })
    .sort((left, right) => left.path.localeCompare(right.path));

  await clearWorktreeVault(config.worktreeDir);
  await writeBackupGitignore(config.worktreeDir);

  for (const entry of entries) {
    if (entry.kind === "directory") {
      continue;
    }

    const targetPath = join(config.worktreeDir, entry.path);
    if (!isContainedPath(config.worktreeDir, targetPath)) {
      log("warn", "Git backup skipping entry that escapes worktree", {
        fileId: entry.fileId,
        path: entry.path,
        resolvedTarget: resolve(targetPath),
        worktreeDir: config.worktreeDir,
      });
      continue;
    }

    if (entry.kind === "text") {
      await Bun.write(targetPath, readTextContent(contentFiles, entry.fileId));
      continue;
    }

    await Bun.write(targetPath, readBinaryContent(contentFiles, entry.fileId));
  }
}

export async function runGitBackupOnce(
  config: GitBackupConfig,
  server?: Server,
): Promise<"committed" | "noop"> {
  const marker = join(config.worktreeDir, `.bun-${Bun.randomUUIDv7()}`);
  await Bun.write(marker, "");
  await Bun.file(marker).delete();
  if (!(await Bun.file(join(config.worktreeDir, ".git", "HEAD")).exists())) {
    await runGit(
      config.worktreeDir,
      ["init", "--initial-branch", config.branch],
      config,
    );
  }

  const backupToken = computeBackupToken(config, server);
  if (
    (await readStoredBackupToken(config)) === backupToken &&
    (await isWorktreeClean(config))
  ) {
    return "noop";
  }

  await runGit(
    config.worktreeDir,
    ["config", "user.name", config.authorName],
    config,
  );
  await runGit(
    config.worktreeDir,
    ["config", "user.email", config.authorEmail],
    config,
  );
  await ensureRemote(config);
  await ensureBranch(config);

  await materializeVault(config, server);

  await runGit(config.worktreeDir, ["add", "--all"], config);
  const stagedDiff = await runGit(
    config.worktreeDir,
    ["diff", "--cached", "--quiet"],
    config,
    true,
  );
  if (stagedDiff.exitCode === 0) {
    await writeStoredBackupToken(config, backupToken);
    return "noop";
  }
  if (stagedDiff.exitCode !== 1) {
    throw new Error(
      stagedDiff.stderr || "Failed to inspect staged backup diff",
    );
  }

  await runGit(
    config.worktreeDir,
    ["commit", "-m", `backup: ${new Date().toISOString()}`],
    config,
  );
  await runGit(
    config.worktreeDir,
    ["push", "--set-upstream", REMOTE_NAME, config.branch],
    config,
  );
  await writeStoredBackupToken(config, backupToken);
  return "committed";
}

export function createGitBackupJob(
  config: GitBackupConfig,
  server: Server,
): GitBackupJob {
  let timer: ReturnType<typeof setTimeout> | null = null;
  let running: Promise<void> | null = null;
  let started = false;
  let stopped = false;

  const schedule = () => {
    if (stopped) {
      return;
    }
    timer = setTimeout(() => {
      timer = null;
      void run("scheduled");
    }, config.intervalMs);
  };

  const run = async (reason: "startup" | "scheduled") => {
    if (stopped || running) {
      return;
    }

    running = (async () => {
      try {
        const result = await runGitBackupOnce(config, server);
        log("info", "Git backup finished", {
          reason,
          result,
          branch: config.branch,
          worktreeDir: config.worktreeDir,
        });
      } catch (error) {
        log("error", "Git backup failed", {
          reason,
          branch: config.branch,
          worktreeDir: config.worktreeDir,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    })();

    try {
      await running;
    } finally {
      running = null;
      schedule();
    }
  };

  return {
    start() {
      if (started || stopped) {
        return;
      }
      started = true;
      log("info", "Git backup job enabled", {
        branch: config.branch,
        intervalMs: config.intervalMs,
        worktreeDir: config.worktreeDir,
      });
      void run("startup");
    },
    async stop() {
      stopped = true;
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      await running;
    },
  };
}
