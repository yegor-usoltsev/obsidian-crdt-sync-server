import { createGitBackupJob, readGitBackupConfig } from "./git-backup.ts";
import { log } from "./log.ts";
import { createSyncServer } from "./server.ts";

const port = Number(Bun.env.PORT ?? 3000);
const authToken = Bun.env.AUTH_TOKEN;
const dataDir = Bun.env.DATA_DIR ?? "./data/db";

const MIN_AUTH_TOKEN_LENGTH = 32;

if (!authToken) {
  log("error", "AUTH_TOKEN environment variable is required");
  process.exit(1);
}

if (authToken.length < MIN_AUTH_TOKEN_LENGTH) {
  log(
    "error",
    "AUTH_TOKEN is too short — use at least 32 characters of randomness",
    {
      length: authToken.length,
      minimum: MIN_AUTH_TOKEN_LENGTH,
    },
  );
  process.exit(1);
}

const gitBackupConfig = readGitBackupConfig(Bun.env, dataDir);
const server = await createSyncServer({
  authToken,
  dataDir,
});
const gitBackup = gitBackupConfig
  ? createGitBackupJob(gitBackupConfig, server)
  : null;

async function shutdown(signal: string): Promise<void> {
  log("info", "Shutting down", { signal });
  try {
    await Promise.all([server.destroy(), gitBackup?.stop()]);
  } catch (err) {
    log("error", "Error during shutdown", { error: String(err) });
  }
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));

process.on("unhandledRejection", (reason: unknown) => {
  log("error", "Unhandled promise rejection", { reason: String(reason) });
});

await server.listen(port);
log("info", "Server started", {
  port,
  dataDir,
  gitBackupEnabled: gitBackupConfig !== null,
});
gitBackup?.start();
