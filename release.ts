import { $ } from "bun";

type ReleaseType = "major" | "minor" | "patch";

function parseReleaseType(value: string | undefined): ReleaseType {
  if (value === "major" || value === "minor" || value === "patch") {
    return value;
  }
  throw new Error("Usage: bun run release.ts [major|minor|patch]");
}

async function gitOutput(
  args: string[],
  allowFailure = false,
): Promise<string> {
  const result = await $`git ${args}`.quiet().nothrow();
  const stdout = result.stdout.toString().trim();
  const stderr = result.stderr.toString().trim();
  if (!allowFailure && result.exitCode !== 0) {
    throw new Error(stderr || stdout || `git ${args.join(" ")} failed`);
  }
  return stdout;
}

function parseVersion(tag: string): [number, number, number] {
  const match = /^v(\d+)\.(\d+)\.(\d+)$/.exec(tag);
  if (!match) {
    throw new Error(`Unsupported tag format: ${tag}`);
  }
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

function bumpVersion(
  [major, minor, patch]: [number, number, number],
  type: ReleaseType,
): string {
  if (type === "major") {
    return `${major + 1}.0.0`;
  }
  if (type === "minor") {
    return `${major}.${minor + 1}.0`;
  }
  return `${major}.${minor}.${patch + 1}`;
}

async function main() {
  const releaseType = parseReleaseType(process.argv[2]);
  const branch = await gitOutput(["rev-parse", "--abbrev-ref", "HEAD"]);
  if (branch !== "main") {
    throw new Error("Release script must be run from the main branch");
  }

  const status = await gitOutput(["status", "--short"], true);
  if (status.length > 0) {
    throw new Error("Working tree must be clean before creating a release");
  }

  await gitOutput(["fetch", "--tags", "origin"]);
  await gitOutput(["pull", "--ff-only", "origin", "main"]);

  const latestTag =
    (await gitOutput(["tag", "-l", "v*", "--sort=-v:refname"], true))
      .split("\n")
      .find(Boolean) ?? "v0.0.0";
  const nextVersion = bumpVersion(parseVersion(latestTag), releaseType);
  const nextTag = `v${nextVersion}`;

  await gitOutput(["tag", nextTag]);
  await gitOutput(["push", "origin", nextTag]);

  console.log(`Released ${nextTag}`);
}

await main();
