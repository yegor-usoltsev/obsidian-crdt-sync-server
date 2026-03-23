import { describe, expect, it } from "bun:test";
import { validateWorktreeOverlap } from "../../src/backup/git-backup";

describe("git-backup", () => {
  describe("validateWorktreeOverlap", () => {
    it("rejects worktree under data dir", () => {
      const result = validateWorktreeOverlap("./data/git", "./data");
      expect(result).not.toBeNull();
      expect(result).toContain("overlaps");
    });

    it("rejects data dir under worktree", () => {
      const result = validateWorktreeOverlap("/backup", "/backup/data");
      expect(result).not.toBeNull();
      expect(result).toContain("overlaps");
    });

    it("rejects identical paths", () => {
      const result = validateWorktreeOverlap("./data", "./data");
      expect(result).not.toBeNull();
    });

    it("accepts non-overlapping paths", () => {
      const result = validateWorktreeOverlap("/backup/vault", "/srv/data");
      expect(result).toBeNull();
    });

    it("accepts sibling directories", () => {
      const result = validateWorktreeOverlap("./backup", "./data");
      expect(result).toBeNull();
    });
  });
});
