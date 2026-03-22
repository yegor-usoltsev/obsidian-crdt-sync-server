import { beforeEach, describe, expect, it } from "bun:test";
import {
  clearRateLimits,
  isRateLimited,
  recordAuthFailure,
  validateTokenConfig,
  verifyToken,
} from "../../src/transport/auth";

describe("auth", () => {
  beforeEach(() => {
    clearRateLimits();
  });

  describe("validateTokenConfig", () => {
    it("rejects empty token", () => {
      expect(validateTokenConfig("")).not.toBeNull();
    });

    it("rejects token shorter than 32 chars", () => {
      expect(validateTokenConfig("short")).not.toBeNull();
      expect(validateTokenConfig("a".repeat(31))).not.toBeNull();
    });

    it("accepts token of 32+ chars", () => {
      expect(validateTokenConfig("a".repeat(32))).toBeNull();
      expect(validateTokenConfig("a".repeat(64))).toBeNull();
    });

    it("rejects token longer than 1024 chars", () => {
      expect(validateTokenConfig("a".repeat(1025))).not.toBeNull();
    });
  });

  describe("verifyToken", () => {
    const token = "a".repeat(32);

    it("returns true for matching tokens", () => {
      expect(verifyToken(token, token)).toBe(true);
    });

    it("returns false for non-matching tokens", () => {
      expect(verifyToken(token, "b".repeat(32))).toBe(false);
    });

    it("returns false for short provided token", () => {
      expect(verifyToken("short", token)).toBe(false);
    });

    it("returns false for different length tokens", () => {
      expect(verifyToken("a".repeat(32), "a".repeat(33))).toBe(false);
    });

    it("uses constant-time comparison", () => {
      // Timing-safe equal is used internally; we just verify correctness
      const t1 = "x".repeat(64);
      const t2 = "x".repeat(64);
      expect(verifyToken(t1, t2)).toBe(true);
    });
  });

  describe("rate limiting", () => {
    it("does not rate limit on first attempt", () => {
      expect(isRateLimited("192.168.1.1")).toBe(false);
    });

    it("rate limits after too many failures", () => {
      const source = "10.0.0.1";
      for (let i = 0; i < 10; i++) {
        recordAuthFailure(source);
      }
      expect(isRateLimited(source)).toBe(true);
    });

    it("does not rate limit below threshold", () => {
      const source = "10.0.0.2";
      for (let i = 0; i < 5; i++) {
        recordAuthFailure(source);
      }
      expect(isRateLimited(source)).toBe(false);
    });
  });
});
