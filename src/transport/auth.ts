/**
 * Authentication: token validation, constant-time comparison, rate limiting.
 */

import { timingSafeEqual } from "node:crypto";
import { log } from "../shared/log";

const MIN_TOKEN_LENGTH = 32;
const MAX_TOKEN_LENGTH = 1024;

/** Rate limiter for auth failures per source. */
const authFailures = new Map<string, { count: number; lastAttempt: number }>();
const RATE_LIMIT_WINDOW_MS = 60_000;
const MAX_FAILURES_PER_WINDOW = 10;
const BLOCK_DURATION_MS = 300_000; // 5 minutes after exceeding limit

/**
 * Validate a configured auth token meets minimum security requirements.
 */
export function validateTokenConfig(token: string): string | null {
  if (!token) return "Auth token is required";
  if (token.length < MIN_TOKEN_LENGTH)
    return `Auth token must be at least ${MIN_TOKEN_LENGTH} characters`;
  if (token.length > MAX_TOKEN_LENGTH)
    return `Auth token must be at most ${MAX_TOKEN_LENGTH} characters`;
  return null;
}

/**
 * Constant-time token comparison to prevent timing attacks.
 */
export function verifyToken(provided: string, expected: string): boolean {
  if (provided.length < MIN_TOKEN_LENGTH) return false;
  if (expected.length < MIN_TOKEN_LENGTH) return false;

  const a = Buffer.from(provided);
  const b = Buffer.from(expected);

  // Pad shorter buffer to prevent length leaking
  if (a.length !== b.length) {
    const padded = Buffer.alloc(Math.max(a.length, b.length));
    a.copy(padded);
    // Always do the comparison to keep timing constant
    timingSafeEqual(padded, padded);
    return false;
  }

  return timingSafeEqual(a, b);
}

/**
 * Check if a source is rate-limited due to auth failures.
 */
export function isRateLimited(source: string): boolean {
  const entry = authFailures.get(source);
  if (!entry) return false;

  const now = Date.now();

  // Check if blocked
  if (entry.count >= MAX_FAILURES_PER_WINDOW) {
    if (now - entry.lastAttempt < BLOCK_DURATION_MS) return true;
    // Reset after block duration
    authFailures.delete(source);
    return false;
  }

  // Clean up old entries
  if (now - entry.lastAttempt > RATE_LIMIT_WINDOW_MS) {
    authFailures.delete(source);
    return false;
  }

  return false;
}

/**
 * Record an auth failure for rate limiting.
 */
export function recordAuthFailure(source: string): void {
  const now = Date.now();
  const entry = authFailures.get(source);

  if (!entry || now - entry.lastAttempt > RATE_LIMIT_WINDOW_MS) {
    authFailures.set(source, { count: 1, lastAttempt: now });
  } else {
    entry.count++;
    entry.lastAttempt = now;
  }

  const current = authFailures.get(source)!;
  if (current.count >= MAX_FAILURES_PER_WINDOW) {
    log("warn", "Auth rate limit reached", { source, count: current.count });
  }
}

/** Clear rate limit state (for testing). */
export function clearRateLimits(): void {
  authFailures.clear();
}
