type LogLevel = "info" | "warn" | "error";

export function log(
  level: LogLevel,
  message: string,
  context?: Record<string, unknown>,
): void {
  const entry = JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    message,
    ...Object.fromEntries(
      Object.entries(context ?? {}).filter(([, value]) => value !== undefined),
    ),
  });
  (level === "error"
    ? console.error
    : level === "warn"
      ? console.warn
      : console.log)(entry);
}
