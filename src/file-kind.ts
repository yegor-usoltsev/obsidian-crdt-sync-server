export type FileKind = "text" | "binary" | "directory";

export function coerceFileKind(value: unknown): FileKind | undefined {
  return value === "text" || value === "binary" || value === "directory"
    ? value
    : undefined;
}
