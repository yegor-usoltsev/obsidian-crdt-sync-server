import { isAbsolute, normalize } from "pathe";
import * as v from "valibot";
import type { FileKind } from "./file-kind.ts";
import { coerceFileKind } from "./file-kind.ts";
import type { AppliedMetadataOp } from "./meta-doc.ts";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;
const requestTypes = ["file.create", "file.rename", "file.delete"] as const;
const requiredString = (fieldName: string) =>
  v.pipe(v.string(), v.nonEmpty(`${fieldName} must be a non-empty string`));
const fileKind = (fieldName: string) =>
  v.picklist(
    ["text", "binary", "directory"] satisfies FileKind[],
    `${fieldName} must be "text", "binary", or "directory"`,
  );

const vaultPath = (fieldName: string) =>
  v.pipe(
    requiredString(fieldName),
    v.transform((path) => normalize(path).split("\\").join("/")),
    v.check(
      (path) => !isAbsolute(path) && !path.startsWith("/"),
      `${fieldName} must be a relative path`,
    ),
    v.check((path) => {
      return path.split("/").every((segment) => {
        return segment !== ".." && segment !== "." && segment.length > 0;
      });
    }, `${fieldName} must not contain path traversal or empty segments`),
  );

const baseRequest = {
  clientId: v.optional(requiredString("clientId")),
  clientOpId: v.optional(requiredString("clientOpId")),
  fileId: requiredString("fileId"),
  timestamp: v.optional(
    v.pipe(v.number(), v.finite(), v.transform(Math.trunc)),
  ),
};

const metadataRequestSchema = v.variant("type", [
  v.object({
    ...baseRequest,
    type: v.literal("file.create"),
    path: vaultPath("path"),
    kind: fileKind("kind"),
  }),
  v.object({
    ...baseRequest,
    type: v.literal("file.rename"),
    oldPath: v.optional(vaultPath("oldPath")),
    newPath: vaultPath("newPath"),
    path: v.optional(vaultPath("path")),
    kind: fileKind("kind"),
  }),
  v.object({
    ...baseRequest,
    type: v.literal("file.delete"),
    path: v.optional(vaultPath("path")),
  }),
]);

export type MetadataOpRequest = v.InferOutput<typeof metadataRequestSchema>;

export function normalizeMetadataOpRequest(value: unknown): MetadataOpRequest {
  if (!isRecord(value) || typeof value.type !== "string") {
    throw new Error("Metadata op request must be an object with a type");
  }
  if (!requestTypes.includes(value.type as (typeof requestTypes)[number])) {
    throw new Error(`Unsupported metadata op type "${value.type}"`);
  }

  const parsed = v.safeParse(metadataRequestSchema, value);
  if (!parsed.success) {
    throw new Error(parsed.issues[0]?.message ?? "Invalid metadata op request");
  }

  return parsed.output;
}

export function buildMetadataCommitMessage(
  request: MetadataOpRequest,
  applied: AppliedMetadataOp,
): Record<string, unknown> {
  return {
    type: "metadata.commit",
    requestType: applied.requestType,
    clientId: request.clientId,
    clientOpId: request.clientOpId,
    fileId: request.fileId,
    metaEventId: applied.eventId,
    path: applied.currentPath,
    kind:
      applied.currentKind ??
      (request.type === "file.delete" ? undefined : request.kind),
    oldPath: applied.oldPath,
    newPath: applied.newPath,
    timestamp: request.timestamp,
    deduplicated: applied.deduplicated === true,
  };
}

export function buildMetadataRejectMessage(
  request: MetadataOpRequest,
  current: { path?: string; kind?: FileKind } | undefined,
  error: unknown,
): Record<string, unknown> {
  return {
    type: "metadata.reject",
    requestType: request.type,
    clientId: request.clientId,
    clientOpId: request.clientOpId,
    fileId: request.fileId,
    path: request.type === "file.rename" ? request.newPath : request.path,
    kind:
      current?.kind ??
      (request.type === "file.delete" ? undefined : request.kind),
    oldPath: request.type === "file.rename" ? request.oldPath : undefined,
    newPath: request.type === "file.rename" ? request.newPath : undefined,
    currentPath: current?.path,
    reason: error instanceof Error ? error.message : String(error),
    timestamp: Date.now(),
  };
}

export function buildMetadataRejectMessageFromRaw(
  value: unknown,
  reason: string,
): Record<string, unknown> | null {
  if (!isRecord(value)) {
    return null;
  }

  const clientId =
    typeof value.clientId === "string" && value.clientId.length > 0
      ? value.clientId
      : undefined;
  const clientOpId =
    typeof value.clientOpId === "string" && value.clientOpId.length > 0
      ? value.clientOpId
      : undefined;
  if (!clientId || !clientOpId) {
    return null;
  }

  return {
    type: "metadata.reject",
    requestType: value.type,
    clientId,
    clientOpId,
    fileId: value.fileId,
    kind: coerceFileKind(value.kind),
    reason,
    timestamp: Date.now(),
  };
}
