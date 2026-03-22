/**
 * Control-plane protocol message types and validation.
 */

import * as v from "valibot";

/** Maximum metadata request payload size. */
export const METADATA_PAYLOAD_LIMIT = 256 * 1024; // 256 KiB

/** Maximum content payload per file. */
export const CONTENT_PAYLOAD_LIMIT = 200 * 1024 * 1024; // 200 MiB

// --- Message types ---

export const MetadataIntentSchema = v.object({
  type: v.picklist(["create", "rename", "move", "delete"]),
  clientId: v.string(),
  operationId: v.string(),
  fileId: v.optional(v.string()),
  path: v.optional(v.string()),
  newPath: v.optional(v.string()),
  kind: v.optional(v.picklist(["text", "binary", "directory"])),
  contentAnchor: v.optional(v.number()),
  contentDigest: v.optional(v.string()),
});

export const ControlMessageSchema = v.variant("action", [
  v.object({
    action: v.literal("metadata.intent"),
    payload: MetadataIntentSchema,
  }),
  v.object({
    action: v.literal("metadata.subscribe"),
    sinceRevision: v.optional(v.number()),
  }),
  v.object({
    action: v.literal("history.list"),
    fileId: v.string(),
  }),
  v.object({
    action: v.literal("history.restore"),
    fileId: v.string(),
    historyEntryId: v.number(),
  }),
  v.object({
    action: v.literal("diagnostics.request"),
  }),
  v.object({
    action: v.literal("ping"),
  }),
]);

export type ControlMessage = v.InferOutput<typeof ControlMessageSchema>;

// --- Response types ---

export interface MetadataCommitResponse {
  action: "metadata.commit";
  payload: {
    operationId: string;
    fileId: string;
    path: string;
    kind: string;
    deleted: boolean;
    contentAnchor: number;
    revision: number;
    epoch: string;
  };
}

export interface MetadataRejectResponse {
  action: "metadata.reject";
  payload: {
    operationId: string;
    reason: string;
  };
}

export interface EpochChangeResponse {
  action: "epoch.change";
  payload: {
    epoch: string;
    revision: number;
  };
}

export interface PongResponse {
  action: "pong";
}

export interface ErrorResponse {
  action: "error";
  message: string;
}

export type ControlResponse =
  | MetadataCommitResponse
  | MetadataRejectResponse
  | EpochChangeResponse
  | PongResponse
  | ErrorResponse;

/**
 * Parse and validate a control channel message.
 * Returns the parsed message or an error string.
 */
export function parseControlMessage(
  raw: string | ArrayBuffer,
): ControlMessage | { error: string } {
  try {
    const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);

    // Payload size check
    if (text.length > METADATA_PAYLOAD_LIMIT) {
      return { error: "payload exceeds metadata size limit" };
    }

    const json = JSON.parse(text);
    const result = v.safeParse(ControlMessageSchema, json);

    if (!result.success) {
      return {
        error: `invalid message: ${result.issues[0]?.message ?? "unknown"}`,
      };
    }

    return result.output;
  } catch (e) {
    return {
      error: `parse error: ${e instanceof Error ? e.message : String(e)}`,
    };
  }
}
