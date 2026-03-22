/**
 * Core types shared across server modules.
 */

export type FileId = string;
export type FileKind = "text" | "binary" | "directory";

/** Canonical file metadata stored in the registry. */
export interface FileMetadata {
  fileId: FileId;
  path: string;
  kind: FileKind;
  deleted: boolean;
  createdAt: number;
  updatedAt: number;
  contentDigest: string | null;
  contentSize: number | null;
  contentModTime: number | null;
  contentAnchor: number;
}

/** Metadata intent from a client. */
export type MetadataIntentType = "create" | "rename" | "move" | "delete";

export interface MetadataIntent {
  type: MetadataIntentType;
  clientId: string;
  operationId: string;
  fileId?: FileId;
  path?: string;
  newPath?: string;
  kind?: FileKind;
  contentAnchor?: number;
  contentDigest?: string;
}

/** Authoritative result after processing an intent. */
export interface MetadataCommit {
  operationId: string;
  fileId: FileId;
  path: string;
  kind: FileKind;
  deleted: boolean;
  contentAnchor: number;
  revision: number;
  epoch: string;
}

/** Rejection for an invalid intent. */
export interface MetadataReject {
  operationId: string;
  reason: string;
}

/** History entry for append-only log. */
export interface HistoryEntry {
  id: number;
  fileId: FileId;
  operationType: MetadataIntentType | "restore" | "content-update";
  path: string;
  kind: FileKind;
  contentDigest: string | null;
  contentSize: number | null;
  contentAnchor: number;
  clientId: string;
  operationId: string;
  timestamp: number;
  revision: number;
  epoch: string;
}

/** Blob metadata stored alongside content. */
export interface BlobRecord {
  fileId: FileId;
  digest: string;
  size: number;
  contentAnchor: number;
  storedAt: number;
}

/** Settings snapshot record. */
export interface SettingsSnapshot {
  configPath: string;
  digest: string;
  size: number;
  contentAnchor: number;
  storedAt: number;
}
