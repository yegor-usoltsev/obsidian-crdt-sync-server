import { join, normalize, relative } from "pathe";
import type { Doc } from "yjs";
import * as Y from "yjs";
import type { FileKind } from "./file-kind.ts";
import { coerceFileKind } from "./file-kind.ts";
import type { MetadataOpRequest } from "./messages.ts";

export const CONTENT_DOCUMENT_NAME = "vault-content";
export const META_DOCUMENT_NAME = "vault-meta";
export const METADATA_INIT_ORIGIN = "server:metadata:init";
export const METADATA_OP_ORIGIN = "server:metadata:op";

export interface MetadataFragments {
  files: Y.Map<Y.Map<unknown>>;
  events: Y.Array<Y.Map<unknown>>;
  serverState: Y.Map<unknown>;
}

export interface AppliedMetadataOp {
  eventId: number;
  requestType: MetadataOpRequest["type"];
  currentPath?: string;
  currentKind?: FileKind;
  oldPath?: string;
  newPath?: string;
  deletedContentFileIds?: string[];
  deduplicated?: boolean;
}

function asBoolean(value: unknown): boolean {
  return value === true;
}

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : undefined;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function createYMap(
  entries: Record<string, string | number | boolean | undefined>,
): Y.Map<unknown> {
  const map = new Y.Map<unknown>();

  for (const [key, value] of Object.entries(entries)) {
    if (value !== undefined) {
      map.set(key, value);
    }
  }

  return map;
}

function getTimestamp(timestamp?: number): number {
  return typeof timestamp === "number" && Number.isFinite(timestamp)
    ? Math.trunc(timestamp)
    : Date.now();
}

function findActiveFileIdByPath(
  files: Y.Map<Y.Map<unknown>>,
  path: string,
  ignoredFileId?: string,
): string | undefined {
  for (const [candidateFileId, metadata] of files.entries()) {
    if (candidateFileId === ignoredFileId) {
      continue;
    }

    if (metadata.get("path") === path && !asBoolean(metadata.get("deleted"))) {
      return candidateFileId;
    }
  }

  return undefined;
}

function getRequiredFileMetadata(
  files: Y.Map<Y.Map<unknown>>,
  fileId: string,
): Y.Map<unknown> {
  const metadata = files.get(fileId);
  if (!metadata) {
    throw new Error(`Unknown fileId "${fileId}"`);
  }

  return metadata;
}

function getEventPath(event: Y.Map<unknown>): string | undefined {
  return asString(event.get("path")) ?? asString(event.get("newPath"));
}

function getFileKind(metadata: Y.Map<unknown>): FileKind {
  return coerceFileKind(metadata.get("kind")) ?? "text";
}

function normalizeVaultPath(path: string): string {
  return normalize(path).split("\\").join("/");
}

function isSameOrDescendant(path: string, parent: string): boolean {
  const normalizedPath = normalizeVaultPath(path);
  const normalizedParent = normalizeVaultPath(parent);
  return (
    normalizedPath === normalizedParent ||
    normalizedPath.startsWith(`${normalizedParent}/`)
  );
}

function renamePath(path: string, oldPath: string, newPath: string): string {
  const normalizedPath = normalizeVaultPath(path);
  const normalizedOldPath = normalizeVaultPath(oldPath);
  const normalizedNewPath = normalizeVaultPath(newPath);
  if (normalizedPath === normalizedOldPath) {
    return normalizedNewPath;
  }

  return normalizeVaultPath(
    join(normalizedNewPath, relative(normalizedOldPath, normalizedPath)),
  );
}

function listActiveDescendants(
  files: Y.Map<Y.Map<unknown>>,
  directoryPath: string,
  ignoredFileId?: string,
): Array<{ fileId: string; metadata: Y.Map<unknown>; path: string }> {
  const descendants: Array<{
    fileId: string;
    metadata: Y.Map<unknown>;
    path: string;
  }> = [];

  for (const [fileId, metadata] of files.entries()) {
    if (fileId === ignoredFileId || asBoolean(metadata.get("deleted"))) {
      continue;
    }

    const path = asString(metadata.get("path"));
    if (!path || !isSameOrDescendant(path, directoryPath)) {
      continue;
    }

    descendants.push({ fileId, metadata, path });
  }

  return descendants;
}

function isEquivalentReplay(
  event: Y.Map<unknown>,
  request: MetadataOpRequest,
): boolean {
  if (
    event.get("type") !== request.type ||
    event.get("fileId") !== request.fileId
  ) {
    return false;
  }

  switch (request.type) {
    case "file.create":
      return (
        event.get("path") === request.path &&
        getFileKind(event) === request.kind
      );
    case "file.rename":
      return (
        getFileKind(event) === request.kind &&
        event.get("newPath") === request.newPath &&
        (request.oldPath === undefined ||
          event.get("oldPath") === request.oldPath)
      );
    case "file.delete":
      return request.path === undefined || event.get("path") === request.path;
  }
}

function findExistingReplay(
  events: Y.Array<Y.Map<unknown>>,
  request: MetadataOpRequest,
): AppliedMetadataOp | undefined {
  if (!request.clientId || !request.clientOpId) {
    return undefined;
  }

  for (const event of events.toArray()) {
    if (
      event.get("clientId") !== request.clientId ||
      event.get("clientOpId") !== request.clientOpId
    ) {
      continue;
    }

    if (!isEquivalentReplay(event, request)) {
      throw new Error(
        `clientOpId "${request.clientOpId}" was replayed with different payload`,
      );
    }

    const requestType = event.get("type");
    if (
      requestType !== "file.create" &&
      requestType !== "file.rename" &&
      requestType !== "file.delete"
    ) {
      throw new Error(
        `Stored event has unsupported type "${String(requestType)}"`,
      );
    }

    return {
      eventId: asNumber(event.get("eventId")) ?? 0,
      requestType,
      currentPath: getEventPath(event),
      currentKind: coerceFileKind(event.get("kind")) ?? "text",
      oldPath: asString(event.get("oldPath")),
      newPath: asString(event.get("newPath")),
      deduplicated: true,
    };
  }

  return undefined;
}

export function getMetadataFragments(document: Doc): MetadataFragments {
  return {
    files: document.getMap<Y.Map<unknown>>("files"),
    events: document.getArray<Y.Map<unknown>>("events"),
    serverState: document.getMap<unknown>("serverState"),
  };
}

export function initializeMetadataDoc(document: Doc): boolean {
  const { serverState } = getMetadataFragments(document);
  let changed = false;

  if (serverState.get("lastEventId") === undefined) {
    serverState.set("lastEventId", 0);
    changed = true;
  }

  if (serverState.get("serverEpoch") === undefined) {
    serverState.set("serverEpoch", 1);
    changed = true;
  }

  return changed;
}

export function getCurrentMetadataForFileId(
  document: Doc,
  fileId: string,
): { path?: string; kind?: FileKind } | undefined {
  const { files } = getMetadataFragments(document);
  const metadata = files.get(fileId);
  if (!metadata) {
    return undefined;
  }

  return {
    path: asString(metadata.get("path")),
    kind: getFileKind(metadata),
  };
}

export function applyMetadataOp(
  document: Doc,
  request: MetadataOpRequest,
  deleteContentFingerprint?: string,
): AppliedMetadataOp {
  const { files, events, serverState } = getMetadataFragments(document);
  const existingReplay = findExistingReplay(events, request);
  if (existingReplay) {
    return existingReplay;
  }

  const timestamp = getTimestamp(request.timestamp);
  const lastEventId = asNumber(serverState.get("lastEventId")) ?? 0;
  const nextEventId = lastEventId + 1;

  switch (request.type) {
    case "file.create": {
      if (files.has(request.fileId)) {
        throw new Error(`fileId "${request.fileId}" already exists`);
      }

      const conflictingFileId = findActiveFileIdByPath(files, request.path);
      if (conflictingFileId) {
        throw new Error(
          `path "${request.path}" already belongs to fileId "${conflictingFileId}"`,
        );
      }

      const metadata = createYMap({
        path: request.path,
        kind: request.kind,
        deleted: false,
        createdAt: timestamp,
        updatedAt: timestamp,
      });
      files.set(request.fileId, metadata);
      events.push([
        createYMap({
          eventId: nextEventId,
          type: request.type,
          fileId: request.fileId,
          path: request.path,
          kind: request.kind,
          timestamp,
          clientId: request.clientId,
          clientOpId: request.clientOpId,
        }),
      ]);
      serverState.set("lastEventId", nextEventId);

      return {
        eventId: nextEventId,
        requestType: request.type,
        currentPath: request.path,
        currentKind: request.kind,
      };
    }

    case "file.rename": {
      const metadata = getRequiredFileMetadata(files, request.fileId);
      if (asBoolean(metadata.get("deleted"))) {
        throw new Error(`fileId "${request.fileId}" is deleted`);
      }

      const currentPath = asString(metadata.get("path"));
      if (!currentPath) {
        throw new Error(`fileId "${request.fileId}" is missing a current path`);
      }
      const currentKind = getFileKind(metadata);

      if (request.oldPath && request.oldPath !== currentPath) {
        throw new Error(
          `rename expected "${request.oldPath}" but current path is "${currentPath}"`,
        );
      }
      if ((currentKind === "directory") !== (request.kind === "directory")) {
        throw new Error(
          `fileId "${request.fileId}" kind cannot change between file and directory`,
        );
      }

      if (request.kind === "directory") {
        if (
          request.newPath !== currentPath &&
          isSameOrDescendant(request.newPath, currentPath)
        ) {
          throw new Error(
            `directory "${currentPath}" cannot be moved into "${request.newPath}"`,
          );
        }

        const movedEntries = listActiveDescendants(files, currentPath);
        const movedPaths = new Set(
          movedEntries.map(({ path }) =>
            renamePath(path, currentPath, request.newPath),
          ),
        );
        for (const [candidateFileId, candidateMetadata] of files.entries()) {
          if (
            candidateFileId === request.fileId ||
            asBoolean(candidateMetadata.get("deleted"))
          ) {
            continue;
          }

          const candidatePath = asString(candidateMetadata.get("path"));
          if (!candidatePath) {
            continue;
          }
          if (movedEntries.some(({ fileId }) => fileId === candidateFileId)) {
            continue;
          }

          if (movedPaths.has(candidatePath)) {
            throw new Error(
              `path "${candidatePath}" would conflict with directory rename`,
            );
          }
        }

        for (const entry of movedEntries) {
          entry.metadata.set(
            "path",
            renamePath(entry.path, currentPath, request.newPath),
          );
          if (entry.fileId === request.fileId || entry.path !== currentPath) {
            entry.metadata.set("updatedAt", timestamp);
          }
        }
      } else {
        const conflictingFileId = findActiveFileIdByPath(
          files,
          request.newPath,
          request.fileId,
        );
        if (conflictingFileId) {
          throw new Error(
            `path "${request.newPath}" already belongs to fileId "${conflictingFileId}"`,
          );
        }
        metadata.set("path", request.newPath);
        metadata.set("updatedAt", timestamp);
      }
      metadata.set("kind", request.kind);
      events.push([
        createYMap({
          eventId: nextEventId,
          type: request.type,
          fileId: request.fileId,
          oldPath: currentPath,
          newPath: request.newPath,
          path: request.newPath,
          kind: request.kind,
          timestamp,
          clientId: request.clientId,
          clientOpId: request.clientOpId,
        }),
      ]);
      serverState.set("lastEventId", nextEventId);

      return {
        eventId: nextEventId,
        requestType: request.type,
        currentPath: request.newPath,
        currentKind: request.kind,
        oldPath: currentPath,
        newPath: request.newPath,
      };
    }

    case "file.delete": {
      const metadata = getRequiredFileMetadata(files, request.fileId);
      if (asBoolean(metadata.get("deleted"))) {
        return {
          eventId: asNumber(serverState.get("lastEventId")) ?? 0,
          requestType: "file.delete",
          currentPath: asString(metadata.get("path")) ?? request.path,
          currentKind: getFileKind(metadata),
          deduplicated: true,
        };
      }

      const currentPath = asString(metadata.get("path")) ?? request.path;
      const currentKind = getFileKind(metadata);
      let deletedContentFileIds: string[] | undefined;
      if (currentKind === "directory" && currentPath) {
        const deletedEntries = listActiveDescendants(files, currentPath);
        deletedContentFileIds = deletedEntries
          .filter(
            ({ metadata: descendant }) =>
              getFileKind(descendant) !== "directory",
          )
          .map(({ fileId }) => fileId);
        for (const { metadata: descendant } of deletedEntries) {
          descendant.set("deleted", true);
          descendant.set("updatedAt", timestamp);
        }
      } else {
        metadata.set("deleted", true);
        metadata.set("updatedAt", timestamp);
        if (currentKind !== "directory") {
          deletedContentFileIds = [request.fileId];
        }
      }
      events.push([
        createYMap({
          eventId: nextEventId,
          type: request.type,
          fileId: request.fileId,
          path: currentPath,
          kind: currentKind,
          contentFingerprint:
            currentKind === "directory" ? undefined : deleteContentFingerprint,
          timestamp,
          clientId: request.clientId,
          clientOpId: request.clientOpId,
        }),
      ]);
      serverState.set("lastEventId", nextEventId);

      return {
        eventId: nextEventId,
        requestType: request.type,
        currentPath,
        currentKind,
        deletedContentFileIds,
      };
    }
  }
}
