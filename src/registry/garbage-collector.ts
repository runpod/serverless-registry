// We have 2 modes for the garbage collector, unreferenced and untagged.
// Unreferenced will delete all blobs that are not referenced by any manifest.
// Untagged will delete all blobs that are not referenced by any manifest and are not tagged.

import jwt from "@tsndr/cloudflare-worker-jwt";
import { ServerError } from "../errors";
import { ManifestSchema, manifestSchema } from "../manifest";
import { hexToDigest } from "../user";
import { getRegistryReference, parseRegistryReference } from "./references";

const ACTIVE_UPLOAD_MAX_AGE_MS = 25 * 60 * 60 * 1000;
export const DEFAULT_GC_MINIMUM_OBJECT_AGE_MS = 60 * 60 * 1000;

export type GarbageCollectionMode = "unreferenced" | "untagged";
export type GCOptions = {
  name: string;
  mode: GarbageCollectionMode;
  dryRun?: boolean;
  excludedReferences?: string[];
};

export type GarbageCollectionResult = {
  success: boolean;
  objectCount: number;
  bytes: number;
};

// The garbage collector checks for dangling layers in the namespace. It's a lock free
// GC, but on-conflict (when there is an ongoing manifest insertion, or an ongoing garbage collection),
// the methods can throw errors.
//
// Summary:
//          insertParent() {
//              gcMark = getGCMark(); // get last gc mark
//              mark = updateInsertMark(); // mark insertion
//              defer cleanInsertMark(mark);
//              checkEveryChildIsOK();
//              gcMarkIsEqualAndNotOngoingGc(gcMark); // make sure not ongoing deletion mark after checking child is in db
//              insertParent(); // insert parent in db
//           }
//
//           gc() {
//             insertionMark = getInsertionMark() // get last insertion mark
//             mark = setGCMark() // marks deletion as gc
//             defer { cleanGCMark(mark); } // clean up mark
//             checkNotOngoingInsertMark(mark) // makes sure not ongoing updateInsertMark, and no new one
//             deleteChildrenWithoutParent(); // go ahead and clean children
//           }
//
// This makes it so: after every layer is OK we can proceed and insert the manifest, as there is no ongoing GC
// In the GC code, if there is an insertion on-going, there is an error.
export class GarbageCollector {
  private registry: R2Bucket;

  constructor(
    registry: R2Bucket,
    private minimumObjectAgeMs = DEFAULT_GC_MINIMUM_OBJECT_AGE_MS,
  ) {
    this.registry = registry;
  }

  async markForGarbageCollection(namespace: string): Promise<string> {
    const etag = crypto.randomUUID();
    const deletion = await this.registry.put(`${namespace}/gc/marker`, etag);
    if (deletion === null) throw new Error("unreachable");
    // set last_update so inserters are able to invalidate
    await this.registry.put(`${namespace}/gc/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });
    return etag;
  }

  async cleanupGarbageCollectionMark(namespace: string) {
    // set last_update so inserters can confirm that a GC didnt happen while they were confirming data
    await this.registry.put(`${namespace}/gc/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });
    await this.registry.delete(`${namespace}/gc/marker`);
  }

  async getGCMarker(namespace: string): Promise<string> {
    const object = await this.registry.head(`${namespace}/gc/last_update`);
    if (object === null) {
      return "";
    }

    if (object.customMetadata === undefined) {
      return "";
    }

    return object.customMetadata["timestamp"] ?? "mark";
  }

  async checkCanInsertData(namespace: string, mark: string): Promise<boolean> {
    const gcMarker = await this.registry.head(`${namespace}/gc/marker`);
    if (gcMarker !== null) {
      return false;
    }

    const newMarker = await this.getGCMarker(namespace);
    // There's been a new garbage collection since we started the check for insertion
    if (newMarker !== mark) return false;

    return true;
  }

  // If successful, it inserted in R2 that its going
  // to start inserting data that might conflight with GC.
  async markForInsertion(namespace: string): Promise<string> {
    const uid = crypto.randomUUID();
    // mark that there is an on-going insertion
    const deletion = await this.registry.put(`${namespace}/insertion/${uid}`, uid);
    if (deletion === null) throw new Error("unreachable");
    // set last_update so GC is able to invalidate
    await this.registry.put(`${namespace}/insertion/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });

    return uid;
  }

  async cleanInsertion(namespace: string, tag: string) {
    // update again to invalidate GC and the insertion is safe
    await this.registry.put(`${namespace}/insertion/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });

    await this.registry.delete(`${namespace}/insertion/${tag}`);
  }

  async getInsertionMark(namespace: string): Promise<string> {
    const object = await this.registry.head(`${namespace}/insertion/last_update`);
    if (object === null) {
      return "";
    }

    if (object.customMetadata === undefined) {
      return "";
    }

    return object.customMetadata["timestamp"] ?? "mark";
  }

  async checkIfGCCanContinue(namespace: string, mark: string): Promise<boolean> {
    const objects = await this.registry.list({ prefix: `${namespace}/insertion` });
    for (const object of objects.objects) {
      if (object.key.endsWith("/last_update")) continue;
      if (object.uploaded.getTime() + 1000 * 60 <= Date.now()) {
        await this.registry.delete(object.key);
      } else {
        return false;
      }
    }

    // call again to clean more
    if (objects.truncated) return false;

    const newMark = await this.getInsertionMark(namespace);
    if (newMark !== mark) {
      return false;
    }

    return true;
  }

  private async list(prefix: string, callback: (object: R2Object) => Promise<boolean>): Promise<boolean> {
    const listed = await this.registry.list({ prefix });
    for (const object of listed.objects) {
      if ((await callback(object)) === false) {
        return false;
      }
    }

    let truncated = listed.truncated;
    let cursor = listed.truncated ? listed.cursor : undefined;

    while (truncated) {
      const next = await this.registry.list({ prefix, cursor });
      for (const object of next.objects) {
        if ((await callback(object)) === false) {
          return false;
        }
      }
      cursor = next.truncated ? next.cursor : undefined;
      truncated = next.truncated;
    }
    return true;
  }

  async collect(options: GCOptions): Promise<GarbageCollectionResult> {
    if (options.dryRun) return this.collectInner(options);

    await this.markForGarbageCollection(options.name);
    try {
      return await this.collectInner(options);
    } finally {
      // if this fails, user can always call a custom endpoint to clean it up
      await this.cleanupGarbageCollectionMark(options.name);
    }
  }

  private async collectInner(options: GCOptions): Promise<GarbageCollectionResult> {
    const mark = await this.getInsertionMark(options.name);
    const excludedReferences = new Set(options.excludedReferences ?? []);
    const manifestKeysByDigest = new Map<string, Set<string>>();
    const manifestDataByDigest = new Map<string, ManifestSchema>();
    const manifestObjectsByKey = new Map<string, R2Object>();
    let objectCount = 0;
    let bytes = 0;

    await this.list(`${options.name}/manifests/`, async (manifestObject) => {
      if (!manifestObject.checksums.sha256) {
        throw new ServerError("manifest is missing its sha256 checksum");
      }
      const digest = hexToDigest(manifestObject.checksums.sha256);
      const keys = manifestKeysByDigest.get(digest) ?? new Set<string>();
      keys.add(manifestObject.key);
      manifestKeysByDigest.set(digest, keys);
      manifestObjectsByKey.set(manifestObject.key, manifestObject);
      return true;
    });

    const loadManifest = async (digest: string): Promise<ManifestSchema> => {
      const cached = manifestDataByDigest.get(digest);
      if (cached) return cached;

      const key = manifestKeysByDigest.get(digest)?.values().next().value;
      if (!key) {
        throw new ServerError(`referenced manifest ${digest} is missing`);
      }
      const object = await this.registry.get(key);
      if (!object) {
        throw new ServerError(`manifest ${digest} disappeared during garbage collection`);
      }
      const parsed = manifestSchema.safeParse(await object.json());
      if (!parsed.success) {
        throw new ServerError(`manifest ${digest} is invalid`);
      }
      manifestDataByDigest.set(digest, parsed.data);
      return parsed.data;
    };

    const relatedManifests = new Map<string, Set<string>>();
    const addManifestRelationship = (source: string, destination: string) => {
      const relationships = relatedManifests.get(source) ?? new Set<string>();
      relationships.add(destination);
      relatedManifests.set(source, relationships);
    };

    for (const digest of manifestKeysByDigest.keys()) {
      const manifest = await loadManifest(digest);
      if (manifest.schemaVersion !== 2) continue;
      if ("manifests" in manifest) {
        manifest.manifests.forEach((child) => addManifestRelationship(digest, child.digest));
      } else if (manifest.subject && manifestKeysByDigest.has(manifest.subject.digest)) {
        addManifestRelationship(digest, manifest.subject.digest);
        addManifestRelationship(manifest.subject.digest, digest);
      }
    }

    const liveManifests = new Set<string>();
    const pendingManifests: string[] = [];
    for (const [digest, keys] of manifestKeysByDigest) {
      const tagged = [...keys].some((key) => {
        const reference = key.split("/").pop();
        return reference && !reference.startsWith("sha256:") && !excludedReferences.has(reference);
      });
      const recentlyUploaded = [...keys].some((key) => {
        const reference = key.split("/").pop();
        if (reference && excludedReferences.has(reference)) return false;
        const object = manifestObjectsByKey.get(key);
        return object && object.uploaded.getTime() + this.minimumObjectAgeMs > Date.now();
      });
      if (options.mode === "unreferenced" || tagged || recentlyUploaded) {
        liveManifests.add(digest);
        pendingManifests.push(digest);
      }
    }

    while (pendingManifests.length > 0) {
      const digest = pendingManifests.pop()!;
      for (const relatedDigest of relatedManifests.get(digest) ?? []) {
        if (liveManifests.has(relatedDigest)) continue;
        liveManifests.add(relatedDigest);
        pendingManifests.push(relatedDigest);
      }
    }

    if (options.mode === "untagged") {
      const untaggedManifestKeys = new Set<string>();
      for (const [digest, keys] of manifestKeysByDigest) {
        keys.forEach((key) => {
          const reference = key.split("/").pop();
          if (reference && excludedReferences.has(reference)) untaggedManifestKeys.add(key);
        });
        if (liveManifests.has(digest)) continue;
        keys.forEach((key) => untaggedManifestKeys.add(key));
      }
      untaggedManifestKeys.forEach((key) => {
        const object = manifestObjectsByKey.get(key);
        if (!object) return;
        objectCount++;
        bytes += object.size;
      });
      if (untaggedManifestKeys.size > 0 && !options.dryRun) {
        const keys = [...untaggedManifestKeys];
        for (let index = 0; index < keys.length; index += 1000) {
          if (!(await this.checkIfGCCanContinue(options.name, mark))) {
            throw new ServerError("there is a manifest insertion going, the garbage collection shall stop");
          }
          await this.registry.delete(keys.slice(index, index + 1000));
        }
      }
    }

    const referencedBlobs = new Set<string>();
    for (const digest of liveManifests) {
      const manifest = await loadManifest(digest);
      if (manifest.schemaVersion === 1) {
        manifest.fsLayers.forEach((layer) => referencedBlobs.add(layer.blobSum));
      } else if (!("manifests" in manifest)) {
        referencedBlobs.add(manifest.config.digest);
        manifest.layers.forEach((layer) => referencedBlobs.add(layer.digest));
      }
    }

    await this.list(`${options.name}/uploads/`, async (uploadObject) => {
      if (uploadObject.uploaded.getTime() + ACTIVE_UPLOAD_MAX_AGE_MS < Date.now()) return true;
      const object = await this.registry.get(uploadObject.key);
      if (!object) return true;
      const encodedState = await object.json<{ jwt?: string }>();
      if (!encodedState.jwt) throw new ServerError("active upload state is invalid");
      const state = jwt.decode<{ direct?: { objectKey?: string } }>(encodedState.jwt).payload;
      const objectKey = state?.direct?.objectKey;
      const prefix = `${options.name}/blobs/`;
      if (objectKey?.startsWith(prefix)) referencedBlobs.add(objectKey.slice(prefix.length));
      return true;
    });

    const retainedReferences = new Set<string>();
    await this.list(`${options.name}/blobs/`, async (object) => {
      const hash = object.key.split("/").pop();
      if (hash && referencedBlobs.has(hash)) {
        const reference = await getRegistryReference(this.registry, object);
        if (reference) retainedReferences.add(reference);
      }
      return true;
    });

    let unreferencedKeys = new Set<string>();
    const scheduledLegacyTargets = new Set<string>();
    const deleteThreshold = 15;
    const flushUnreferencedKeys = async () => {
      if (unreferencedKeys.size === 0) return;
      if (!options.dryRun) {
        if (!(await this.checkIfGCCanContinue(options.name, mark))) {
          throw new ServerError("there is a manifest insertion going, the garbage collection shall stop");
        }
        await this.registry.delete([...unreferencedKeys]);
      }
      unreferencedKeys = new Set<string>();
    };

    await this.list(`${options.name}/blobs/`, async (object) => {
      const hash = object.key.split("/").pop();
      if (hash && !referencedBlobs.has(hash) && object.uploaded.getTime() + this.minimumObjectAgeMs <= Date.now()) {
        objectCount++;
        bytes += object.size;
        const reference = await getRegistryReference(this.registry, object);
        if (
          reference &&
          parseRegistryReference(reference) &&
          !retainedReferences.has(reference) &&
          !scheduledLegacyTargets.has(reference)
        ) {
          scheduledLegacyTargets.add(reference);
          unreferencedKeys.add(reference);
          const target = await this.registry.head(reference);
          if (target) {
            objectCount++;
            bytes += target.size;
          }
        }
        unreferencedKeys.add(object.key);
        if (unreferencedKeys.size > deleteThreshold) await flushUnreferencedKeys();
      }
      return true;
    });
    await flushUnreferencedKeys();

    return { success: true, objectCount, bytes };
  }
}
