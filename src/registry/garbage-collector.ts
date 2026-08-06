import jwt from "@tsndr/cloudflare-worker-jwt";
import { ServerError } from "../errors";
import { ManifestSchema, manifestSchema } from "../manifest";
import { hexToDigest } from "../user";
import { getRegistryReference, parseRegistryReference } from "./references";

const ACTIVE_UPLOAD_MAX_AGE_MS = 25 * 60 * 60 * 1000;
const DELETE_BATCH_SIZE = 100;
const FILTER_BYTE_SIZE = 2 * 1024 * 1024;
const FILTER_HASH_COUNT = 7;
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

class ConservativeBloomFilter {
  private bits = new Uint8Array(FILTER_BYTE_SIZE);
  private bitCount = this.bits.length * 8;

  private hashes(value: string): [number, number] {
    let first = 0x811c9dc5;
    let second = 0x9e3779b9;
    for (let index = 0; index < value.length; index++) {
      const code = value.charCodeAt(index);
      first = Math.imul(first ^ code, 0x01000193) >>> 0;
      second = Math.imul(second ^ code, 0x85ebca6b) >>> 0;
    }
    return [first, second | 1];
  }

  add(value: string): boolean {
    const [first, second] = this.hashes(value);
    let changed = false;
    for (let index = 0; index < FILTER_HASH_COUNT; index++) {
      const bit = ((first + Math.imul(index, second)) >>> 0) % this.bitCount;
      const byte = bit >>> 3;
      const mask = 1 << (bit & 7);
      if ((this.bits[byte] & mask) === 0) {
        this.bits[byte] |= mask;
        changed = true;
      }
    }
    return changed;
  }

  has(value: string): boolean {
    const [first, second] = this.hashes(value);
    for (let index = 0; index < FILTER_HASH_COUNT; index++) {
      const bit = ((first + Math.imul(index, second)) >>> 0) % this.bitCount;
      const byte = bit >>> 3;
      const mask = 1 << (bit & 7);
      if ((this.bits[byte] & mask) === 0) return false;
    }
    return true;
  }
}

export class GarbageCollector {
  constructor(
    private registry: R2Bucket,
    private minimumObjectAgeMs = DEFAULT_GC_MINIMUM_OBJECT_AGE_MS,
  ) {}

  async markForGarbageCollection(namespace: string): Promise<string> {
    const token = crypto.randomUUID();
    const marker = await this.registry.put(`${namespace}/gc/marker`, token, {
      onlyIf: { etagDoesNotMatch: "*" },
    });
    if (marker === null) {
      throw new ServerError("garbage collection is already running", 409);
    }
    await this.registry.put(`${namespace}/gc/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });
    return token;
  }

  async cleanupGarbageCollectionMark(namespace: string) {
    await this.registry.put(`${namespace}/gc/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });
    await this.registry.delete(`${namespace}/gc/marker`);
  }

  async getGCMarker(namespace: string): Promise<string> {
    const object = await this.registry.head(`${namespace}/gc/last_update`);
    if (object === null || object.customMetadata === undefined) return "";
    return object.customMetadata["timestamp"] ?? "mark";
  }

  async checkCanInsertData(namespace: string, mark: string): Promise<boolean> {
    if ((await this.registry.head(`${namespace}/gc/marker`)) !== null) return false;
    return (await this.getGCMarker(namespace)) === mark;
  }

  async markForInsertion(namespace: string): Promise<string> {
    const uid = crypto.randomUUID();
    const insertion = await this.registry.put(`${namespace}/insertion/${uid}`, uid);
    if (insertion === null) throw new Error("unreachable");
    await this.registry.put(`${namespace}/insertion/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });
    return uid;
  }

  async cleanInsertion(namespace: string, tag: string) {
    await this.registry.put(`${namespace}/insertion/last_update`, null, {
      customMetadata: { timestamp: `${Date.now()}-${crypto.randomUUID()}` },
    });
    await this.registry.delete(`${namespace}/insertion/${tag}`);
  }

  async getInsertionMark(namespace: string): Promise<string> {
    const object = await this.registry.head(`${namespace}/insertion/last_update`);
    if (object === null || object.customMetadata === undefined) return "";
    return object.customMetadata["timestamp"] ?? "mark";
  }

  async checkIfGCCanContinue(namespace: string, mark: string): Promise<boolean> {
    const objects = await this.registry.list({ prefix: `${namespace}/insertion` });
    for (const object of objects.objects) {
      if (object.key.endsWith("/last_update")) continue;
      if (object.uploaded.getTime() + 60_000 <= Date.now()) {
        await this.registry.delete(object.key);
      } else {
        return false;
      }
    }
    if (objects.truncated) return false;
    return (await this.getInsertionMark(namespace)) === mark;
  }

  async withGarbageCollectionLock<T>(namespace: string, callback: () => Promise<T>): Promise<T> {
    await this.markForGarbageCollection(namespace);
    try {
      const insertionMark = await this.getInsertionMark(namespace);
      if (!(await this.checkIfGCCanContinue(namespace, insertionMark))) {
        throw new ServerError("manifest insertion is in progress", 409);
      }
      return await callback();
    } finally {
      await this.cleanupGarbageCollectionMark(namespace);
    }
  }

  private async list(prefix: string, callback: (object: R2Object) => Promise<boolean>): Promise<boolean> {
    let cursor: string | undefined;
    do {
      const page = await this.registry.list({
        prefix,
        cursor,
        include: ["customMetadata"],
      } as unknown as R2ListOptions);
      for (const object of page.objects) {
        if (!(await callback(object))) return false;
      }
      cursor = page.truncated ? page.cursor : undefined;
    } while (cursor);
    return true;
  }

  private manifestDigest(object: R2Object): string {
    if (!object.checksums.sha256) {
      throw new ServerError("manifest is missing its sha256 checksum");
    }
    return hexToDigest(object.checksums.sha256);
  }

  private async loadManifest(key: string): Promise<ManifestSchema> {
    const object = await this.registry.get(key);
    if (!object) throw new ServerError(`manifest ${key} disappeared during garbage collection`);
    const parsed = manifestSchema.safeParse(await object.json());
    if (!parsed.success) throw new ServerError(`manifest ${key} is invalid`);
    return parsed.data;
  }

  private isRecent(object: R2Object): boolean {
    return object.uploaded.getTime() + this.minimumObjectAgeMs > Date.now();
  }

  async collect(options: GCOptions): Promise<GarbageCollectionResult> {
    if (options.dryRun) return this.collectInner(options);
    return this.withGarbageCollectionLock(options.name, () => this.collectInner(options));
  }

  private async collectInner(options: GCOptions): Promise<GarbageCollectionResult> {
    const insertionMark = await this.getInsertionMark(options.name);
    const excludedReferences = new Set(options.excludedReferences ?? []);
    const liveManifests = new ConservativeBloomFilter();
    let objectCount = 0;
    let bytes = 0;

    await this.list(`${options.name}/manifests/`, async (object) => {
      const digest = this.manifestDigest(object);
      const reference = object.key.split("/").pop();
      if (!reference) return true;
      const digestReference = reference.startsWith("sha256:");
      const excludedAndOld = excludedReferences.has(reference) && !this.isRecent(object);
      if (options.mode === "unreferenced" || (!digestReference && !excludedAndOld) || this.isRecent(object)) {
        liveManifests.add(digest);
      }
      if (!digestReference && !excludedAndOld) {
        const canonical = await this.registry.head(`${options.name}/manifests/${digest}`);
        if (!canonical) throw new ServerError(`canonical manifest ${digest} is missing`);
      }
      return true;
    });

    let liveSetChanged: boolean;
    do {
      liveSetChanged = false;
      await this.list(`${options.name}/manifests/sha256:`, async (object) => {
        const digest = this.manifestDigest(object);
        const manifest = await this.loadManifest(object.key);
        if (manifest.schemaVersion !== 2) return true;

        if ("manifests" in manifest) {
          if (liveManifests.has(digest)) {
            for (const child of manifest.manifests) {
              liveSetChanged = liveManifests.add(child.digest) || liveSetChanged;
            }
          }
        } else if (manifest.subject) {
          if (liveManifests.has(digest)) {
            liveSetChanged = liveManifests.add(manifest.subject.digest) || liveSetChanged;
          }
          if (liveManifests.has(manifest.subject.digest)) {
            liveSetChanged = liveManifests.add(digest) || liveSetChanged;
          }
        }
        return true;
      });
    } while (liveSetChanged);

    let manifestKeys: string[] = [];
    const flushManifestKeys = async () => {
      if (manifestKeys.length === 0) return;
      if (!options.dryRun) {
        if (!(await this.checkIfGCCanContinue(options.name, insertionMark))) {
          throw new ServerError("manifest insertion is in progress", 409);
        }
        await this.registry.delete(manifestKeys);
      }
      manifestKeys = [];
    };

    if (options.mode === "untagged") {
      await this.list(`${options.name}/manifests/`, async (object) => {
        const digest = this.manifestDigest(object);
        const reference = object.key.split("/").pop();
        if (!reference) return true;
        const deleteExcludedReference = excludedReferences.has(reference) && !this.isRecent(object);
        if (!deleteExcludedReference && liveManifests.has(digest)) return true;
        objectCount++;
        bytes += object.size;
        manifestKeys.push(object.key);
        if (manifestKeys.length >= DELETE_BATCH_SIZE) await flushManifestKeys();
        return true;
      });
      await flushManifestKeys();
    }

    const referencedBlobs = new ConservativeBloomFilter();
    await this.list(`${options.name}/manifests/sha256:`, async (object) => {
      const digest = this.manifestDigest(object);
      if (!liveManifests.has(digest)) return true;
      const manifest = await this.loadManifest(object.key);
      if (manifest.schemaVersion === 1) {
        manifest.fsLayers.forEach((layer) => referencedBlobs.add(layer.blobSum));
      } else if (!("manifests" in manifest)) {
        referencedBlobs.add(manifest.config.digest);
        manifest.layers.forEach((layer) => referencedBlobs.add(layer.digest));
      }
      return true;
    });

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

    const retainedReferences = new ConservativeBloomFilter();
    await this.list(`${options.name}/blobs/`, async (object) => {
      const hash = object.key.split("/").pop();
      if (hash && referencedBlobs.has(hash)) {
        const reference = await getRegistryReference(this.registry, object);
        if (reference) retainedReferences.add(reference);
      }
      return true;
    });

    const scheduledLegacyTargets = new ConservativeBloomFilter();
    let blobKeys: string[] = [];
    const flushBlobKeys = async () => {
      if (blobKeys.length === 0) return;
      if (!options.dryRun) {
        if (!(await this.checkIfGCCanContinue(options.name, insertionMark))) {
          throw new ServerError("manifest insertion is in progress", 409);
        }
        await this.registry.delete(blobKeys);
      }
      blobKeys = [];
    };

    await this.list(`${options.name}/blobs/`, async (object) => {
      const hash = object.key.split("/").pop();
      if (!hash || referencedBlobs.has(hash) || this.isRecent(object)) return true;

      objectCount++;
      bytes += object.size;
      const reference = await getRegistryReference(this.registry, object);
      if (
        reference &&
        parseRegistryReference(reference) &&
        !retainedReferences.has(reference) &&
        scheduledLegacyTargets.add(reference)
      ) {
        blobKeys.push(reference);
        const target = await this.registry.head(reference);
        if (target) {
          objectCount++;
          bytes += target.size;
        }
      }
      blobKeys.push(object.key);
      if (blobKeys.length >= DELETE_BATCH_SIZE) await flushBlobKeys();
      return true;
    });
    await flushBlobKeys();

    return { success: true, objectCount, bytes };
  }
}
