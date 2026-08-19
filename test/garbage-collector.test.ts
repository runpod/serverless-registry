import { describe, expect, test } from "vitest";
import { GarbageCollector } from "../src/registry/garbage-collector";

type StoredObject = {
  body: string;
  etag: string;
  uploaded: Date;
  customMetadata?: Record<string, string>;
};

class FakeR2Bucket {
  objects = new Map<string, StoredObject>();
  nextEtag = 0;

  async head(key: string): Promise<R2Object | null> {
    const object = this.objects.get(key);
    return object ? (this.toR2Object(key, object) as R2Object) : null;
  }

  async put(key: string, value: string | null, options?: R2PutOptions): Promise<R2Object | null> {
    const existing = this.objects.get(key);
    const onlyIf = options?.onlyIf;
    if (onlyIf?.etagDoesNotMatch === "*" && existing) return null;
    if (onlyIf?.etagMatches && existing?.etag !== onlyIf.etagMatches) return null;

    const object: StoredObject = {
      body: value ?? "",
      etag: `etag-${++this.nextEtag}`,
      uploaded: new Date(),
      customMetadata: options?.customMetadata,
    };
    this.objects.set(key, object);
    return this.toR2Object(key, object) as R2Object;
  }

  async delete(key: string | string[]): Promise<void> {
    for (const item of Array.isArray(key) ? key : [key]) this.objects.delete(item);
  }

  private toR2Object(key: string, object: StoredObject) {
    return {
      key,
      version: "1",
      size: object.body.length,
      etag: object.etag,
      httpEtag: `\"${object.etag}\"`,
      checksums: {},
      uploaded: object.uploaded,
      customMetadata: object.customMetadata,
    };
  }
}

describe("garbage collection leases", () => {
  test("an expired marker can be claimed", async () => {
    const bucket = new FakeR2Bucket();
    await bucket.put("repo/gc/marker", "stale", {
      customMetadata: { token: "stale", expiresAt: "0" },
    });
    const collector = new GarbageCollector(bucket as unknown as R2Bucket);

    const lease = await collector.markForGarbageCollection("repo");

    expect(lease.token).not.toBe("stale");
    const marker = await bucket.head("repo/gc/marker");
    expect(marker?.customMetadata?.token).toBe(lease.token);
  });

  test("an active marker cannot be replaced", async () => {
    const bucket = new FakeR2Bucket();
    await bucket.put("repo/gc/marker", "active", {
      customMetadata: { token: "active", expiresAt: (Date.now() + 60_000).toString() },
    });
    const collector = new GarbageCollector(bucket as unknown as R2Bucket);

    await expect(collector.markForGarbageCollection("repo")).rejects.toMatchObject({ status: 409 });
  });

  test("cleanup cannot release a newer lease", async () => {
    const bucket = new FakeR2Bucket();
    const collector = new GarbageCollector(bucket as unknown as R2Bucket);
    const oldLease = await collector.markForGarbageCollection("repo");
    await bucket.put("repo/gc/marker", "new", {
      customMetadata: { token: "new", expiresAt: (Date.now() + 60_000).toString() },
    });

    await collector.cleanupGarbageCollectionMark("repo", oldLease);

    const marker = await bucket.head("repo/gc/marker");
    expect(marker?.customMetadata?.token).toBe("new");
  });
});
