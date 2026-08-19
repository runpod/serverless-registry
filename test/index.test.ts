import { afterAll, describe, expect, test } from "vitest";
import { SHA256_PREFIX_LEN, getSHA256 } from "../src/user";
import { TagsList } from "../src/router";
import { Env } from "..";
import { RegistryTokens } from "../src/token";
import { RegistryAuthProtocolTokenPayload } from "../src/auth";
import { registries } from "../src/registry/registry";
import { RegistryHTTPClient } from "../src/registry/http";
import { encode } from "@cfworker/base64url";
import { ManifestSchema } from "../src/manifest";
import { limit } from "../src/chunk";
import { DELETION_CLAIM_MAX_AGE_MS, encodeState } from "../src/registry/r2";
import worker from "../index";
import { createExecutionContext, env, waitOnExecutionContext } from "cloudflare:test";

async function generateManifest(name: string, schemaVersion: 1 | 2 = 2): Promise<ManifestSchema> {
  const data = "bla";
  const sha256 = await getSHA256(data);
  const res = await fetch(createRequest("POST", `/v2/${name}/blobs/uploads/`, null, {}));
  expect(res.ok).toBeTruthy();
  const blob = new Blob([data]).stream();
  const stream = limit(blob, data.length);
  const res2 = await fetch(createRequest("PATCH", res.headers.get("location")!, stream, {}));
  expect(res2.ok).toBeTruthy();
  const last = await fetch(createRequest("PUT", res2.headers.get("location")! + "&digest=" + sha256, null, {}));
  expect(last.ok).toBeTruthy();
  return schemaVersion === 1
    ? {
        schemaVersion,
        fsLayers: [{ blobSum: sha256 }],
        architecture: "amd64",
      }
    : {
        schemaVersion,
        layers: [
          { size: data.length, digest: sha256, mediaType: "shouldbeanything" },
          { size: data.length, digest: sha256, mediaType: "shouldbeanything" },
        ],
        config: { size: data.length, digest: sha256, mediaType: "configmediatypeshouldntbechecked" },
        mediaType: "shouldalsobeanythingforretrocompatibility",
      };
}

function createRequest(method: string, path: string, body: ReadableStream | null, headers = {}) {
  return new Request(new URL("https://registry.com" + path), { method, body: body, headers });
}

function shuffleArray<T>(array: T[]) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }

  return array;
}

function usernamePasswordToAuth(username: string, password: string): string {
  return `Basic ${btoa(`${username}:${password}`)}`;
}

async function fetchUnauth(r: Request): Promise<Response> {
  const ctx = createExecutionContext();
  const res = await worker.fetch(r, env as Env, ctx);
  await waitOnExecutionContext(ctx);
  return res as Response;
}

async function fetch(r: Request): Promise<Response> {
  r.headers.append("Authorization", usernamePasswordToAuth("hello", "world"));
  return await fetchUnauth(r);
}

describe("v2", () => {
  test("/v2", async () => {
    const response = await fetch(createRequest("GET", "/v2/", null));
    expect(response.status).toBe(200);
  });

  test("GET /v2/:name/blobs/:digest resolves legacy uuid pointer blobs", async () => {
    const name = "ptr-test";
    const data = "hello-layer-bytes";
    const digest = await getSHA256(data);
    const uuid = crypto.randomUUID();
    const bindings = env as Env;

    // actual bytes live at the uuid key
    await bindings.REGISTRY.put(uuid, data, { sha256: digest.slice(SHA256_PREFIX_LEN) });
    // blob key contains just a uuid pointer (legacy layout)
    await bindings.REGISTRY.put(`${name}/blobs/${digest}`, uuid);

    const res = await fetch(createRequest("GET", `/v2/${name}/blobs/${digest}`, null));
    expect(res.ok).toBeTruthy();
    expect(res.headers.get("docker-content-digest")).toBe(digest);
    expect(res.headers.get("content-length")).toBe(`${data.length}`);
    expect(await res.text()).toBe(data);
  });

  test("GET /v2/:name/blobs/:digest resolves legacy reference metadata stubs", async () => {
    const name = "ptr-meta-test";
    const data = "hello-layer-bytes-meta";
    const digest = await getSHA256(data);
    const uuid = crypto.randomUUID();
    const bindings = env as Env;

    await bindings.REGISTRY.put(uuid, data, { sha256: digest.slice(SHA256_PREFIX_LEN) });
    await bindings.REGISTRY.put(`${name}/blobs/${digest}`, uuid, {
      customMetadata: {
        "x-serverless-registry-reference": uuid,
        "x-serverless-registry-digest": digest,
      },
    });

    const res = await fetch(createRequest("GET", `/v2/${name}/blobs/${digest}`, null));
    expect(res.ok).toBeTruthy();
    expect(res.headers.get("docker-content-digest")).toBe(digest);
    expect(res.headers.get("content-length")).toBe(`${data.length}`);
    expect(await res.text()).toBe(data);

    await bindings.REGISTRY.delete(`${name}/blobs/${digest}`);
    await bindings.REGISTRY.delete(uuid);
  });

  test("HEAD /v2/:name/blobs/:digest resolves legacy uuid pointer blobs", async () => {
    const name = "ptr-head-test";
    const data = "hello-layer-bytes-2";
    const digest = await getSHA256(data);
    const uuid = crypto.randomUUID();
    const bindings = env as Env;

    await bindings.REGISTRY.put(uuid, data, { sha256: digest.slice(SHA256_PREFIX_LEN) });
    await bindings.REGISTRY.put(`${name}/blobs/${digest}`, uuid);

    const res = await fetch(createRequest("HEAD", `/v2/${name}/blobs/${digest}`, null));
    expect(res.ok).toBeTruthy();
    expect(res.headers.get("docker-content-digest")).toBe(digest);
    expect(res.headers.get("content-length")).toBe(`${data.length}`);
  });

  test("Username password authenticatiom fails gracefully when wrong format", async () => {
    const res = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: `Basic ${encode("hello")}:${encode("t")}`,
      }),
    );
    expect(res.status).toBe(401);
  });

  test("Username password authenticatiom fails gracefully when password is wrong", async () => {
    const cred = encode(`hello:t`);
    const res = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: `Basic ${cred}`,
      }),
    );
    expect(res.status).toBe(401);
  });

  test("Simple username password authenticatiom fails gracefully when password is wrong", async () => {
    const cred = encode(`hello:t`);
    const res = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: `Basic ${cred}`,
      }),
    );
    expect(res.status).toBe(401);
  });

  test("Simple username password authenticatiom fails gracefully when username is wrong", async () => {
    const cred = encode(`hell0:world`);
    const res = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: `Basic ${cred}`,
      }),
    );
    expect(res.status).toBe(401);
  });

  test("Simple username password authentication", async () => {
    const res = await fetchUnauth(createRequest("GET", `/v2/`, null, {}));
    expect(res.status).toBe(401);
    expect(res.ok).toBeFalsy();
    const resAuth = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: usernamePasswordToAuth("hellO", "worlD"),
      }),
    );
    expect(resAuth.status).toBe(401);
    expect(resAuth.ok).toBeFalsy();
    const resAuthCorrect = await fetchUnauth(
      createRequest("GET", `/v2/`, null, {
        Authorization: usernamePasswordToAuth("hello", "world"),
      }),
    );
    expect(resAuthCorrect.ok).toBeTruthy();
  });
});

async function createManifest(name: string, schema: ManifestSchema, tag?: string): Promise<{ sha256: string }> {
  const data = JSON.stringify(schema);
  const sha256 = await getSHA256(data);
  if (!tag) {
    tag = sha256;
  }

  const res = await fetch(
    createRequest("PUT", `/v2/${name}/manifests/${tag}`, new Blob([data]).stream(), {
      "Content-Type": "application/gzip",
    }),
  );
  if (!res.ok) {
    throw new Error(await res.text());
  }
  expect(res.ok).toBeTruthy();
  expect(res.headers.get("docker-content-digest")).toEqual(sha256);
  return { sha256 };
}

describe("v2 manifests", () => {
  test("HEAD /v2/:name/manifests/:reference NOT FOUND", async () => {
    const response = await fetch(createRequest("GET", "/v2/notfound/manifests/reference", null));
    expect(response.status).toBe(404);
    const json = await response.json();
    expect(json).toEqual({
      errors: [
        {
          code: "MANIFEST_UNKNOWN",
          message: "manifest unknown",
          detail: {
            Tag: "reference",
          },
        },
      ],
    });
  });

  test("HEAD /v2/:name/manifests/:reference works", async () => {
    const reference = "123456";
    const name = "name";
    const data = "{}";
    const sha256 = await getSHA256(data);
    const bindings = env as Env;
    await bindings.REGISTRY.put(`${name}/manifests/${reference}`, "{}", {
      httpMetadata: { contentType: "application/gzip" },
      sha256: sha256.slice(SHA256_PREFIX_LEN),
    });
    const res = await fetch(createRequest("HEAD", `/v2/${name}/manifests/${reference}`, null));
    expect(res.ok).toBeTruthy();
    expect(Object.fromEntries(res.headers)).toEqual({
      "content-length": "2",
      "content-type": "application/gzip",
      "docker-content-digest": sha256,
    });
    await bindings.REGISTRY.delete(`${name}/manifests/${reference}`);
  });

  test("PUT then DELETE /v2/:name/manifests/:reference works", async () => {
    const { sha256 } = await createManifest("hello-world", await generateManifest("hello-world"), "hello");
    const bindings = env as Env;

    {
      const listObjects = await bindings.REGISTRY.list({ prefix: "hello-world/blobs/" });
      expect(listObjects.objects.length).toEqual(1);

      const gcRes = await fetch(new Request("http://registry.com/v2/hello-world/gc", { method: "POST" }));
      if (!gcRes.ok) {
        throw new Error(`${gcRes.status}: ${await gcRes.text()}`);
      }

      const listObjectsAfterGC = await bindings.REGISTRY.list({ prefix: "hello-world/blobs/" });
      expect(listObjectsAfterGC.objects.length).toEqual(1);
    }

    expect(await bindings.REGISTRY.head(`hello-world/manifests/hello`)).toBeTruthy();
    const res = await fetch(createRequest("DELETE", `/v2/hello-world/manifests/${sha256}`, null));
    expect(res.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`hello-world/manifests/${sha256}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`hello-world/manifests/hello`)).toBeNull();

    const listObjects = await bindings.REGISTRY.list({ prefix: "hello-world/blobs/" });
    expect(listObjects.objects.length).toEqual(1);

    const listObjectsManifests = await bindings.REGISTRY.list({ prefix: "hello-world/manifests/" });
    expect(listObjectsManifests.objects.length).toEqual(0);

    const gcRes = await fetch(new Request("http://registry.com/v2/hello-world/gc", { method: "POST" }));
    if (!gcRes.ok) {
      throw new Error(`${gcRes.status}: ${await gcRes.text()}`);
    }

    const listObjectsAfterGC = await bindings.REGISTRY.list({ prefix: "hello-world/blobs/" });
    expect(listObjectsAfterGC.objects.length).toEqual(0);
  });

  test("tag deletion requires the expected digest", async () => {
    const name = "conditional-tag-delete";
    const manifest = await generateManifest(name);
    const { sha256: oldDigest } = await createManifest(name, { ...manifest, annotations: { version: "old" } }, "build");
    const { sha256: currentDigest } = await createManifest(
      name,
      { ...manifest, annotations: { version: "current" } },
      "build",
    );

    const staleDelete = await fetch(
      createRequest("DELETE", `/v2/${name}/manifests/build`, null, {
        "X-Runpod-Expected-Digest": oldDigest,
      }),
    );
    expect(staleDelete.status).toBe(412);

    const current = await fetch(createRequest("HEAD", `/v2/${name}/manifests/build`, null));
    expect(current.headers.get("docker-content-digest")).toBe(currentDigest);

    const currentDelete = await fetch(
      createRequest("DELETE", `/v2/${name}/manifests/build`, null, {
        "X-Runpod-Expected-Digest": currentDigest,
      }),
    );
    expect(currentDelete.status).toBe(202);
  });

  test("deletion claims block tag reads and writes until final deletion", async () => {
    const name = "claimed-tag-delete";
    const manifest = await generateManifest(name);
    const { sha256 } = await createManifest(name, manifest, "build");

    const claimResponse = await fetch(
      createRequest(
        "POST",
        `/v2/_maintenance/${name}/tags/build/claim`,
        new Blob([JSON.stringify({ digest: sha256 })]).stream(),
        { "Content-Type": "application/json" },
      ),
    );
    expect(claimResponse.status).toBe(201);
    const claim = (await claimResponse.json()) as { token: string };

    const claimedHead = await fetch(createRequest("HEAD", `/v2/${name}/manifests/build`, null));
    expect(claimedHead.status).toBe(423);

    const digestDelete = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${sha256}`, null));
    expect(digestDelete.status).toBe(409);

    const replacement = JSON.stringify({ ...manifest, annotations: { version: "replacement" } });
    const claimedPut = await fetch(
      createRequest("PUT", `/v2/${name}/manifests/build`, new Blob([replacement]).stream(), {
        "Content-Type": "application/gzip",
      }),
    );
    expect(claimedPut.status).toBe(409);

    const deleteResponse = await fetch(
      createRequest("DELETE", `/v2/${name}/manifests/build`, null, {
        "X-Runpod-Deletion-Claim": claim.token,
        "X-Runpod-Expected-Digest": sha256,
      }),
    );
    expect(deleteResponse.status).toBe(202);
  });

  test("expired deletion claims do not block digest deletion", async () => {
    const name = "expired-tag-claim";
    const manifest = await generateManifest(name);
    const { sha256 } = await createManifest(name, manifest, "build");
    const bindings = env as Env;
    const claimKey = `${name}/deletion-claims/build`;
    await bindings.REGISTRY.put(claimKey, "expired", {
      customMetadata: {
        token: "expired",
        digest: sha256,
        expiresAt: (Date.now() - DELETION_CLAIM_MAX_AGE_MS).toString(),
      },
    });

    const response = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${sha256}`, null));

    expect(response.status).toBe(202);
    expect(await bindings.REGISTRY.head(claimKey)).toBeNull();
  });

  test("active deletion claims still block digest deletion", async () => {
    const name = "active-tag-claim";
    const manifest = await generateManifest(name);
    const { sha256 } = await createManifest(name, manifest, "build");
    const bindings = env as Env;
    await bindings.REGISTRY.put(`${name}/deletion-claims/build`, "active", {
      customMetadata: {
        token: "active",
        digest: sha256,
        expiresAt: (Date.now() + DELETION_CLAIM_MAX_AGE_MS).toString(),
      },
    });

    const response = await fetch(createRequest("DELETE", `/v2/${name}/manifests/${sha256}`, null));

    expect(response.status).toBe(409);
  });

  test("untagged garbage collection removes digest manifests after tag deletion", async () => {
    const name = "gc-untagged-manifest";
    const manifest = await generateManifest(name);
    const { sha256 } = await createManifest(name, manifest, "archived");
    const bindings = env as Env;

    const deleteResponse = await fetch(createRequest("DELETE", `/v2/${name}/manifests/archived`, null));
    expect(deleteResponse.status).toBe(202);

    const gcResponse = await fetch(createRequest("POST", `/v2/${name}/gc?mode=untagged`, null));
    expect(gcResponse.ok).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${sha256}`)).toBeNull();
  });

  test("untagged garbage collection preserves layers shared by a retained manifest", async () => {
    const name = "gc-shared-layer";
    const manifest = await generateManifest(name);
    if (manifest.schemaVersion !== 2 || "manifests" in manifest) throw new Error("unexpected manifest");
    const { sha256: oldDigest } = await createManifest(name, { ...manifest, annotations: { version: "old" } }, "old");
    const { sha256: currentDigest } = await createManifest(
      name,
      { ...manifest, annotations: { version: "current" } },
      "current",
    );
    const bindings = env as Env;

    const deleteResponse = await fetch(createRequest("DELETE", `/v2/${name}/manifests/old`, null));
    expect(deleteResponse.status).toBe(202);
    const gcResponse = await fetch(createRequest("POST", `/v2/${name}/gc?mode=untagged`, null));
    expect(gcResponse.ok).toBeTruthy();

    expect(await bindings.REGISTRY.head(`${name}/manifests/${oldDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${currentDigest}`)).toBeTruthy();
    expect((await bindings.REGISTRY.list({ prefix: `${name}/blobs/` })).objects).toHaveLength(1);
  });

  test("dry-run garbage collection reports reclaimable bytes and preserves shared layers", async () => {
    const name = "gc-dry-run";
    const manifest = await generateManifest(name);
    const { sha256 } = await createManifest(name, manifest, "first");
    await createManifest(name, manifest, "second");
    const bindings = env as Env;

    const estimate = async (references: string[]) => {
      const response = await fetch(
        createRequest(
          "POST",
          `/v2/${name}/gc?mode=untagged&dry_run=true`,
          new Blob([JSON.stringify({ references })]).stream(),
          { "Content-Type": "application/json" },
        ),
      );
      expect(response.ok).toBeTruthy();
      return (await response.json()) as {
        success: boolean;
        objectCount: number;
        bytes: number;
      };
    };

    const sharedEstimate = await estimate(["first"]);
    expect(sharedEstimate).toEqual({
      success: true,
      objectCount: 1,
      bytes: JSON.stringify(manifest).length,
    });

    const fullEstimate = await estimate(["first", "second"]);
    expect(fullEstimate.success).toBeTruthy();
    expect(fullEstimate.objectCount).toBe(4);
    expect(fullEstimate.bytes).toBeGreaterThan(sharedEstimate.bytes);
    expect(await bindings.REGISTRY.head(`${name}/manifests/first`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/manifests/second`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${sha256}`)).toBeTruthy();
    expect((await bindings.REGISTRY.list({ prefix: `${name}/blobs/` })).objects).toHaveLength(1);
  });

  test("garbage collection preserves config blobs and removes legacy pointer targets", async () => {
    const name = "gc-image-retention";
    const bindings = env as Env;
    const configData = "distinct-config";
    const layerData = "retained-layer";
    const orphanData = "orphaned-layer";
    const legacyData = "legacy-orphaned-layer";
    const configDigest = await getSHA256(configData);
    const layerDigest = await getSHA256(layerData);
    const orphanDigest = await getSHA256(orphanData);
    const legacyDigest = await getSHA256(legacyData);
    const legacyKey = crypto.randomUUID();

    await bindings.REGISTRY.put(`${name}/blobs/${configDigest}`, configData, {
      sha256: configDigest.slice(SHA256_PREFIX_LEN),
    });
    await bindings.REGISTRY.put(`${name}/blobs/${layerDigest}`, layerData, {
      sha256: layerDigest.slice(SHA256_PREFIX_LEN),
    });
    await bindings.REGISTRY.put(`${name}/blobs/${orphanDigest}`, orphanData, {
      sha256: orphanDigest.slice(SHA256_PREFIX_LEN),
    });
    await bindings.REGISTRY.put(legacyKey, legacyData, {
      sha256: legacyDigest.slice(SHA256_PREFIX_LEN),
    });
    await bindings.REGISTRY.put(`${name}/blobs/${legacyDigest}`, legacyKey, {
      customMetadata: { "x-serverless-registry-reference": legacyKey },
    });

    await createManifest(
      name,
      {
        schemaVersion: 2,
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        config: {
          mediaType: "application/vnd.oci.image.config.v1+json",
          digest: configDigest,
          size: configData.length,
        },
        layers: [
          {
            mediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
            digest: layerDigest,
            size: layerData.length,
          },
        ],
      },
      "current",
    );

    const gcRes = await fetch(createRequest("POST", `/v2/${name}/gc?mode=untagged`, null));
    expect(gcRes.ok).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/blobs/${configDigest}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/blobs/${layerDigest}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/blobs/${orphanDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/blobs/${legacyDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(legacyKey)).toBeNull();
  });

  test("untagged garbage collection preserves OCI referrers", async () => {
    const name = "gc-referrers";
    const manifest = await generateManifest(name);
    const { sha256: subjectDigest } = await createManifest(name, manifest, "current");
    if (manifest.schemaVersion !== 2 || "manifests" in manifest) throw new Error("unexpected manifest");
    const { sha256: referrerDigest } = await createManifest(name, {
      ...manifest,
      artifactType: "application/vnd.example.signature",
      subject: {
        mediaType: manifest.mediaType,
        digest: subjectDigest,
        size: JSON.stringify(manifest).length,
      },
    });
    const bindings = env as Env;

    const firstGC = await fetch(createRequest("POST", `/v2/${name}/gc?mode=untagged`, null));
    expect(firstGC.ok).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${subjectDigest}`)).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${referrerDigest}`)).toBeTruthy();

    const deleteResponse = await fetch(createRequest("DELETE", `/v2/${name}/manifests/current`, null));
    expect(deleteResponse.status).toBe(202);
    const secondGC = await fetch(createRequest("POST", `/v2/${name}/gc?mode=untagged`, null));
    expect(secondGC.ok).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${subjectDigest}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`${name}/manifests/${referrerDigest}`)).toBeNull();
  });

  test("garbage collection preserves recently uploaded blobs", async () => {
    const name = "gc-recent-upload";
    const data = "recent-upload-data";
    const digest = await getSHA256(data);
    const bindings = env as Env;
    const configuredMinimumAge = bindings.GC_MINIMUM_OBJECT_AGE_MS;
    bindings.GC_MINIMUM_OBJECT_AGE_MS = `${60 * 60 * 1000}`;
    try {
      await bindings.REGISTRY.put(`${name}/blobs/${digest}`, data, {
        sha256: digest.slice(SHA256_PREFIX_LEN),
      });
      const response = await fetch(createRequest("POST", `/v2/${name}/gc?mode=untagged`, null));
      expect(response.ok).toBeTruthy();
      expect(await bindings.REGISTRY.head(`${name}/blobs/${digest}`)).toBeTruthy();
    } finally {
      bindings.GC_MINIMUM_OBJECT_AGE_MS = configuredMinimumAge;
      await bindings.REGISTRY.delete(`${name}/blobs/${digest}`);
    }
  });

  test("garbage collection preserves blobs used by active direct uploads", async () => {
    const name = "gc-active-upload";
    const data = "active-upload-data";
    const digest = await getSHA256(data);
    const bindings = env as Env;
    await bindings.REGISTRY.put(`${name}/blobs/${digest}`, data, {
      sha256: digest.slice(SHA256_PREFIX_LEN),
    });
    await encodeState(
      {
        parts: [],
        chunks: [],
        uploadId: "",
        registryUploadId: "active-upload",
        byteRange: 0,
        name,
        direct: { objectKey: `${name}/blobs/${digest}`, expectedDigest: digest },
      },
      bindings,
    );

    const firstGC = await fetch(createRequest("POST", `/v2/${name}/gc?mode=untagged`, null));
    expect(firstGC.ok).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/blobs/${digest}`)).toBeTruthy();

    await bindings.REGISTRY.delete(`${name}/uploads/active-upload`);
    const secondGC = await fetch(createRequest("POST", `/v2/${name}/gc?mode=untagged`, null));
    expect(secondGC.ok).toBeTruthy();
    expect(await bindings.REGISTRY.head(`${name}/blobs/${digest}`)).toBeNull();
  });

  test("PUT multiple parts then DELETE /v2/:name/manifests/:reference works", async () => {
    const { sha256 } = await createManifest("hello/world", await generateManifest("hello/world"), "hello");
    const bindings = env as Env;
    expect(await bindings.REGISTRY.head(`hello/world/manifests/hello`)).toBeTruthy();
    const res = await fetch(createRequest("DELETE", `/v2/hello/world/manifests/${sha256}`, null));
    expect(res.status).toEqual(202);
    expect(await bindings.REGISTRY.head(`hello/world/manifests/${sha256}`)).toBeNull();
    expect(await bindings.REGISTRY.head(`hello/world/manifests/hello`)).toBeNull();
  });

  test("PUT then list tags with GET /v2/:name/tags/list", async () => {
    const { sha256 } = await createManifest("hello-world-list", await generateManifest("hello-world-list"), `hello`);
    const expectedRes = ["hello", sha256];
    for (let i = 0; i < 50; i++) {
      expectedRes.push(`hello-${i}`);
    }

    expectedRes.sort();
    const shuffledRes = shuffleArray([...expectedRes]);
    for (const tag of shuffledRes) {
      await createManifest("hello-world-list", await generateManifest("hello-world-list"), tag);
    }

    const tagsRes = await fetch(createRequest("GET", `/v2/hello-world-list/tags/list?n=1000`, null));
    const tags = (await tagsRes.json()) as TagsList;
    expect(tags.name).toEqual("hello-world-list");
    expect(tags.tags).toEqual(expectedRes);

    const res = await fetch(createRequest("DELETE", `/v2/hello-world-list/manifests/${sha256}`, null));
    expect(res.ok).toBeTruthy();
    const tagsResEmpty = await fetch(createRequest("GET", `/v2/hello-world-list/tags/list`, null));
    const tagsEmpty = (await tagsResEmpty.json()) as TagsList;
    expect(tagsEmpty.tags).toHaveLength(0);
  });
});

describe("tokens", async () => {
  test("auth payload push on /v2", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("GET", "/v2/", null), {
      capabilities: ["push"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeTruthy();
  });

  test("auth payload pull on /v2", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("GET", "/v2/", null), {
      capabilities: ["pull"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeTruthy();
  });

  test("auth payload without capabilities cannot GET registry base paths", async () => {
    for (const url of ["https://registry.runpod.net/", "https://registry.com/v2/"]) {
      const { verified } = RegistryTokens.verifyPayload(new Request(url), {
        username: "test",
        capabilities: [],
        exp: Math.floor(Date.now() / 1000) + 60,
        aud: url,
      });
      expect(verified).toBeFalsy();
    }
  });

  test("auth payload push can GET the registry root on any host", async () => {
    for (const url of ["https://registry.runpod.net/", "https://registry.example/?check=1"]) {
      const { verified } = RegistryTokens.verifyPayload(new Request(url), {
        username: "test",
        capabilities: ["push"],
        exp: Math.floor(Date.now() / 1000) + 60,
        aud: url,
      });
      expect(verified).toBeTruthy();
    }
  });

  test("auth payload push on /v2/whatever with HEAD", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("HEAD", "/v2/whatever", null), {
      capabilities: ["push"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeTruthy();
  });

  test("auth payload push on /v2/whatever with push mutations", async () => {
    for (const mutationMethod of ["PATCH", "POST"]) {
      const { verified } = RegistryTokens.verifyPayload(createRequest(mutationMethod, "/v2/whatever", null), {
        capabilities: ["push"],
      } as RegistryAuthProtocolTokenPayload);
      expect(verified).toBeTruthy();
    }
  });

  test("auth payload push without delete cannot DELETE", async () => {
    const { verified } = RegistryTokens.verifyPayload(
      createRequest("DELETE", "/v2/whatever/manifests/sha256:abc", null),
      {
        capabilities: ["push"],
      } as RegistryAuthProtocolTokenPayload,
    );
    expect(verified).toBeFalsy();
  });

  test("auth payload push without delete cannot run gc", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("POST", "/v2/whatever/gc", null), {
      capabilities: ["push"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeFalsy();
  });

  test("auth payload delete can DELETE and run gc", async () => {
    const del = RegistryTokens.verifyPayload(createRequest("DELETE", "/v2/whatever/manifests/sha256:abc", null), {
      capabilities: ["delete"],
    } as RegistryAuthProtocolTokenPayload);
    expect(del.verified).toBeTruthy();

    const gc = RegistryTokens.verifyPayload(createRequest("POST", "/v2/whatever/gc", null), {
      capabilities: ["delete"],
    } as RegistryAuthProtocolTokenPayload);
    expect(gc.verified).toBeTruthy();
  });

  test("auth payload delete scoped to another image cannot DELETE", async () => {
    const { verified } = RegistryTokens.verifyPayload(
      createRequest("DELETE", "/v2/whatever/manifests/sha256:abc", null),
      {
        capabilities: ["delete"],
        imageName: "someotherimage",
      } as RegistryAuthProtocolTokenPayload,
    );
    expect(verified).toBeFalsy();
  });

  test("auth payload delete scoped to the same image can DELETE", async () => {
    const { verified } = RegistryTokens.verifyPayload(
      createRequest("DELETE", "/v2/whatever/manifests/sha256:abc", null),
      {
        capabilities: ["delete"],
        imageName: "whatever",
      } as RegistryAuthProtocolTokenPayload,
    );
    expect(verified).toBeTruthy();
  });

  test("auth payload pull on /v2/whatever with mutations", async () => {
    for (const mutationMethod of ["PATCH", "POST", "DELETE"]) {
      const { verified } = RegistryTokens.verifyPayload(createRequest(mutationMethod, "/v2/whatever", null), {
        capabilities: ["pull"],
      } as RegistryAuthProtocolTokenPayload);
      expect(verified).toBeFalsy();
    }
  });

  test("auth payload push/pull on /v2/whatever with push mutations", async () => {
    for (const mutationMethod of ["PATCH", "POST"]) {
      const { verified } = RegistryTokens.verifyPayload(createRequest(mutationMethod, "/v2/whatever", null), {
        capabilities: ["pull", "push"],
      } as RegistryAuthProtocolTokenPayload);
      expect(verified).toBeTruthy();
    }
  });

  test("auth payload pull without delete cannot read maintenance inventory", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("GET", "/v2/_maintenance/repositories", null), {
      capabilities: ["pull"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeFalsy();
  });

  test("auth payload pull and delete can use maintenance routes", async () => {
    for (const method of ["GET", "POST", "DELETE"]) {
      const { verified } = RegistryTokens.verifyPayload(
        createRequest(method, "/v2/_maintenance/whatever/tags/build/claim", null),
        {
          capabilities: ["pull", "delete"],
        } as RegistryAuthProtocolTokenPayload,
      );
      expect(verified).toBeTruthy();
    }
  });

  test("auth payload push on GET", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("GET", "/v2/whatever", null), {
      capabilities: ["push"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeFalsy();
  });

  test("auth payload push/pull on GET", async () => {
    const { verified } = RegistryTokens.verifyPayload(createRequest("GET", "/v2/whatever", null), {
      capabilities: ["push", "pull"],
    } as RegistryAuthProtocolTokenPayload);
    expect(verified).toBeTruthy();
  });
});

test("registries configuration", async () => {
  const testCases = [
    {
      configuration: undefined,
      expected: [],
      error: "",
      partialError: false,
    },
    {
      configuration: "[]",
      expected: [],
      error: "",
      partialError: false,
    },
    {
      configuration: "{}",
      expected: [],
      error: "Error parsing registries JSON: zod error: - invalid_type: Expected array, received object: ",
      partialError: false,
    },
    {
      configuration: "[{}]",
      expected: [],
      error:
        "Error parsing registries JSON: zod error: - invalid_type: Required: 0,registry\n\t- invalid_type: Required: 0,password_env\n\t- invalid_type: Required: 0,username",
      partialError: false,
    },
    {
      configuration: `[{ "registry": "no-url/hello-world" }]`,
      expected: [],
      error:
        "Error parsing registries JSON: zod error: - invalid_string: Invalid url: 0,registry\n\t- invalid_type: Required: 0,password_env\n\t- invalid_type: Required: 0,username",
      partialError: false,
    },
    {
      configuration: "bla bla bla no json",
      expected: [],
      error: "Error parsing registries JSON: error SyntaxError: Unexpected token",
      partialError: true,
    },
    {
      configuration: `[{
        "registry": "https://hello.com/domain",
        "username": "hello world",
        "password_env": "PASSWORD_ENV"
      }]`,
      expected: [
        {
          registry: "https://hello.com/domain",
          username: "hello world",
          password_env: "PASSWORD_ENV",
        },
      ],
      partialError: false,
      error: "",
    },
    {
      configuration: `[{
        "registry": "https://hello.com/domain",
        "username": "hello world",
        "password_env": "PASSWORD_ENV"
      }, {
        "registry": "https://hello2.com/domain",
        "username": "hello world 2",
        "password_env": "PASSWORD_ENV 2"
      }]`,
      expected: [
        {
          registry: "https://hello.com/domain",
          username: "hello world",
          password_env: "PASSWORD_ENV",
        },
        {
          registry: "https://hello2.com/domain",
          username: "hello world 2",
          password_env: "PASSWORD_ENV 2",
        },
      ],
      partialError: false,
      error: "",
    },
  ] as const;

  const bindings = env as Env;
  const bindingCopy = { ...bindings };
  for (const testCase of testCases) {
    bindingCopy.REGISTRIES_JSON = testCase.configuration;
    const expectErrorOutput = testCase.error !== "";
    let calledError = false;
    const prevConsoleError = console.error;
    console.error = (output) => {
      if (!testCase.partialError) {
        expect(output).toEqual(testCase.error);
      } else {
        expect(output).toContain(testCase.error);
      }

      calledError = true;
    };
    const r = registries(bindingCopy);
    expect(r).toEqual(testCase.expected);
    expect(calledError).toEqual(expectErrorOutput);
    console.error = prevConsoleError;
  }
});

describe("maintenance inventory", () => {
  test("lists repositories without walking every blob", async () => {
    const bindings = env as Env;
    const orphanObject = crypto.randomUUID();
    await bindings.REGISTRY.put(orphanObject, "legacy-layer");
    await createManifest("maintenance-repo-a", await generateManifest("maintenance-repo-a"), "build-a");
    await createManifest("maintenance-repo-b", await generateManifest("maintenance-repo-b"), "build-b");

    const repositories: string[] = [];
    let cursor: string | undefined;
    do {
      const query = new URLSearchParams({ limit: "1" });
      if (cursor) query.set("cursor", cursor);
      const response = await fetch(createRequest("GET", `/v2/_maintenance/repositories?${query}`, null));
      expect(response.ok).toBeTruthy();
      const page = (await response.json()) as { repositories: string[]; cursor?: string };
      repositories.push(...page.repositories);
      cursor = page.cursor;
    } while (cursor && !repositories.includes("maintenance-repo-b"));

    expect(repositories).toContain("maintenance-repo-a");
    expect(repositories).toContain("maintenance-repo-b");
  });

  test("lists tag timestamps for a repository", async () => {
    await createManifest("maintenance-repo-a", await generateManifest("maintenance-repo-a"), "build-a");
    const response = await fetch(createRequest("GET", "/v2/_maintenance/maintenance-repo-a/tags", null));
    expect(response.ok).toBeTruthy();
    const page = (await response.json()) as {
      tags: Array<{ reference: string; digest: string; uploadedAt: string }>;
    };

    expect(page.tags).toEqual([
      expect.objectContaining({
        reference: "build-a",
        digest: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
        uploadedAt: expect.any(String),
      }),
    ]);
    expect(new Date(page.tags[0].uploadedAt).toString()).not.toBe("Invalid Date");
  });
});

describe("http client", () => {
  const bindings = env as Env;
  let envBindings = { ...bindings };
  const prevFetch = global.fetch;

  afterAll(() => {
    global.fetch = prevFetch;
  });

  test("test manifest exists", async () => {
    envBindings = { ...bindings };
    envBindings.JWT_REGISTRY_TOKENS_PUBLIC_KEY = "";
    envBindings.PASSWORD = "world";
    envBindings.USERNAME = "hello";
    envBindings.REGISTRIES_JSON = undefined;
    global.fetch = async function (r: RequestInfo | URL): Promise<Response> {
      return fetch(new Request(r));
    };
    const client = new RegistryHTTPClient(envBindings, {
      registry: "https://localhost",
      password_env: "PASSWORD",
      username: "hello",
    });
    const res = await client.manifestExists("namespace/hello", "latest");
    if ("response" in res) {
      expect(await res.response.json()).toEqual({ status: res.response.status });
    }

    expect("exists" in res && res.exists).toBe(false);
  });
});

describe("push and catalog", () => {
  test("push and then use the catalog", async () => {
    await createManifest("hello-world-main", await generateManifest("hello-world-main"), "hello");
    await createManifest("hello-world-main", await generateManifest("hello-world-main"), "latest");
    await createManifest("hello-world-main", await generateManifest("hello-world-main"), "hello-2");
    await createManifest("hello", await generateManifest("hello"), "hello");
    await createManifest("hello/hello", await generateManifest("hello/hello"), "hello");

    const response = await fetch(createRequest("GET", "/v2/_catalog", null));
    expect(response.ok).toBeTruthy();
    const body = (await response.json()) as { repositories: string[] };
    expect(body).toEqual({
      repositories: ["hello-world-main", "hello/hello", "hello"],
    });
    const expectedRepositories = body.repositories;
    const tagsRes = await fetch(createRequest("GET", `/v2/hello-world-main/tags/list?n=1000`, null));
    const tags = (await tagsRes.json()) as TagsList;
    expect(tags.name).toEqual("hello-world-main");
    expect(tags.tags).toEqual([
      "hello",
      "hello-2",
      "latest",
      "sha256:a8a29b609fa044cf3ee9a79b57a6fbfb59039c3e9c4f38a57ecb76238bf0dec6",
    ]);

    const repositoryBuildUp: string[] = [];
    let currentPath = "/v2/_catalog?n=1";
    for (let i = 0; i < 3; i++) {
      const response = await fetch(createRequest("GET", currentPath, null));
      expect(response.ok).toBeTruthy();
      const body = (await response.json()) as { repositories: string[] };
      if (body.repositories.length === 0) {
        break;
      }
      expect(body.repositories).toHaveLength(1);

      repositoryBuildUp.push(...body.repositories);
      const url = new URL(response.headers.get("Link")!.split(";")[0].trim());
      currentPath = url.pathname + url.search;
    }

    expect(repositoryBuildUp).toEqual(expectedRepositories);
  });

  test("(v1) push and then use the catalog", async () => {
    await createManifest("hello-world-main", await generateManifest("hello-world-main", 1), "hello");
    await createManifest("hello-world-main", await generateManifest("hello-world-main", 1), "latest");
    await createManifest("hello-world-main", await generateManifest("hello-world-main", 1), "hello-2");
    await createManifest("hello", await generateManifest("hello", 1), "hello");
    await createManifest("hello/hello", await generateManifest("hello/hello", 1), "hello");

    const response = await fetch(createRequest("GET", "/v2/_catalog", null));
    expect(response.ok).toBeTruthy();
    const body = (await response.json()) as { repositories: string[] };
    expect(body).toEqual({
      repositories: ["hello-world-main", "hello/hello", "hello"],
    });
    const expectedRepositories = body.repositories;
    const tagsRes = await fetch(createRequest("GET", `/v2/hello-world-main/tags/list?n=1000`, null));
    const tags = (await tagsRes.json()) as TagsList;
    expect(tags.name).toEqual("hello-world-main");
    expect(tags.tags).toEqual([
      "hello",
      "hello-2",
      "latest",
      "sha256:a70525d2dd357c6ece8d9e0a5a232e34ca3bbceaa1584d8929cdbbfc81238210",
    ]);

    const repositoryBuildUp: string[] = [];
    let currentPath = "/v2/_catalog?n=1";
    for (let i = 0; i < 3; i++) {
      const response = await fetch(createRequest("GET", currentPath, null));
      expect(response.ok).toBeTruthy();
      const body = (await response.json()) as { repositories: string[] };
      if (body.repositories.length === 0) {
        break;
      }
      expect(body.repositories).toHaveLength(1);

      repositoryBuildUp.push(...body.repositories);
      const url = new URL(response.headers.get("Link")!.split(";")[0].trim());
      currentPath = url.pathname + url.search;
    }

    expect(repositoryBuildUp).toEqual(expectedRepositories);
  });
});
