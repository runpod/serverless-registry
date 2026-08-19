import { Router } from "itty-router";
import { BlobUnknownError, ManifestUnknownError } from "./v2-errors";
import { InternalError, ServerError } from "./errors";
import { errorString, jsonHeaders, wrap } from "./utils";
import { hexToDigest } from "./user";
import { ManifestTagsListTooBigError } from "./v2-responses";
import { Env } from "..";
import { MINIMUM_CHUNK, MAXIMUM_CHUNK, MAXIMUM_CHUNK_UPLOAD_SIZE } from "./chunk";
import {
  CheckLayerResponse,
  CheckManifestResponse,
  DirectUploadInfo,
  FinishedUploadObject,
  GetLayerResponse,
  GetManifestResponse,
  PutManifestResponse,
  RegistryError,
  UploadObject,
  registries,
} from "./registry/registry";
import { RegistryHTTPClient } from "./registry/http";
import { deletionClaimIsActive } from "./registry/r2";

const v2Router = Router({ base: "/v2/" });
const TAG_REFERENCE_PATTERN = /^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$/;
const DIGEST_PATTERN = /^sha256:[a-f0-9]{64}$/;

v2Router.get("/", async (_req, _env: Env) => {
  return new Response();
});

v2Router.get("/_maintenance/repositories", async (req, env: Env) => {
  const requestedLimit = Number(req.query.limit ?? 100);
  const limit = Number.isFinite(requestedLimit) ? Math.min(Math.max(requestedLimit, 1), 1000) : 100;
  let cursor = req.query.cursor?.toString();
  const repositories = new Set<string>();
  let pages = 0;

  do {
    const page = await env.REGISTRY.list({
      delimiter: "/",
      limit: Math.max(1, limit - repositories.size),
      cursor,
    });
    page.delimitedPrefixes.forEach((prefix) => repositories.add(prefix.replace(/\/$/, "")));
    cursor = page.truncated ? page.cursor : undefined;
    pages++;
  } while (cursor && repositories.size < limit && pages < 50);

  return new Response(
    JSON.stringify({
      repositories: [...repositories],
      cursor,
    }),
    { headers: jsonHeaders() },
  );
});

v2Router.get("/_maintenance/:name+/tags", async (req, env: Env) => {
  const requestedLimit = Number(req.query.limit ?? 1000);
  const limit = Number.isFinite(requestedLimit) ? Math.min(Math.max(requestedLimit, 1), 1000) : 1000;
  const { name } = req.params;
  const page = await env.REGISTRY.list({
    prefix: `${name}/manifests/`,
    limit,
    cursor: req.query.cursor?.toString(),
  });
  const tags = page.objects.flatMap((object) => {
    const reference = object.key.slice(`${name}/manifests/`.length);
    if (!TAG_REFERENCE_PATTERN.test(reference) || !object.checksums.sha256) return [];
    return [
      {
        reference,
        digest: hexToDigest(object.checksums.sha256),
        uploadedAt: object.uploaded.toISOString(),
      },
    ];
  });

  return new Response(
    JSON.stringify({
      tags,
      cursor: page.truncated ? page.cursor : undefined,
    }),
    { headers: jsonHeaders() },
  );
});

v2Router.post("/_maintenance/:name+/tags/:reference/claim", async (req, env: Env) => {
  const { name, reference } = req.params;
  if (!TAG_REFERENCE_PATTERN.test(reference)) {
    return new Response(JSON.stringify({ error: "invalid tag reference" }), {
      status: 400,
      headers: jsonHeaders(),
    });
  }

  const body = await req.json<{ digest?: unknown }>().catch(() => null);
  if (!body || typeof body.digest !== "string" || !DIGEST_PATTERN.test(body.digest)) {
    return new Response(JSON.stringify({ error: "invalid expected digest" }), {
      status: 400,
      headers: jsonHeaders(),
    });
  }

  const result = await env.REGISTRY_CLIENT.claimManifestTag(name, reference, body.digest);
  if (!result.claimed) {
    const status = result.reason === "not_found" ? 404 : result.reason === "digest_mismatch" ? 412 : 409;
    return new Response(JSON.stringify({ error: result.reason }), { status, headers: jsonHeaders() });
  }
  return new Response(JSON.stringify({ token: result.token }), { status: 201, headers: jsonHeaders() });
});

v2Router.delete("/_maintenance/:name+/tags/:reference/claim", async (req, env: Env) => {
  const token = req.headers.get("X-Runpod-Deletion-Claim");
  if (!token) {
    return new Response(JSON.stringify({ error: "missing deletion claim" }), {
      status: 400,
      headers: jsonHeaders(),
    });
  }
  const released = await env.REGISTRY_CLIENT.releaseManifestTagClaim(req.params.name, req.params.reference, token);
  return new Response(null, { status: released ? 204 : 409 });
});

v2Router.get("/_catalog", async (req, env: Env) => {
  const { n, last } = req.query;
  const response = await env.REGISTRY_CLIENT.listRepositories(
    n ? parseInt(n?.toString()) : undefined,
    last?.toString(),
  );
  if ("response" in response) {
    return response.response;
  }

  const url = new URL(req.url);
  return new Response(
    JSON.stringify({
      repositories: response.repositories,
    }),
    {
      headers: {
        Link: `${url.protocol}//${url.hostname}${url.pathname}?n=${n ?? 1000}&last=${response.cursor ?? ""}; rel=next`,
      },
    },
  );
});

v2Router.delete("/:name+/manifests/:reference", async (req, env: Env) => {
  const { last, limit } = req.query;
  const { name, reference } = req.params;

  if (!reference.startsWith("sha256:")) {
    const expectedDigest = req.headers.get("X-Runpod-Expected-Digest") ?? undefined;
    if (expectedDigest && !DIGEST_PATTERN.test(expectedDigest)) {
      return new Response(JSON.stringify({ error: "invalid expected digest" }), {
        status: 400,
        headers: jsonHeaders(),
      });
    }

    const claimToken = req.headers.get("X-Runpod-Deletion-Claim") ?? undefined;
    const result = await env.REGISTRY_CLIENT.deleteManifestTag(name, reference, expectedDigest, claimToken);
    if (!result.deleted) {
      const status = result.reason === "not_found" ? 404 : result.reason === "digest_mismatch" ? 412 : 409;
      return new Response(JSON.stringify({ error: result.reason }), { status, headers: jsonHeaders() });
    }
    return new Response("", {
      status: 202,
      headers: { "Content-Length": "None" },
    });
  }

  const manifest = await env.REGISTRY.head(`${name}/manifests/${reference}`);
  if (manifest === null) {
    return new Response(JSON.stringify(ManifestUnknownError(reference)), { status: 404, headers: jsonHeaders() });
  }
  let claimCursor: string | undefined;
  do {
    const claims = await env.REGISTRY.list({
      prefix: `${name}/deletion-claims/`,
      limit: 100,
      cursor: claimCursor,
      include: ["customMetadata"],
    } as unknown as R2ListOptions);
    const expiredClaims: string[] = [];
    for (const claim of claims.objects) {
      if (deletionClaimIsActive(claim)) {
        return new Response(JSON.stringify({ error: "manifest tag deletion is in progress" }), {
          status: 409,
          headers: jsonHeaders(),
        });
      }
      expiredClaims.push(claim.key);
    }
    if (expiredClaims.length > 0) await env.REGISTRY.delete(expiredClaims);
    claimCursor = claims.truncated ? claims.cursor : undefined;
  } while (claimCursor);

  const limitInt = parseInt(limit?.toString() ?? "1000", 10);
  const tags = await env.REGISTRY.list({
    prefix: `${name}/manifests`,
    limit: isNaN(limitInt) ? 1000 : limitInt,
    startAfter: last?.toString(),
  });
  for (const tag of tags.objects) {
    if (!tag.checksums.sha256) {
      continue;
    }

    if (hexToDigest(tag.checksums.sha256) === reference) {
      await env.REGISTRY.delete(tag.key);
    }
  }

  if (tags.truncated) {
    return new Response(JSON.stringify(ManifestTagsListTooBigError), {
      status: 400,
      headers: {
        "Link": `${req.url}/?last=${tags.truncated ? tags.cursor : ""}; rel=next`,
        "Content-Type": "application/json",
      },
    });
  }

  // Last but not least, delete the digest manifest
  await env.REGISTRY.delete(`${name}/manifests/${reference}`);
  return new Response("", {
    status: 202,
    headers: {
      "Content-Length": "None",
    },
  });
});

v2Router.head("/:name+/manifests/:reference", async (req, env: Env) => {
  const { name, reference } = req.params;
  const res = await env.REGISTRY_CLIENT.manifestExists(name, reference);
  if ("exists" in res && res.exists) {
    return new Response(null, {
      headers: {
        "Content-Length": res.size.toString(),
        "Content-Type": res.contentType,
        "Docker-Content-Digest": res.digest,
      },
    });
  }

  if ("response" in res && res.response.status === 423) return res.response;

  let checkManifestResponse: CheckManifestResponse | null = null;
  const registryList = registries(env);
  for (const registry of registryList) {
    const client = new RegistryHTTPClient(env, registry);
    const response = await client.manifestExists(name, reference);
    if ("response" in response) {
      continue;
    }

    if (response.exists) {
      checkManifestResponse = {
        size: response.size,
        digest: response.digest,
        contentType: response.contentType,
        exists: true,
      };

      // If the error is that it doesn't exist
      if ("exists" in res && !res.exists) {
        const manifestResponse = await client.getManifest(name, response.digest);
        if ("response" in manifestResponse) {
          console.warn(
            "Can't sync with fallback registry because it has returned an error:",
            manifestResponse.response.status,
          );
          break;
        }

        const [putResponse, err] = await wrap(
          env.REGISTRY_CLIENT.putManifest(name, reference, manifestResponse.stream, manifestResponse.contentType),
        );
        if (err) {
          console.error("Error sync manifest into client:", errorString(err));
        }

        if (putResponse && "response" in putResponse) {
          console.error("Error sync manifest into client (non 200 status):", putResponse.response.status);
        }
      }

      break;
    }
  }

  if (checkManifestResponse === null || !checkManifestResponse.exists)
    return new Response(JSON.stringify(ManifestUnknownError(reference)), { status: 404, headers: jsonHeaders() });

  return new Response(null, {
    headers: {
      "Content-Length": checkManifestResponse.size.toString(),
      "Content-Type": checkManifestResponse.contentType,
      "Docker-Content-Digest": checkManifestResponse.digest,
    },
  });
});

v2Router.get("/:name+/manifests/:reference", async (req, env: Env, context: ExecutionContext) => {
  const { name, reference } = req.params;
  const res = await env.REGISTRY_CLIENT.getManifest(name, reference);
  if (!("response" in res)) {
    return new Response(res.stream, {
      headers: {
        "Content-Length": res.size.toString(),
        "Content-Type": res.contentType,
        "Docker-Content-Digest": res.digest,
      },
    });
  }

  if ("response" in res && res.response.status === 423) return res.response;

  let getManifestResponse: GetManifestResponse | null = null;
  const registriesList = registries(env);
  for (const registry of registriesList) {
    const client = new RegistryHTTPClient(env, registry);
    const response = await client.getManifest(name, reference);
    if ("response" in response) {
      continue;
    }

    getManifestResponse = response;
    if (res.response.status !== 404) {
      // Don't upload the manifest if there is an error
      break;
    }

    const [s1, s2] = getManifestResponse.stream.tee();
    getManifestResponse.stream = s1;
    context.waitUntil(
      (async () => {
        const [response, err] = await wrap(
          env.REGISTRY_CLIENT.putManifest(name, reference, s2, getManifestResponse.contentType),
        );
        if (err) {
          console.error("Error uploading asynchronously the manifest ", reference, "into main registry");
          return;
        }

        if (response && "response" in response) {
          console.error("Error uploading asynchronously manifest:", response.response.status);
        }
      })(),
    );
    break;
  }

  if (getManifestResponse === null)
    return new Response(JSON.stringify(ManifestUnknownError(reference)), { status: 404, headers: jsonHeaders() });

  return new Response(getManifestResponse.stream, {
    headers: {
      "Content-Length": getManifestResponse.size.toString(),
      "Content-Type": getManifestResponse.contentType,
      "Docker-Content-Digest": getManifestResponse.digest,
    },
  });
});

v2Router.put("/:name+/manifests/:reference", async (req, env: Env) => {
  if (!req.headers.get("Content-Type")) {
    throw new ServerError("Content type not defined", 400);
  }

  const { name, reference } = req.params;
  const [res, err] = await wrap<PutManifestResponse | RegistryError, Error>(
    env.REGISTRY_CLIENT.putManifest(name, reference, req.body!, req.headers.get("Content-Type")!),
  );
  if (err) {
    console.error("Error putting manifest:", errorString(err));
    return new InternalError();
  }

  if ("response" in res) {
    return res.response;
  }

  return new Response(null, {
    status: 201,
    headers: {
      "Location": res.location,
      "Docker-Content-Digest": res.digest,
    },
  });
});

v2Router.get("/:name+/blobs/:digest", async (req, env: Env, context: ExecutionContext) => {
  const { name, digest } = req.params;
  const res = await env.REGISTRY_CLIENT.getLayer(name, digest);
  if (!("response" in res)) {
    return new Response(res.stream, {
      headers: {
        "Docker-Content-Digest": res.digest,
        "Content-Length": `${res.size}`,
      },
    });
  }

  let layerResponse: GetLayerResponse | null = null;
  const registriesList = registries(env);
  for (const registry of registriesList) {
    const client = new RegistryHTTPClient(env, registry);
    const response = await client.getLayer(name, digest);
    if ("response" in response) {
      continue;
    }

    layerResponse = response;
    const [s1, s2] = layerResponse.stream.tee();
    layerResponse.stream = s1;
    context.waitUntil(
      (async () => {
        const [response, err] = await wrap(env.REGISTRY_CLIENT.monolithicUpload(name, digest, s2, layerResponse.size));
        if (err) {
          console.error("Error uploading asynchronously the layer ", digest, "into main registry");
          return;
        }

        if (response === false) {
          console.error("Layer might be too big for the registry client", layerResponse.size);
        }
      })(),
    );
    break;
  }

  if (layerResponse === null) return new Response(JSON.stringify(BlobUnknownError), { status: 404 });

  return new Response(layerResponse.stream, {
    headers: {
      "Docker-Content-Digest": layerResponse.digest,
      "Content-Length": `${layerResponse.size}`,
    },
  });
});

v2Router.delete("/:name+/blobs/uploads/:id", async (req, env: Env) => {
  const { name, id } = req.params;
  const [res, err] = await wrap<true | RegistryError, Error>(env.REGISTRY_CLIENT.cancelUpload(name, id));
  if (err) {
    console.error("Error cancelling upload:", errorString(err));
    return new InternalError();
  }

  if (res !== true && "response" in res) {
    return res.response;
  }

  return new Response(null, { status: 204, headers: { "Content-Length": "0" } });
});

// this is the first thing that the client asks for in an upload
v2Router.post("/:name+/blobs/uploads/", async (req, env: Env) => {
  const { name } = req.params;
  const [uploadObject, err] = await wrap<UploadObject | RegistryError, Error>(env.REGISTRY_CLIENT.startUpload(name));
  if (err) {
    return new InternalError();
  }

  if ("response" in uploadObject) {
    return uploadObject.response;
  }

  const range = `${uploadObject.range.join("-")}`;
  // Return a res with a Location header indicating where to send the data to complete the upload
  return new Response(null, {
    status: 202,
    headers: {
      "Content-Length": "0",
      "Content-Range": range,
      "Range": range,
      "Location": uploadObject.location,
      "Docker-Upload-UUID": uploadObject.id,
      "OCI-Chunk-Min-Length": `${Math.max(MINIMUM_CHUNK, uploadObject.minimumBytesPerChunk ?? MINIMUM_CHUNK)}`,
      "OCI-Chunk-Max-Length": `${Math.min(
        MAXIMUM_CHUNK_UPLOAD_SIZE,
        uploadObject.maximumBytesPerChunk ?? MAXIMUM_CHUNK,
      )}`,
    },
  });
});

v2Router.post("/:name+/blobs/uploads/direct", async (req, env: Env) => {
  const { name } = req.params;
  let payload: { digest?: string; size?: number } = {};
  try {
    payload = await req.json();
  } catch (error) {
    console.warn("direct upload missing payload", errorString(error));
    return new Response(JSON.stringify({ message: "invalid payload" }), { status: 400, headers: jsonHeaders() });
  }

  if (!payload.digest) {
    return new Response(JSON.stringify({ message: "digest required" }), { status: 400, headers: jsonHeaders() });
  }

  const [directUpload, err] = await wrap<DirectUploadInfo | RegistryError, Error>(
    env.REGISTRY_CLIENT.startDirectUpload(name, { digest: payload.digest, size: payload.size }),
  );
  if (err) {
    return new InternalError();
  }

  if ("response" in directUpload) {
    return directUpload.response;
  }

  return new Response(
    JSON.stringify({
      upload_id: directUpload.upload.id,
      location: directUpload.upload.location,
      upload_url: directUpload.uploadUrl,
      expires_in: directUpload.expiresIn,
      headers: directUpload.headers ?? {},
      parts: directUpload.parts ?? [],
    }),
    { headers: jsonHeaders() },
  );
});

v2Router.get("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const [uploadObject, err] = await wrap<UploadObject | RegistryError, Error>(
    env.REGISTRY_CLIENT.getUpload(name, uuid),
  );
  if (err) {
    return new InternalError();
  }

  if ("response" in uploadObject) {
    return uploadObject.response;
  }

  return new Response(null, {
    status: 204,
    headers: {
      "Location": uploadObject.location,
      // Note that the HTTP Range header byte ranges are inclusive and that will be honored, even in non-standard use cases.
      "Range": `${uploadObject.range.join("-")}`,
      "Docker-Upload-UUID": uploadObject.id,
      "OCI-Chunk-Min-Length": `${Math.max(MINIMUM_CHUNK, uploadObject.minimumBytesPerChunk ?? MINIMUM_CHUNK)}`,
      "OCI-Chunk-Max-Length": `${Math.min(
        MAXIMUM_CHUNK_UPLOAD_SIZE,
        uploadObject.maximumBytesPerChunk ?? MAXIMUM_CHUNK,
      )}`,
    },
  });
});

v2Router.patch("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const contentRange = req.headers.get("Content-Range");
  const [start, end] = contentRange?.split("-") ?? [undefined, undefined];
  if (req.body == null) {
    return new Response(null, { status: 400 });
  }

  let contentLengthString = req.headers.get("Content-Length");
  let stream = req.body;
  if (!contentLengthString) {
    const blob = await req.blob();
    contentLengthString = `${blob.size}`;
    stream = blob.stream();
  }

  const url = new URL(req.url);
  const [res, err] = await wrap<UploadObject | RegistryError, Error>(
    env.REGISTRY_CLIENT.uploadChunk(
      name,
      uuid,
      url.pathname + "?" + url.searchParams.toString(),
      stream,
      +contentLengthString,
      end !== undefined && start !== undefined ? [+start, +end] : undefined,
    ),
  );
  if (err) {
    console.error("Uploading chunk:", errorString(err));
    return new InternalError();
  }

  if ("response" in res) {
    return res.response;
  }

  // Return a res indicating that the chunk was successfully uploaded
  return new Response(null, {
    status: 202,
    headers: {
      "Location": res.location,
      // Note that the HTTP Range header byte ranges are inclusive and that will be honored, even in non-standard use cases.
      "Range": `${res.range.join("-")}`,
      "Docker-Upload-UUID": res.id,
    },
  });
});

v2Router.put("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const { digest } = req.query;

  const url = new URL(req.url);
  const [res, err] = await wrap<FinishedUploadObject | RegistryError, Error>(
    env.REGISTRY_CLIENT.finishUpload(
      name,
      uuid,
      url.pathname + "?" + url.searchParams.toString(),
      digest! as string,
      req.body ?? undefined,
      +(req.headers.get("Content-Length") ?? "0"),
      req.headers,
    ),
  );

  if (err) {
    console.error("Error finishing upload:", errorString(err));
    return new InternalError();
  }

  if ("response" in res) {
    return res.response;
  }

  return new Response(null, {
    status: 201,
    headers: {
      "Content-Length": "0",
      "Docker-Content-Digest": res.digest,
      "Location": res.location,
    },
  });
});

v2Router.head("/:name+/blobs/:tag", async (req, env: Env) => {
  const { name, tag } = req.params;

  const res = await env.REGISTRY.head(`${name}/blobs/${tag}`);
  let layerExistsResponse: CheckLayerResponse | null = null;
  if (!res) {
    const registryList = registries(env);
    for (const registry of registryList) {
      const client = new RegistryHTTPClient(env, registry);
      const response = await client.layerExists(name, tag);
      if ("response" in response) {
        continue;
      }

      if (response.exists) {
        layerExistsResponse = response;
        break;
      }
    }

    if (layerExistsResponse === null || !layerExistsResponse.exists)
      return new Response(JSON.stringify(BlobUnknownError), { status: 404 });
  } else {
    if (res.checksums.sha256 === null) {
      throw new ServerError("invalid checksum from R2 backend");
    }

    layerExistsResponse = {
      digest: hexToDigest(res.checksums.sha256!),
      size: res.size,
      exists: true,
    };

    // if we're being asked by digest, keep it stable
    if (tag.startsWith("sha256:")) {
      layerExistsResponse.digest = tag;
    }

    // legacy compat: if the blob is a tiny reference stub, resolve size from the referenced object
    if (res.size <= 64) {
      const md = res.customMetadata ?? {};
      const ref = md["X-Serverless-Registry-Reference"] ?? md["x-serverless-registry-reference"];
      const uuidRe = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      let key: string | undefined;
      if (ref && uuidRe.test(ref.trim())) {
        key = ref.trim();
      } else {
        // fallback: some older stubs just store the uuid as the blob body
        const pointerObj = await env.REGISTRY.get(`${name}/blobs/${tag}`);
        if (pointerObj) {
          const buf = await pointerObj.arrayBuffer();
          const text = new TextDecoder().decode(buf).trim();
          if (uuidRe.test(text)) {
            key = text;
          }
        }
      }
      if (key) {
        const targetHead = await env.REGISTRY.head(key);
        if (targetHead) {
          layerExistsResponse.size = targetHead.size;
        }
      }
    }
  }

  return new Response(null, {
    headers: {
      "Content-Length": layerExistsResponse.size.toString(),
      "Docker-Content-Digest": layerExistsResponse.digest,
    },
  });
});

export type TagsList = {
  name: string;
  tags: string[];
};

v2Router.get("/:name+/tags/list", async (req, env: Env) => {
  const { name } = req.params;
  const { n: nStr = 50, last } = req.query;
  const n = +nStr;
  if (isNaN(n)) {
    throw new ServerError("invalid 'n' parameter", 400);
  }

  const tags = await env.REGISTRY.list({
    prefix: `${name}/manifests`,
    limit: n,
    startAfter: last ? `${name}/manifests/${last}` : undefined,
  });

  const keys = tags.objects.map((object) => object.key.split("/").pop()!);
  return new Response(
    JSON.stringify({
      name,
      tags: keys,
    }),
    {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Link": `${req.url}?n=${n}&last=${keys.length ? keys[keys.length - 1] : ""}; rel=next`,
      },
    },
  );
});

v2Router.delete("/:name+/blobs/:digest", async (req, env: Env) => {
  const { name, digest } = req.params;

  const res = await env.REGISTRY.head(`${name}/blobs/${digest}`);

  if (!res) {
    return new Response(JSON.stringify(BlobUnknownError), { status: 404 });
  }

  await env.REGISTRY.delete(`${name}/blobs/${digest}`);
  return new Response(null, {
    status: 202,
    headers: {
      "Content-Length": "None",
    },
  });
});

v2Router.post("/:name+/gc", async (req, env: Env) => {
  const { name } = req.params;
  const mode = req.query.mode ?? "unreferenced";
  if (mode !== "unreferenced" && mode !== "untagged") {
    throw new ServerError("Mode must be either 'unreferenced' or 'untagged'", 400);
  }

  const dryRun = req.query.dry_run === "true";
  let excludedReferences: string[] | undefined;
  if (dryRun) {
    let payload: unknown;
    try {
      payload = await req.json();
    } catch {
      throw new ServerError("Dry-run garbage collection requires a JSON body", 400);
    }
    const references = (payload as { references?: unknown })?.references;
    if (
      !Array.isArray(references) ||
      references.length > 100 ||
      !references.every((reference) => typeof reference === "string" && TAG_REFERENCE_PATTERN.test(reference))
    ) {
      throw new ServerError("Dry-run garbage collection references are invalid", 400);
    }
    excludedReferences = [...new Set(references)];
  }

  const result = await env.REGISTRY_CLIENT.garbageCollection(name, mode, {
    dryRun,
    excludedReferences,
  });
  return new Response(JSON.stringify(result), { headers: jsonHeaders() });
});

export default v2Router;
