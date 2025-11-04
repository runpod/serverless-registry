import { $, file } from "bun";
import { extract } from "tar";

import { mkdir } from "node:fs/promises";
import http from "node:http";
import https from "node:https";
import path from "path";
import plimit from "p-limit";
import fetchNode from "node-fetch";
import { ReadableLimiter } from "./limiter";

// push client for the cloudflare-backed oci registry
// - reads an extracted docker save (index.json + blobs)
// - uploads layers in parallel; each layer is sent in chunks using PATCH with content-range
// - uses keep-alive agents to avoid reconnect overhead between chunks

type IndexJSONFile = {
  manifests: {
    digest: string;
  }[];
};

type DockerSaveConfigManifest = {
  config: {
    digest: string;
    size: number;
  };
  layers: {
    digest: string;
  }[];
};

const username = process.env["USERNAME_REGISTRY"];
const password = process.env["REGISTRY_JWT_TOKEN"];
const tarFile = "output.tar";
const imagePath = ".output-image";
const MAX_PARALLEL_LAYERS = +(process.env["MAX_PARALLEL_LAYERS"] ?? 6);
const pool = plimit(isNaN(MAX_PARALLEL_LAYERS) ? 6 : MAX_PARALLEL_LAYERS);
const proto = process.env["INSECURE_HTTP_PUSH"] === "true" ? "http" : "https";

function log(...args: any[]) {
  const ts = new Date().toISOString();
  console.log(ts, "-", ...args);
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

function formatMBps(bytes: number, ms: number): string {
  if (ms <= 0) return "inf";
  const mb = bytes / (1024 * 1024);
  return `${(mb / (ms / 1000)).toFixed(2)} MB/s`;
}

if (!username || !password) {
  console.error("Username or password not defined, push won't be able to authenticate with registry");
  if (!process.env["SKIP_AUTH"]) {
    process.exit(1);
  }
}

const image = process.argv[2];
if (image === undefined) {
  console.error("Usage: bun run index.ts <image>");
  process.exit(1);
}

if (await file(tarFile).exists()) {
  await mkdir(imagePath, { recursive: true });

  await extract({
    file: tarFile,
    cwd: imagePath,
  });
} else {
  const idxExists = await file(path.join(imagePath, "index.json")).exists();
  if (!idxExists) {
    console.error(`Neither ${tarFile} found nor ${imagePath}/index.json present. Aborting.`);
    process.exit(1);
  }
}

const indexJSONFile = (await Bun.file(path.join(imagePath, "index.json")).json()) as IndexJSONFile;
const manifestFile = indexJSONFile.manifests[0].digest.replace("sha256:", "");

const manifestBlobFile = file(`${imagePath}/blobs/sha256/${manifestFile}`);
await Bun.write(`${imagePath}/manifest.json`, manifestBlobFile);

let manifestAny = (await Bun.file(path.join(imagePath, "manifest.json")).json()) as any;
// handle manifest lists by resolving to a single-platform manifest
if (manifestAny && Array.isArray(manifestAny.manifests)) {
  // pick first entry; optionally prefer linux/amd64 if present
  const preferOs = (process.env["PUSH_OS"] ?? "linux").toString();
  const preferArch = (process.env["PUSH_ARCH"] ?? "amd64").toString();
  const preferred = manifestAny.manifests.find(
    (m: any) => m.platform?.os === preferOs && m.platform?.architecture === preferArch,
  );
  const candidates: any[] = preferred
    ? [preferred, ...manifestAny.manifests.filter((m: any) => m !== preferred)]
    : [...manifestAny.manifests];
  let picked: any | undefined;
  let pickedPath: string | undefined;
  for (const cand of candidates) {
    if (!cand?.digest) continue;
    const d = (cand.digest as string).replace("sha256:", "");
    const p = path.join(imagePath, "blobs/sha256/", d);
    if (await file(p).exists()) {
      picked = cand;
      pickedPath = p;
      break;
    }
  }
  if (!picked || !pickedPath) {
    const available = (manifestAny.manifests || []).map((m: any) => m?.digest).filter(Boolean);
    throw new Error(
      `manifest list entries not found under blobs. available digests: ${available.join(
        ", ",
      )}. re-export single-arch (docker pull --platform=${preferOs}/${preferArch} <image>; docker save <image> --output output.tar)) or ensure the selected platform blobs are present.`,
    );
  }
  manifestAny = await Bun.file(pickedPath).json();
  // write out the resolved manifest for visibility/debugging
  await Bun.write(path.join(imagePath, "manifest.json"), JSON.stringify(manifestAny));
  log("resolved manifest list entry", { os: picked.platform?.os, arch: picked.platform?.architecture });
}

const manifest = manifestAny as DockerSaveConfigManifest;
if (!manifest || !Array.isArray((manifest as any).layers)) {
  throw new Error("manifest does not contain layers; ensure image was saved correctly or select a platform");
}

await mkdir(imagePath, { recursive: true });
const tasks = [] as Promise<string>[];

// iterate through every layer, read it and compress to a file
for (const layer of manifest.layers) {
  tasks.push(
    pool(async () => {
      return layer.digest.replace("sha256:", "");
    }),
  );
}

const config = manifest.config;
const configDigest = config.digest.replace("sha256:", "");

const compressedDigests = await Promise.all(tasks);

// precompute layer sizes
const plannedLayers = compressedDigests.map((d) => ({
  digest: d,
  size: file(`${imagePath}/blobs/sha256/${d}`).size,
}));

const plannedConfigSize = file(`${imagePath}/blobs/sha256/${configDigest}`).size;
const plannedTotalBytes = plannedLayers.reduce((acc, l) => acc + l.size, 0) + plannedConfigSize;

log("upload plan", {
  layers: plannedLayers.length + 1,
  totalBytes: formatBytes(plannedTotalBytes),
  avgLayerSizeMB: (plannedLayers.reduce((a, b) => a + b.size, 0) / plannedLayers.length / (1024 * 1024)).toFixed(2),
  parallel: MAX_PARALLEL_LAYERS,
});

// parse target: accept full URL (http/https) or host/path:tag
const parsedTargetUrl = /^(https?:)\/\//i.test(image) ? new URL(image) : new URL(`${proto}://${image}`);
const scheme = parsedTargetUrl.protocol.replace(":", "");
if (scheme === "http") {
  console.error("!! Using plain HTTP !!");
}

const pushTasks = [];
const overallStart = Date.now();
const imageHost = parsedTargetUrl.host;
const imageRepositoryPathParts = parsedTargetUrl.pathname.split(":");
const imageRepositoryPath = imageRepositoryPathParts.slice(0, imageRepositoryPathParts.length - 1).join(":");
const tag =
  imageRepositoryPathParts.length > 1 ? imageRepositoryPathParts[imageRepositoryPathParts.length - 1] : "latest";

const cred = `Basic ${btoa(`${username}:${password}`)}`;

// log basic configuration before we begin
const DISABLE_KEEPALIVE = (process.env["DISABLE_KEEPALIVE"] ?? "false").toString() === "true";
const agent = DISABLE_KEEPALIVE
  ? undefined
  : scheme === "http"
  ? new http.Agent({ keepAlive: true, keepAliveMsecs: 30000, maxSockets: 64, maxFreeSockets: 32 })
  : new https.Agent({ keepAlive: true, keepAliveMsecs: 30000, maxSockets: 64, maxFreeSockets: 32 });

log("starting push", { imageHost, imageRepositoryPath, tag, MAX_PARALLEL_LAYERS, proto: scheme, DISABLE_KEEPALIVE });

// push layer to registry
async function pushLayer(layerDigest: string, readableStream: ReadableStream, totalLayerSize: number) {
  const headers = new Headers({
    authorization: cred,
  });
  if (!agent) headers.set("connection", "close");
  const layerStart = Date.now();

  const layerExistsURL = `${scheme}://${imageHost}/v2${imageRepositoryPath}/blobs/${layerDigest}`;
  const layerExistsResponse = await fetchNode(layerExistsURL, {
    headers,
    method: "HEAD",
    // @ts-ignore node-fetch agent
    agent,
  });

  if (!layerExistsResponse.ok && layerExistsResponse.status !== 404) {
    throw new Error(`${layerExistsURL} responded ${layerExistsResponse.status}: ${await layerExistsResponse.text()}`);
  }

  if (layerExistsResponse.ok) {
    console.log(`${layerDigest} already exists...`);
    return;
  }

  const createUploadURL = `${scheme}://${imageHost}/v2${imageRepositoryPath}/blobs/uploads/`;
  const tCreateStart = Date.now();
  const createUploadResponse = await fetchNode(createUploadURL, {
    headers,
    method: "POST",
    // @ts-ignore node-fetch agent
    agent,
  });
  if (!createUploadResponse.ok) {
    throw new Error(
      `${createUploadURL} responded ${createUploadResponse.status}: ${await createUploadResponse.text()}`,
    );
  }

  const maxChunkLength = +(
    createUploadResponse.headers.get("oci-chunk-max-length") ??
    createUploadResponse.headers.get("OCI-Chunk-Max-Length") ??
    100 * 1024 * 1024
  );

  if (isNaN(maxChunkLength)) {
    throw new Error(`oci-chunk-max-length header is malformed (not a number)`);
  }

  log("created upload", {
    uploadId: createUploadResponse.headers.get("docker-upload-uuid"),
    maxChunkLength,
    tookMs: Date.now() - tCreateStart,
  });

  const reader = readableStream.getReader();
  const uploadId = createUploadResponse.headers.get("docker-upload-uuid");
  if (uploadId === null) {
    throw new Error("Docker-Upload-UUID not defined in headers");
  }

  function parseLocation(location: string) {
    if (location.startsWith("/")) {
      return `${scheme}://${imageHost}${location}`;
    }

    return location;
  }

  let location = createUploadResponse.headers.get("location") ?? `/v2${imageRepositoryPath}/blobs/uploads/${uploadId}`;
  const maxToWrite = Math.min(maxChunkLength, totalLayerSize);

  let written = 0;
  let previousReadable: ReadableLimiter | undefined;
  let totalLayerSizeLeft = totalLayerSize;
  let chunkIndex = 0;
  let lastPatchEnd = Date.now();

  while (totalLayerSizeLeft > 0) {
    const toWriteNow = Math.min(maxToWrite, totalLayerSizeLeft);
    const startOffset = written;
    const endOffsetInclusive = startOffset + toWriteNow - 1;
    const contentRange = `${startOffset}-${endOffsetInclusive}`;
    const expectedResponseRange = `0-${endOffsetInclusive}`;
    const current = new ReadableLimiter(reader as ReadableStreamDefaultReader, toWriteNow, previousReadable);
    const patchChunkUploadURL = parseLocation(location);

    // we have to do fetchNode because Bun doesn't allow setting custom Content-Length.
    // https://github.com/oven-sh/bun/issues/10507
    const tPatchStart = Date.now();
    const idleMs = tPatchStart - lastPatchEnd;

    log("patch start", {
      layer: layerDigest,
      chunkIndex,
      contentRange,
      toWriteNow,
      location: patchChunkUploadURL,
      idleMsBeforePatch: idleMs,
    });

    const patchChunkResult = await fetchNode(patchChunkUploadURL, {
      method: "PATCH",
      body: current,
      headers: new Headers({
        "content-range": contentRange,
        // keep sending Range for compatibility with some proxies; server reads Content-Range
        "range": expectedResponseRange,
        "authorization": cred,
        "content-length": `${toWriteNow}`,
      }),
      // @ts-ignore node-fetch agent
      agent,
    });
    if (!agent) patchChunkResult as any; // keep tree-shaking from removing usage

    if (!patchChunkResult.ok) {
      const body = await patchChunkResult.text();
      throw new Error(
        `patch failed layer=${layerDigest} chunkIndex=${chunkIndex} contentRange=${contentRange} url=${patchChunkUploadURL} status=${patchChunkResult.status} body=${body}`,
      );
    }

    const rangeResponse = patchChunkResult.headers.get("range");
    if (rangeResponse !== expectedResponseRange) {
      throw new Error(
        `unexpected range header: got=${rangeResponse} expected=${expectedResponseRange} layer=${layerDigest} chunkIndex=${chunkIndex}`,
      );
    }

    previousReadable = current;
    totalLayerSizeLeft -= previousReadable.written;
    written += previousReadable.written;
    location = patchChunkResult.headers.get("location") ?? location;

    const dt = Date.now() - tPatchStart;
    const mb = previousReadable.written / (1024 * 1024);
    const mbps = dt > 0 ? (mb / (dt / 1000)).toFixed(2) : "inf";

    log("patch done", {
      layer: layerDigest,
      chunkIndex,
      wroteBytes: previousReadable.written,
      tookMs: dt,
      speedMBps: mbps,
      nextLocation: location,
      remainingBytes: totalLayerSizeLeft,
    });

    chunkIndex++;
    lastPatchEnd = Date.now();
    if (totalLayerSizeLeft != 0) log(layerDigest + ":", totalLayerSizeLeft, "upload bytes left.");
  }

  const uploadURL = new URL(parseLocation(location));
  uploadURL.searchParams.append("digest", layerDigest);

  const tPutStart = Date.now();
  const response = await fetchNode(uploadURL.toString(), {
    method: "PUT",
    headers: new Headers({
      Authorization: cred,
      ...(agent ? {} : { connection: "close" }),
    }),
    // @ts-ignore node-fetch agent
    agent,
  });

  if (!response.ok) {
    throw new Error(`${uploadURL.toString()} failed with ${response.status}: ${await response.text()}`);
  }

  const layerMs = Date.now() - layerStart;
  log("layer complete", {
    layer: layerDigest,
    chunks: chunkIndex,
    totalBytes: written,
    tookMs: Date.now() - tPutStart,
    layerMs,
    avgSpeed: formatMBps(written, layerMs),
  });
}

const layersManifest = [] as {
  readonly mediaType: "application/vnd.oci.image.layer.v1.tar+gzip";
  readonly size: number;
  readonly digest: `sha256:${string}`;
}[];

for (const compressedDigest of compressedDigests) {
  let layer = file(`${imagePath}/blobs/sha256/${compressedDigest}`);

  layersManifest.push({
    mediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
    size: layer.size,
    digest: `sha256:${compressedDigest}`,
  } as const);

  pushTasks.push(
    pool(async () => {
      const maxRetries = +(process.env["MAX_RETRIES"] ?? 8);
      if (isNaN(maxRetries)) throw new Error("MAX_RETRIES is not a number");

      for (let i = 0; i < maxRetries; i++) {
        const digest = `sha256:${compressedDigest}`;
        const stream = layer.stream();
        try {
          log("Upload layer start", { layer: digest, size: layer.size });
          await pushLayer(digest, stream, layer.size);
          return;
        } catch (err) {
          console.error(digest, "failed to upload", maxRetries - i - 1, "left...", err);
          layer = file(`${imagePath}/blobs/sha256/${compressedDigest}`);
        }
      }
    }),
  );
}

pushTasks.push(
  pool(async () => {
    const maxRetries = +(process.env["MAX_RETRIES"] ?? 8);
    if (isNaN(maxRetries)) throw new Error("MAX_RETRIES is not a number");

    for (let i = 0; i < maxRetries; i++) {
      let configLayer = file(`${imagePath}/blobs/sha256/${configDigest}`);
      const stream = configLayer.stream();
      const digest = `sha256:${configDigest}`;
      try {
        await pushLayer(digest, stream, configLayer.size);
        return;
      } catch (err) {
        console.error(digest, "failed to upload", maxRetries - i - 1, "left...", err);
        configLayer = file(`${imagePath}/blobs/sha256/${configDigest}`);
      }
    }
  }),
);

const promises = await Promise.allSettled(pushTasks);
for (const promise of promises) {
  if (promise.status === "rejected") {
    throw promise.reason;
  }
}

const manifestObject = {
  schemaVersion: 2,
  mediaType: "application/vnd.oci.image.manifest.v1+json",
  config: {
    mediaType: "application/vnd.oci.image.config.v1+json",
    size: config.size,
    digest: `sha256:${configDigest}`,
  },
  layers: layersManifest,
} as const;

const manifestUploadURL = `${scheme}://${imageHost}/v2${imageRepositoryPath}/manifests/${tag}`;
const responseManifestUpload = await fetchNode(manifestUploadURL, {
  headers: {
    "authorization": cred,
    "content-type": manifestObject.mediaType,
    ...(agent ? ({} as any) : ({ connection: "close" } as any)),
  },
  body: JSON.stringify(manifestObject),
  method: "PUT",
  // @ts-ignore node-fetch agent
  agent,
});

if (!responseManifestUpload.ok) {
  throw new Error(
    `manifest upload ${manifestUploadURL} returned ${
      responseManifestUpload.status
    }: ${await responseManifestUpload.text()}`,
  );
}

const overallMs = Date.now() - overallStart;

log("push complete", {
  tookMs: overallMs,
  totalBytes: formatBytes(plannedTotalBytes),
  avgSpeed: formatMBps(plannedTotalBytes, overallMs),
});
