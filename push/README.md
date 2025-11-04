## push chunked images to serverless-registry

this tool pushes docker/oci images to the serverless-registry using chunked uploads. it respects the registry’s
advertised `oci-chunk-max-length` and sends each layer in sequential chunks over a keep-alive connection.

## requirements

- bun
- the image exported via `docker save` as `output.tar` in this directory, or an already-extracted image tree in
  `.output-image/` containing `index.json` and `blobs/`

## quick start

```bash
bun install

# option a: start from a docker save tarball in this folder
docker save <image> --output output.tar

# option b: use a pre-extracted tree at .output-image/
#   ensure .output-image/index.json exists and blobs are under .output-image/blobs/sha256/

# run the pusher
USERNAME_REGISTRY=<username> REGISTRY_JWT_TOKEN=<token> bun run index.ts <registry-host>/<repo>:<tag>
```

## environment variables

- `USERNAME_REGISTRY` (required): basic auth username
- `REGISTRY_JWT_TOKEN` (required): basic auth password/token
- `SKIP_AUTH` (optional): set to any value to bypass the username/password check (useful for local testing)
- `INSECURE_HTTP_PUSH` (optional): set to `true` to use http instead of https (for local registries)
- `MAX_PARALLEL_LAYERS` (optional): number of layers to upload concurrently (default 6)
- `MAX_RETRIES` (optional): per-layer retry count on transient failures (default 8)

## how it works

- reads `output.tar` and extracts to `.output-image/`, or uses an existing `.output-image/`
- parses `.output-image/index.json` and resolves the manifest and layer digests
- uploads layers in parallel using a per-layer loop that:
  - creates an upload with `POST /v2/<repo>/blobs/uploads/`
  - streams fixed-size chunks with `PATCH` using `content-range: <start>-<end>` and an explicit `content-length`
  - completes the layer with a final `PUT` on `.../uploads/<uuid>?digest=<sha256:...>`
- after all layers and config are present, uploads the manifest with `PUT /v2/<repo>/manifests/<tag>`

## performance notes

- keep-alive is enabled for all requests to avoid connection setup between chunks
- the pusher respects `oci-chunk-max-length` advertised by the registry, you can’t force larger chunks than the server
  allows
- can tweak `MAX_PARALLEL_LAYERS` depending on uplink throughput

## pushing locally

set `INSECURE_HTTP_PUSH=true` to use http:

```bash
INSECURE_HTTP_PUSH=true USERNAME_REGISTRY=<username> REGISTRY_JWT_TOKEN=<token> \
  bun run index.ts localhost:8787/my/repo:latest
```
