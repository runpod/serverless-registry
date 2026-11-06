// Validation of the token realm advertised by an upstream registry in its
// WWW-Authenticate challenge. The realm is where the configured credential is
// sent, so it must never be taken from the challenge without checking it
// against what the operator configured.

// Registries whose token service lives on a different host than the registry.
// Everything else is expected to serve tokens from the registry host itself
// (ghcr.io, gcr.io, *.pkg.dev, quay.io, ECR, ACR, Harbor) and does not need an entry.
const KNOWN_REALM_HOSTS: Readonly<Record<string, readonly string[]>> = {
  "registry-1.docker.io": ["auth.docker.io"],
  "index.docker.io": ["auth.docker.io"],
  "docker.io": ["auth.docker.io"],
};

const HTTPS_PROTOCOL = "https:";

export class RealmNotAllowedError extends Error {
  constructor(registry: URL, realm: string, reason: string) {
    super(
      `refusing to send credentials to token realm ${JSON.stringify(realm)} advertised by registry ${registry.host}: ${reason}. ` +
        `Add the realm host to auth_realm_hosts in REGISTRIES_JSON if it is trusted`,
    );
    this.name = "RealmNotAllowedError";
  }
}

function isIpLiteral(hostname: string): boolean {
  return hostname.startsWith("[") || /^[0-9.]+$/.test(hostname);
}

// Returns the parsed realm URL when credentials may be sent to it, throws RealmNotAllowedError otherwise.
export function assertRealmAllowed(realm: string, registry: URL, configuredHosts: readonly string[] = []): URL {
  let url: URL;
  try {
    url = new URL(realm);
  } catch {
    throw new RealmNotAllowedError(registry, realm, "realm is not a valid URL");
  }

  if (url.username !== "" || url.password !== "") {
    throw new RealmNotAllowedError(registry, realm, "realm must not contain userinfo");
  }

  const isRegistryOrigin = url.host === registry.host && url.protocol === registry.protocol;
  if (isRegistryOrigin) return url;

  if (url.protocol !== HTTPS_PROTOCOL) {
    throw new RealmNotAllowedError(registry, realm, "realm must use https");
  }
  if (url.port !== "") {
    throw new RealmNotAllowedError(registry, realm, "realm must use the default https port");
  }
  if (isIpLiteral(url.hostname)) {
    throw new RealmNotAllowedError(registry, realm, "realm must not be an IP address");
  }

  const allowedHosts = [registry.hostname, ...(KNOWN_REALM_HOSTS[registry.hostname] ?? []), ...configuredHosts];
  if (!allowedHosts.includes(url.hostname)) {
    throw new RealmNotAllowedError(
      registry,
      realm,
      `host ${url.hostname} is not the registry host or an allowed realm host`,
    );
  }
  return url;
}
