import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { env } from "cloudflare:test";
import { Env } from "..";
import { registries } from "../src/registry/registry";
import { RegistryHTTPClient } from "../src/registry/http";

type Recorded = { url: string; method: string; headers: Headers; body: string; redirect?: RequestInit["redirect"] };
type Handler = (call: Recorded) => Response;

const PASSWORD = "s3cret-registry-password";
const BASIC_CREDENTIALS = btoa(`hello:${PASSWORD}`);
const EVIL_HOST = "evil.example";

const prevFetch = global.fetch;
let calls: Recorded[] = [];

function installFetch(handler: Handler) {
  calls = [];
  global.fetch = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const req = new Request(input, init);
    const call: Recorded = {
      url: req.url,
      method: req.method,
      headers: req.headers,
      body: await req.text(),
      redirect: init?.redirect,
    };
    calls.push(call);
    return handler(call);
  }) as typeof fetch;
}

function challenge(header: string): Response {
  return new Response("{}", { status: 401, headers: { "WWW-Authenticate": header } });
}

function tokenResponse(token = "tok"): Response {
  return new Response(JSON.stringify({ token, expires_in: 60 }), { status: 200 });
}

function registryHandler(wwwAuthenticate: string, onToken: Handler = () => tokenResponse()): Handler {
  return (call) => (new URL(call.url).pathname === "/v2/" ? challenge(wwwAuthenticate) : onToken(call));
}

function clientFor(registry: string, extra: { auth_realm_hosts?: string[] } = {}) {
  const bindings = { ...(env as Env), PASSWORD } as Env;
  return new RegistryHTTPClient(bindings, { registry, password_env: "PASSWORD", username: "hello", ...extra });
}

function callsTo(host: string): Recorded[] {
  return calls.filter((c) => new URL(c.url).hostname === host);
}

function expectNoCredentialLeak() {
  for (const call of calls) {
    expect(call.body).not.toContain(PASSWORD);
    expect(call.url).not.toContain(PASSWORD);
    expect(call.headers.get("Authorization") ?? "").not.toContain(BASIC_CREDENTIALS);
  }
}

beforeEach(() => {
  calls = [];
});

afterEach(() => {
  global.fetch = prevFetch;
});

describe("realm allowlist: legitimate upstreams keep authenticating", () => {
  test("realm on the configured registry host", async () => {
    installFetch(registryHandler('Bearer realm="https://ghcr.io/token",service="ghcr.io"'));
    const ctx = await clientFor("https://ghcr.io/runpod").authenticate();
    expect(ctx.accessToken).toBe("tok");
    const tokenCall = calls.find((c) => c.url.startsWith("https://ghcr.io/token"))!;
    expect(tokenCall.body).toContain(PASSWORD);
  });

  test.each(["registry-1.docker.io", "index.docker.io", "docker.io"])(
    "Docker Hub %s uses auth.docker.io",
    async (host) => {
      installFetch(registryHandler('Bearer realm="https://auth.docker.io/token",service="registry.docker.io"'));
      const ctx = await clientFor(`https://${host}/library`).authenticate();
      expect(ctx.accessToken).toBe("tok");
      expect(callsTo("auth.docker.io")).toHaveLength(1);
    },
  );

  test("realm host listed in auth_realm_hosts", async () => {
    installFetch(registryHandler('Bearer realm="https://gitlab.com/jwt/auth",service="container_registry"'));
    const ctx = await clientFor("https://registry.gitlab.com/group", {
      auth_realm_hosts: ["gitlab.com"],
    }).authenticate();
    expect(ctx.accessToken).toBe("tok");
  });

  test("http realm is allowed only on a same-origin http registry", async () => {
    installFetch(registryHandler('Bearer realm="http://localhost:5000/token",service="localhost"'));
    const ctx = await clientFor("http://localhost:5000").authenticate();
    expect(ctx.accessToken).toBe("tok");
  });
});

describe("realm allowlist: untrusted realms receive no credentials", () => {
  const cases: [string, string, string][] = [
    ["foreign host", "https://registry.example", `https://${EVIL_HOST}/token`],
    ["suffix lookalike", "https://registry.example", "https://registry.example.evil.example/token"],
    ["prefix lookalike", "https://registry.example", "https://evilregistry.example/token"],
    ["userinfo trick", "https://registry.example", `https://registry.example@${EVIL_HOST}/token`],
    ["userinfo on the allowed host", "https://registry.example", "https://user:pw@registry.example/token"],
    ["non-default port on the allowed host", "https://registry.example", "https://registry.example:8443/token"],
    ["plain http for an https registry", "https://registry.example", "http://registry.example/token"],
    ["IPv4 literal", "https://registry.example", "https://169.254.169.254/token"],
    ["IPv6 literal", "https://registry.example", "https://[::1]/token"],
    ["trailing dot host", "https://registry.example", "https://registry.example./token"],
    ["Docker Hub registry with foreign realm", "https://registry-1.docker.io", `https://${EVIL_HOST}/token`],
    ["auth.docker.io for a non Docker Hub registry", "https://registry.example", "https://auth.docker.io/token"],
  ];

  test.each(cases)("bearer: %s", async (_name, registry, realm) => {
    installFetch(registryHandler(`Bearer realm="${realm}",service="svc"`));
    await expect(clientFor(registry).authenticate()).rejects.toThrow(/realm/i);
    expect(calls.map((c) => new URL(c.url).pathname)).toEqual(["/v2/"]);
    expectNoCredentialLeak();
  });

  test.each(cases)("basic: %s", async (_name, registry, realm) => {
    installFetch(registryHandler(`Basic realm="${realm}"`));
    await expect(clientFor(registry).authenticate()).rejects.toThrow(/realm/i);
    expect(calls.map((c) => new URL(c.url).pathname)).toEqual(["/v2/"]);
    expectNoCredentialLeak();
  });

  test("basic realm on the configured host still authenticates", async () => {
    installFetch(registryHandler('Basic realm="https://registry.example/auth"', () => new Response("{}")));
    const ctx = await clientFor("https://registry.example").authenticate();
    expect(ctx.accessToken).toBe(BASIC_CREDENTIALS);
  });

  test("an allowlisted realm host does not extend to other registries", async () => {
    installFetch(registryHandler('Bearer realm="https://gitlab.com/jwt/auth",service="svc"'));
    await expect(clientFor("https://registry.example").authenticate()).rejects.toThrow(/realm/i);
    expectNoCredentialLeak();
  });
});

describe("token endpoint redirects", () => {
  test("a redirect from the token endpoint is not followed and carries no credentials", async () => {
    installFetch(
      registryHandler(
        'Bearer realm="https://registry.example/token",service="svc"',
        () => new Response(null, { status: 307, headers: { Location: `https://${EVIL_HOST}/steal` } }),
      ),
    );
    await expect(clientFor("https://registry.example").authenticate()).rejects.toThrow();
    const tokenCalls = calls.filter((c) => new URL(c.url).pathname === "/token");
    expect(tokenCalls.length).toBeGreaterThan(0);
    for (const call of tokenCalls) expect(call.redirect).toBe("manual");
    expect(callsTo(EVIL_HOST)).toHaveLength(0);
  });

  test("a redirect from the basic realm is not followed", async () => {
    installFetch(
      registryHandler(
        'Basic realm="https://registry.example/auth"',
        () => new Response(null, { status: 302, headers: { Location: `https://${EVIL_HOST}/steal` } }),
      ),
    );
    await expect(clientFor("https://registry.example").authenticate()).rejects.toThrow();
    expect(callsTo(EVIL_HOST)).toHaveLength(0);
    for (const call of calls.filter((c) => new URL(c.url).pathname === "/auth")) expect(call.redirect).toBe("manual");
  });
});

describe("service attribute does not choose the registry host", () => {
  test("authenticated requests go to the configured host", async () => {
    installFetch((call) => {
      const url = new URL(call.url);
      if (url.pathname === "/v2/")
        return challenge(`Bearer realm="https://registry.example/token",service="${EVIL_HOST}"`);
      if (url.pathname === "/token") return tokenResponse("bearer-token");
      return new Response(null, { status: 404 });
    });
    await clientFor("https://registry.example/team").manifestExists("app", "latest");
    expect(callsTo(EVIL_HOST)).toHaveLength(0);
    const manifestCall = calls.find((c) => c.url.includes("/manifests/"))!;
    expect(new URL(manifestCall.url).hostname).toBe("registry.example");
    expect(manifestCall.headers.get("Authorization")).toBe("Bearer bearer-token");
  });
});

describe("auth_realm_hosts configuration", () => {
  const entry = (hosts: unknown) =>
    JSON.stringify([
      { registry: "https://registry.example", username: "u", password_env: "P", auth_realm_hosts: hosts },
    ]);
  const parse = (hosts: unknown) => registries({ ...(env as Env), REGISTRIES_JSON: entry(hosts) });

  test("accepts bare hostnames", () => {
    expect(parse(["gitlab.com", "Auth.Example.COM"])[0].auth_realm_hosts).toEqual(["gitlab.com", "auth.example.com"]);
  });

  test.each([
    ["scheme", ["https://gitlab.com"]],
    ["path", ["gitlab.com/jwt"]],
    ["port", ["gitlab.com:443"]],
    ["wildcard", ["*.gitlab.com"]],
    ["userinfo", ["a@gitlab.com"]],
    ["empty", [""]],
  ])("rejects %s", (_name, hosts) => {
    const prevError = console.error;
    console.error = () => {};
    try {
      expect(parse(hosts)).toEqual([]);
    } finally {
      console.error = prevError;
    }
  });

  test("is optional", () => {
    const parsed = registries({
      ...(env as Env),
      REGISTRIES_JSON: JSON.stringify([{ registry: "https://registry.example", username: "u", password_env: "P" }]),
    });
    expect(parsed).toHaveLength(1);
    expect(parsed[0].auth_realm_hosts).toBeUndefined();
  });
});
