const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export const SMALL_BLOB_POINTER_MAX_BYTES = 64;

export function parseRegistryReference(value?: string): string | undefined {
  const reference = value?.trim();
  return reference && UUID_PATTERN.test(reference) ? reference : undefined;
}

export function getRegistryReferenceFromMetadata(object: {
  customMetadata?: Record<string, string>;
}): string | undefined {
  const metadata = object.customMetadata;
  if (!metadata) return undefined;

  for (const [key, value] of Object.entries(metadata)) {
    if (key.toLowerCase() !== "x-serverless-registry-reference") continue;
    return parseRegistryReference(value);
  }

  return undefined;
}

export async function getRegistryReference(
  registry: R2Bucket,
  object: R2Object,
): Promise<string | undefined> {
  const metadataReference = getRegistryReferenceFromMetadata(object);
  if (metadataReference) return metadataReference;
  if (object.size > SMALL_BLOB_POINTER_MAX_BYTES) return undefined;

  const body = await registry.get(object.key);
  if (!body) return undefined;
  return parseRegistryReference(new TextDecoder().decode(await body.arrayBuffer()));
}
