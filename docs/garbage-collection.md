# Removing manifests and garbage collection

Garbage collection is useful due to how [OCI](https://github.com/opencontainers/image-spec/blob/main/manifest.md) container images get shipped to registries.

```json
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {
    "mediaType": "application/vnd.oci.image.config.v1+json",
    "digest": "sha256:b5b2b2c507a0944348e0303114d8d93aaaa081732b86451d9bce1f432a537bc7",
    "size": 7023
  },
  "layers": [
    {
      "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
      "digest": "sha256:9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0",
      "size": 32654
    },
    {
      "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
      "digest": "sha256:3c3a4604a545cdc127456d94e421cd355bca5b528f4a9c1905b15da2eb4a4c6b",
      "size": 16724
    },
    {
      "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
      "digest": "sha256:ec4b8955958665577945c89419d1af06b5f7636b4ac3da7f12184802ad867736",
      "size": 73109
    }
  ]
}
```

This is how a container image manifest looks, it's a "tree-like" structure where the manifest references
layers. If you remove an image from a registry, you're probably just removing its manifest. However, layers
will still be around taking space.

## Removing an image and triggering the garbage collection

To delete an image tag, use `skopeo delete` or an API call. Maintenance clients can include the inventory digest so deletion fails if the tag points to different content.

```
# If you pushed to serverless.workers.dev/my-image:latest
curl -X DELETE \
  -H "Authorization: $CREDENTIAL" \
  -H "X-Runpod-Expected-Digest: sha256:..." \
  https://serverless.workers.dev/my-image/manifests/latest
```

Maintenance cleanup first claims a tag with its inventory digest. Claimed tags reject reads and writes while the caller revalidates external references, and claims expire after 15 minutes if a caller stops before releasing or deleting them. Final deletion requires both the claim token and the same digest.

The digest manifest and unreferenced layers can then be reclaimed with untagged garbage collection.

```
curl -X POST -H "Authorization: $CREDENTIAL" "https://serverless.workers.dev/my-image/gc?mode=untagged"
{"success":true,"objectCount":4,"bytes":128000}
```

A dry run calculates the objects and bytes that would be reclaimed after removing a bounded set of tags. It does not mutate registry data.

```
curl -X POST \
  -H "Authorization: $CREDENTIAL" \
  -H "Content-Type: application/json" \
  -d '{"references":["latest","previous"]}' \
  "https://serverless.workers.dev/my-image/gc?mode=untagged&dry_run=true"
{"success":true,"objectCount":8,"bytes":256000}
```

Objects uploaded within the last hour and blobs attached to active direct uploads remain available for in-progress pushes.

## How does it work

Reachability is evaluated in bounded memory with conservative Bloom filters. Tagged and recent manifests seed the live set, and bounded passes propagate reachability through OCI indexes, subjects, and referrers. Bloom-filter false positives retain extra objects, so they cannot cause live data to be reclaimed. Manifest bodies are processed one at a time.

Live manifests seed another bounded filter for configs and layers. Active uploads and legacy pointer targets are added before unreferenced objects are processed in bounded deletion batches.

Some registries take a lock stop the world approach, however serverless-registry can't really do that due
to its objective of only using R2. However, we need to fail whenever a race condition happens, a data
race that causes data-loss would be completely unacceptable.

That's when we introduce a simple system where instead of taking a lock, we mark in R2
that we are about to create a manifest and that we are inserting data.
If the garbage collector starts and sees that key, it will fail. At the end of the insertion, the insertion mark
gets updated.

The garbage collector acquires a 15-minute lease and renews it before each deletion batch. Lease updates are conditional on the current object ETag, so a stale process cannot renew or release a newer collector's lease. Expired leases can be claimed by a later run.

Let's state some scenarios:

```
PutManifest                       GC
1. markForInsertion()           2. markForGarbageCollection()
...
3. checkLayersExist()           ...
4. checkGCDidntStart() // fails due to ongoing gc
5. insertManifest()
```

```
PutManifest                       GC
1. markForInsertion()           4. markForGarbageCollection()
...
2. checkLayersExist()           6. mark = getInsertionMark();
3. checkGCDidntStart()          7. ... finds a layer to remove
5. insertManifest()             8. checkOnGoingUpdates() // fails due to ongoing updates
9. unmarkForInsertion()
```

```
PutManifest                       GC
1. markForInsertion()           4. markForGarbageCollection()
...
2. checkLayersExist()           6. mark = getInsertionMark();
3. checkGCDidntStart()          7. ... finds a layer to remove
5. insertManifest()             9. checkOnGoingUpdates()
8. unmarkForInsertion()         10. checkMark(mark) // this fails, not latest, can't delete layer
```

```
PutManifest                           GC
4. markForInsertion()                 1. markForGarbageCollection()
5. gcMark = getGCMark()
6. checkLayersExist()                 2. mark = getInsertionMark();
                                      3. checkOngoingUpdates() and checkMark(mark)
                                      7. deleteLayer() and unmarkGarbageCollector();
8. checkGCDidntStart(gcMark) // fails because latest gc marker is different
```

It's a pattern where you build the state you need a lock in, get the mark of when you built that world,
and confirm before making changes from that view that there is nothing that might've changed the view.
