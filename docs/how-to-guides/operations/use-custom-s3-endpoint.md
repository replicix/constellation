# Use a custom S3 endpoint

Point Constellation at an S3-compatible daemon on another host — for
example [floci](https://github.com/floci-io/floci) listening on port
**4566** — instead of Amazon S3. Create the bucket first, then create
the Constellation filesystem prefix and mount as usual.

## Before you start

- The daemon is reachable from this machine (TCP to `HOST:4566`).
- You have credentials the daemon accepts (floci's default is
  `test` / `test` with no real SigV4 enforcement).
- A built `constellation` binary is on `PATH`, or use the path from
  `cargo build -p constellation --release`.

## 1. Point the AWS client at the daemon

Constellation builds its S3 client via `AmazonS3Builder::from_env()`, so
the same Localstack-style knobs the tests use apply here:

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT=http://HOST:4566    # replace HOST with the daemon's hostname or IP
export AWS_ALLOW_HTTP=true             # required for plain http:// endpoints
```

`AWS_ENDPOINT` (also accepted as `AWS_ENDPOINT_URL`) overrides the
regional Amazon endpoint. Leave virtual-hosted-style unset: path-style
requests (`http://HOST:4566/bucket/...`) are the default and match
floci, Localstack, and most self-hosted S3 APIs.

Omit `AWS_ALLOW_HTTP` only when the endpoint is `https://`.

## 2. Create the bucket

Constellation does **not** create the S3 bucket. Create it once with the
AWS CLI against the same endpoint:

```bash
aws --endpoint-url "$AWS_ENDPOINT" s3 mb s3://my-bucket
```

Or with an anonymous path-style PUT (works on floci's default auth):

```bash
curl -sf -X PUT "$AWS_ENDPOINT/my-bucket"
```

Confirm:

```bash
aws --endpoint-url "$AWS_ENDPOINT" s3 ls
# expect: my-bucket
```

If the bucket already exists, `mb` may fail with `BucketAlreadyOwnedByYou`
or `BucketAlreadyExists`; that is fine.

## 3. Create the Constellation filesystem and mount

Pick an empty prefix inside the bucket (first create):

```bash
BUCKET=s3://my-bucket/constellation-demo
mkdir -p /mnt/constellation

constellation doctor --s3 "$BUCKET"      # checks If-None-Match / If-Match
constellation fs create --s3 "$BUCKET"   # once per prefix
constellation mount --s3 "$BUCKET" /mnt/constellation
```

`doctor` must report working conditional puts; multi-node leases need
them. Unmount with `fusermount3 -u /mnt/constellation`.

## Example: floci on another host

On the S3 host (or any machine that can reach Docker on it):

```bash
docker run -d --name floci -p 4566:4566 \
  -e FLOCI_STORAGE_MODE=memory \
  floci/floci:1.7.0-compat
```

On the Constellation host (`192.0.2.10` is the floci host):

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT=http://192.0.2.10:4566
export AWS_ALLOW_HTTP=true

aws --endpoint-url "$AWS_ENDPOINT" s3 mb s3://constellation-ci
constellation doctor --s3 s3://constellation-ci/demo
constellation fs create --s3 s3://constellation-ci/demo
constellation mount --s3 s3://constellation-ci/demo /mnt/constellation
```

The repo's `docker compose` floci service pre-creates `constellation-ci`
via `tests/docker/floci-init.sh` when you run compose locally; a remote
daemon needs the bucket step above.

## Related

- [Configuration](../../reference/configuration.md) — Constellation env
  vars (`CONSTELLATION_S3_*` retries, etc.)
- [README quick start](../../../README.md#quick-start-mount-a-real-s3-bucket) —
  Amazon S3 / profile / SSO credentials without a custom endpoint
