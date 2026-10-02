# Shared by the kind lanes (tests/csi/sanity-kind.sh, tests/csi/k3-smoke.sh):
# source it, it defines functions only.

# floci_up CONTAINER BUCKET: a private floci S3 on the `kind` docker network
# (in memory, so nothing outlives the container), BUCKET made; prints the
# endpoint the cluster reaches it at.
floci_up() {
    local name="$1" bucket="$2"
    docker rm -f "$name" >/dev/null 2>&1 || true
    docker run -d --name "$name" --network kind -e FLOCI_STORAGE_MODE=memory \
        floci/floci:1.7.0-compat >/dev/null
    for _ in $(seq 60); do
        docker exec "$name" curl -sf http://localhost:4566/_floci/health >/dev/null 2>&1 && break
        sleep 1
    done
    docker exec -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
        -e AWS_DEFAULT_REGION=us-east-1 "$name" \
        aws --endpoint-url http://localhost:4566 s3 mb "s3://$bucket" >/dev/null
    local ip
    ip=$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$name")
    echo "http://$ip:4566"
}

# build_image IMAGE: `make csi-image`, unless CSI_SKIP_BUILD=1 (an image
# built some other way, e.g. a musl build outside BuildKit).
build_image() {
    if [ "${CSI_SKIP_BUILD:-0}" != 1 ]; then
        echo "== building $1"
        make -C "$root" csi-image CSI_IMAGE="$1"
    fi
}
