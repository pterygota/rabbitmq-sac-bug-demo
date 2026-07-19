#!/usr/bin/env bash
# Spin up either stock or patched RabbitMQ 4.3.2, wait for the stream port to be
# ready, run the Python repro, tear the broker down. Exits non-zero when the
# repro observes stranded partitions.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
mode="${1:-stock}"
container="sac-bug-demo-$$"

case "$mode" in
  stock)
    image="rabbitmq:4.3.2-management"
    ;;
  patched)
    image="rabbitmq-sac-patched:4.3.2"
    if ! docker image inspect "$image" >/dev/null 2>&1; then
      echo ">>> building patched image $image from local Dockerfile"
      # BuildKit's default docker-container driver breaks under some rootless /
      # nix-provided crun setups. Force the classic builder for portability.
      DOCKER_BUILDKIT=0 docker build -t "$image" "$here"
    fi
    ;;
  *)
    echo "usage: $0 [stock|patched]" >&2
    exit 2
    ;;
esac

cleanup() {
  docker rm -f "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo ">>> starting $image (container $container)"
# Random host port so this works alongside anything else on 5552.
docker run -d --rm --name "$container" \
    -p 127.0.0.1::5552 \
    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
    "$image" >/dev/null
port=$(docker port "$container" 5552 | head -1 | sed 's/.*://')
echo ">>> broker stream port bound to 127.0.0.1:$port"

echo ">>> waiting for broker startup to complete"
# Watch the container logs rather than running `docker exec rabbitmq-diagnostics
# ping`. Running diagnostics against a still-booting rabbit reliably crashes
# the container on this image.
ready=0
for _ in $(seq 1 60); do
  # `docker logs | grep -q` + pipefail is a trap: grep exits early on match,
  # docker logs gets SIGPIPE, and pipefail flags the whole pipeline as failed.
  # Capture to a variable and match without a pipeline instead.
  logs=$(docker logs "$container" 2>&1 || true)
  if [[ "$logs" == *"Server startup complete"* ]]; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "!!! broker did not finish startup within 60s" >&2
  docker logs "$container" 2>&1 | tail -20 >&2
  exit 4
fi
docker exec "$container" rabbitmq-plugins enable rabbitmq_stream >/dev/null
echo ">>> waiting for stream port"
for _ in $(seq 1 30); do
  if bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null; then
    exec 3<&- 3>&-
    break
  fi
  sleep 1
done

echo ">>> running Python repro against $mode broker"
cd "$here"
# uv reads the PEP 723 inline metadata block at the top of repro.py to pick
# rstream automatically; no manual venv/pip step needed.
shift || true
uv run repro.py --port "$port" "$@"
