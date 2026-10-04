#!/bin/bash
#
# Run pogocache's protocol tests against a live gopogo server.
#
#   ./run.sh [go test args...]
#
# Starts bin/gopogo on 127.0.0.1:9401 with every protocol enabled (the same
# settings as upstream's tools/tests/run.sh: --shards=128 --cas=yes), runs the
# tests and stops the server however the tests exit. GOPOGO_BIN overrides the
# server binary.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

bin="${GOPOGO_BIN:-../../bin/gopogo}"
addr=127.0.0.1
port=9401

if (exec 3<>"/dev/tcp/$addr/$port") 2>/dev/null; then
    echo "something is already listening on $addr:$port" >&2
    exit 1
fi

"$bin" --host "$addr" --port "$port" --shards 128 --cas \
    --redis --http --memcache --postgres --quiet &
server=$!
trap 'kill "$server" 2>/dev/null; wait "$server" 2>/dev/null || true' EXIT

for _ in $(seq 100); do
    if (exec 3<>"/dev/tcp/$addr/$port") 2>/dev/null; then
        break
    fi
    if ! kill -0 "$server" 2>/dev/null; then
        echo "gopogo exited before listening on $addr:$port" >&2
        exit 1
    fi
    sleep 0.05
done

go test -v -count=1 "$@" .
