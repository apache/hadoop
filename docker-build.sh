#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build \
    -t quay.io/coreos/hadoop:metering-3.1.1 \
    -f "$DIR/Dockerfile" \
    "$DIR"
