#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

: "${REPO_FILE:?}"
: "${SUB_MGR_FILE:?}"
: "${IMAGE_TAG:=v4.0}"

REPO_MNT="$REPO_FILE:/etc/yum.repos.d/redhat.repo"
SUB_MGR_MNT="$SUB_MGR_FILE:/etc/yum/pluginconf.d/subscription-manager.conf"
imagebuilder \
    -mount "$REPO_MNT" \
    -mount "$SUB_MGR_MNT" \
    -t "registry.access.redhat.com/openshift/ose-hadoop:$IMAGE_TAG" \
    -f "$DIR/Dockerfile.rhel" \
    "$DIR"

docker tag \
    "registry.access.redhat.com/openshift/ose-hadoop:$IMAGE_TAG" \
    "openshift/ose-hadoop:$IMAGE_TAG"
