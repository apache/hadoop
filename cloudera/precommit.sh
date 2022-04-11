#!/usr/bin/env bash
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.
#
# Gerrit is not yet supported by Yetus officially: https://issues.apache.org/jira/browse/YETUS-61
#
# As a workaround this script creates a single patch file from a Gerrit relation chain (GERRIT_REFSPEC),
# then uses that patch file to test the changes for the GERRIT_BRANCH.
#
# Example usage:
#
#  $ GERRIT_REFSPEC=refs/changes/23/169323/1 GERRIT_BRANCH=cdpd-master ./cloudera/precommit.sh
#
#  $ PRECOMMIT_BRANCH=cdpd-master PATCH_FILE=/tmp/feature.patch ./cloudera/precommit.sh

set -e
set -u
set -o pipefail
set -xv

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

GERRIT_REFSPEC="${GERRIT_REFSPEC:-}"
GERRIT_BRANCH="${GERRIT_BRANCH:-}"
BUILD_URL="${BUILD_URL:-}"
WORKSPACE="${WORKSPACE:-"${SCRIPT_DIR}/.."}"

PATCH_FILE="${PATCH_FILE:-}"
GERRIT_ORIGIN="${GERRIT_ORIGIN:-https://gerrit.sjc.cloudera.com/cdh/hadoop}"
GIT_ORIGIN="${GIT_ORIGIN:-https://github.infra.cloudera.com/CDH/hadoop.git}"
PRECOMMIT_BRANCH="${PRECOMMIT_BRANCH:-precommit-${GERRIT_BRANCH}}"

echo "Creating a temp directory..."
TEMP_DIRECTORY=$(mktemp -d /tmp/hadoop_gerrit-${GERRIT_REFSPEC//\//_}-${GERRIT_BRANCH}.XXXXXX)
echo "Temp directory created: $TEMP_DIRECTORY"

YETUSDIR=${TEMP_DIRECTORY}/yetus
ARTIFACTS=${WORKSPACE}/cloudera/precommit_artifacts
SOURCEDIR=${WORKSPACE}

rm -rf "${ARTIFACTS}" "${YETUSDIR}"
mkdir -p "${ARTIFACTS}" "${YETUSDIR}"

if [[ -d /sys/fs/cgroup/pids/user.slice ]]; then
  pids=$(cat /sys/fs/cgroup/pids/user.slice/user-910.slice/pids.max)
  if [[ ${pids} -gt 13000 ]]; then
    PIDMAX=10000
  else
    PIDMAX=5500
  fi
else
  PIDMAX=10000
fi

echo "Downloading Yetus 0.13.0-SNAPSHOT..."
curl -L https://api.github.com/repos/apache/yetus/tarball/6ab19e71eaf3234863424c6f684b34c1d3dcc0ce -o "${TEMP_DIRECTORY}"/yetus.tar.gz
cp ${SOURCEDIR}/cloudera/yetus.tar.gz.md5 "${TEMP_DIRECTORY}"/.
cd "${TEMP_DIRECTORY}"/. && md5sum -c yetus.tar.gz.md5 && cd -
gunzip -c "${TEMP_DIRECTORY}"/yetus.tar.gz | tar xpf - -C "${YETUSDIR}" --strip-components 1
echo "Yetus is downloaded"


if [ ! -f "$PATCH_FILE" ]; then
    if [ ! -z "$GERRIT_REFSPEC" ]; then
        # The Gerrit-Trigger git plugin (https://plugins.jenkins.io/gerrit-trigger/) checkouts the GERRIT_REFSPEC
        # commits for the GERRIT_BRANCH. Let's do the same.

        echo "Creating patch file from gerrit refspec=$GERRIT_REFSPEC branch=$GERRIT_BRANCH ..."

        # Get the patches from Gerrit and create a relation_chain branch
        git branch -D relation_chain &>/dev/null || true
        git fetch "${GERRIT_ORIGIN}" "${GERRIT_REFSPEC}" && git checkout FETCH_HEAD
        git switch -c relation_chain

        # Create a patch file from the patches in the relation chain
        git branch -D "${PRECOMMIT_BRANCH}" &>/dev/null || true
        git checkout -b "${PRECOMMIT_BRANCH}"
        git remote remove temporary_git_remote &>/dev/null || true
        git remote add temporary_git_remote ${GIT_ORIGIN}
        git fetch temporary_git_remote "${GERRIT_BRANCH}"
        git reset --hard  temporary_git_remote/"${GERRIT_BRANCH}"
        gitc='git -c user.name="jenkins" -c user.email="jenkins@cloudera.com"'
        $gitc merge --squash relation_chain
        $gitc commit -m "Patch for Gerrit REFSPEC=${GERRIT_REFSPEC}"
        PATCH_FILE=$(git format-patch HEAD^ -o "${TEMP_DIRECTORY}")

        echo "Patch file is created: ${PATCH_FILE}"
        echo
        echo "#########################################"
        cat "${PATCH_FILE}"
        echo "#########################################"

        # Reset back the repo to the GERRIT_BRANCH
        git reset --hard HEAD^
    fi
fi

YETUS_ARGS+=("--archive-list=checkstyle-errors.xml,spotbugsXml.xml")
YETUS_ARGS+=("--basedir=${SOURCEDIR}")
YETUS_ARGS+=("--personality=${SOURCEDIR}/cloudera/hadoop_personality.sh")
YETUS_ARGS+=("--mvn-settings=${SOURCEDIR}/cloudera/settings.xml")

YETUS_ARGS+=("--build-url=${BUILD_URL}")
YETUS_ARGS+=("--build-url-artifacts=artifact/hadoop/cloudera/precommit_artifacts")
YETUS_ARGS+=("--patch-dir=${ARTIFACTS}")
YETUS_ARGS+=("--console-report-file=${ARTIFACTS}/console-report.txt")
YETUS_ARGS+=("--html-report-file=${ARTIFACTS}/console-report.html")
YETUS_ARGS+=("--brief-report-file=${ARTIFACTS}/email-report.txt")

YETUS_ARGS+=("--branch=${PRECOMMIT_BRANCH}")
YETUS_ARGS+=("--project=hadoop")
YETUS_ARGS+=("--console-urls")
YETUS_ARGS+=("--mvn-custom-repos")
YETUS_ARGS+=("--proclimit=${PIDMAX}")
YETUS_ARGS+=("--reapermode=kill")
YETUS_ARGS+=("--git-offline")
YETUS_ARGS+=("--git-shallow")
YETUS_ARGS+=("--robot")
YETUS_ARGS+=("--sentinel")
YETUS_ARGS+=("--resetrepo")
YETUS_ARGS+=("--tests-filter=checkstyle,pylint")
YETUS_ARGS+=("--mvn-javadoc-goals=process-sources,javadoc:javadoc-no-fork")
YETUS_ARGS+=("--skip-dirs=dev-support,cloudera")
YETUS_ARGS+=("--excludes=${SOURCEDIR}/cloudera/precommit-excludes.txt")

YETUS_ARGS+=("${PATCH_FILE}")

export MAVEN_OPTS="-Xms256m -Xmx1536m -Dhttps.protocols=TLSv1.2 -Dhttps.cipherSuites=TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"

# Removing the artifacts from the gitdiffs*, it's not configurable on Yetus yet
trap "sed -i.bak '/^cloudera\/precommit_artifacts/d' ${ARTIFACTS}/gitdiff{lines,content}.txt &>/dev/null || true" EXIT

TESTPATCHBIN=${YETUSDIR}/precommit/src/main/shell/test-patch.sh
/bin/bash ${TESTPATCHBIN} "${YETUS_ARGS[@]}"
