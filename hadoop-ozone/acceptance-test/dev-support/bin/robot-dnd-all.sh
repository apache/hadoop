#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

#Dir od the definition of the dind based test exeucution container
DOCKERDIR="$DIR/../docker"

#Dir to save the results
TARGETDIR="$DIR/../../target/dnd"

#Dir to mount the distribution from
OZONEDIST="$DIR/../../../../hadoop-dist/target/ozone"

#Name and imagename of the temporary, dind based test containers
DOCKER_IMAGE_NAME=ozoneacceptance
DOCKER_INSTANCE_NAME="${DOCKER_INSTANCE_NAME:-ozoneacceptance}"

teardown() {
   docker stop "$DOCKER_INSTANCE_NAME"
}

trap teardown EXIT

#Make sure it will work even if the ozone is built by an other user. We 
# eneable to run the distribution by an other user
mkdir -p "$TARGETDIR"
mkdir -p "$OZONEDIST/logs"
chmod o+w "$OZONEDIST/logs" || true
chmod -R o+w "$OZONEDIST/etc/hadoop" || true
chmod o+w "$OZONEDIST" || true

rm "$TARGETDIR/docker-compose.log"
docker rm "$DOCKER_INSTANCE_NAME" || true
docker build -t "$DOCKER_IMAGE_NAME" $DIR/../docker

#Starting the dind based environment
docker run --rm -v $DIR/../../../..:/opt/hadoop --privileged -d --name "$DOCKER_INSTANCE_NAME" $DOCKER_IMAGE_NAME
sleep 5

#Starting the tests
docker exec "$DOCKER_INSTANCE_NAME" /opt/hadoop/hadoop-ozone/acceptance-test/dev-support/bin/robot-all.sh
RESULT=$?

docker cp "$DOCKER_INSTANCE_NAME:/root/log.html" "$TARGETDIR/"
docker cp "$DOCKER_INSTANCE_NAME:/root/junit-results.xml" "$TARGETDIR/"
docker cp "$DOCKER_INSTANCE_NAME:/root/docker-compose.log" "$TARGETDIR/"
exit $RESULT
