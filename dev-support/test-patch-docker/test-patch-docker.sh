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

DID=${RANDOM}

## @description  Print a message to stderr if --debug is turned on
## @audience     private
## @stability    stable
## @replaceable  no
## @param        string
function yetus_debug
{
  if [[ -n "${TP_SHELL_SCRIPT_DEBUG}" ]]; then
    echo "[$(date) DEBUG]: $*" 1>&2
  fi
}

## @description  Run docker with some arguments, and
## @description  optionally send to debug
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function dockercmd
{
  yetus_debug "docker $*"
  docker "$@"
}

## @description  Handle command line arguments
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function parse_args
{
  local i

  for i in "$@"; do
    case ${i} in
      --debug)
        TP_SHELL_SCRIPT_DEBUG=true
      ;;
      --dockerversion=*)
        DOCKER_VERSION=${i#*=}
      ;;
      --help|-help|-h|help|--h|--\?|-\?|\?)
        yetus_usage
        exit 0
      ;;
      --java-home=*)
        JAVA_HOME=${i#*=}
      ;;
      --patch-dir=*)
        PATCH_DIR=${i#*=}
      ;;
      --project=*)
        PROJECT_NAME=${i#*=}
      ;;
      *)
      ;;
    esac
  done
}

## @description  Stop and delete all defunct containers
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function stop_exited_containers
{
  local line
  local id
  local value
  local size

  echo "Docker containers in exit state:"

  dockercmd ps -a | grep Exited

  # stop *all* containers that are in exit state for
  # more than > 8 hours
  while read line; do
     id=$(echo "${line}" | cut -f1 -d' ')
     value=$(echo "${line}" | cut -f2 -d' ')
     size=$(echo "${line}" | cut -f3 -d' ')

     if [[ ${size} =~ day
        || ${size} =~ week
        || ${size} =~ month
        || ${size} =~ year ]]; then
          echo "Removing docker ${id}"
          dockercmd rm "${id}"
     fi

     if [[ ${size} =~ hours
        && ${value} -gt 8 ]]; then
        echo "Removing docker ${id}"
        dockercmd rm "${id}"
     fi
  done < <(
    dockercmd ps -a \
    | grep Exited \
    | sed -e 's,ago,,g' \
    | awk '{print $1" "$(NF - 2)" "$(NF - 1)}')
}

## @description  Remove all containers that are not
## @description  are not running + older than 1 day
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function rm_old_containers
{
  local line
  local id
  local value
  local size

  while read line; do
    id=$(echo "${line}" | cut -f1 -d, )
    state=$(echo "${line}" | cut -f2 -d, )
    stoptime=$(echo "${line}" | cut -f3 -d, | cut -f1 -d. )

    # believe it or not, date is not even close to standardized...
    if [[ $(uname -s) == Linux ]]; then

      # GNU date
      stoptime=$(date -d "${stoptime}" "+%s")
    else

      # BSD date
      stoptime=$(date -j -f "%Y-%m-%dT%H:%M:%S" "${stoptime}" "+%s")
    fi

    if [[ ${state} == false ]]; then
      curtime=$(date "+%s")
      ((difftime = curtime - stoptime))
      if [[ ${difftime} -gt 86400 ]]; then
        echo "Removing docker ${id}"
        dockercmd rm "${id}"
      fi
    fi
  done < <(
   # see https://github.com/koalaman/shellcheck/issues/375
   # shellcheck disable=SC2046
    dockercmd inspect \
      -f '{{.Id}},{{.State.Running}},{{.State.FinishedAt}}' \
       $(dockercmd ps -qa) 2>/dev/null)
}

## @description  Remove untagged/unused images
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function remove_untagged_images
{
  # this way is a bit more compatible with older docker versions
  dockercmd images | tail -n +2 | awk '$1 == "<none>" {print $3}' | \
    xargs --no-run-if-empty docker rmi
}

## @description  Remove defunct tagged images
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function remove_old_tagged_images
{
  local line
  local id
  local created

  while read line; do
    id=$(echo "${line}" | awk '{print $1}')
    created=$(echo "${line}" | awk '{print $5}')

    if [[ ${created} =~ week
       || ${created} =~ month
       || ${created} =~ year ]]; then
         echo "Removing docker image ${id}"
         dockercmd rmi "${id}"
    fi

    if [[ ${id} =~ test-patch-base-${PROJECT_NAME}-date ]]; then
      if [[ ${created} =~ day
        || ${created} =~ hours ]]; then
        echo "Removing docker image ${id}"
        dockercmd rmi "${id}"
      fi
    fi
  done < <(dockercmd images)

}

## @description  Performance docker maintenance on Jenkins
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function cleanup_apache_jenkins_docker
{
  echo "=========================="
  echo "Docker Images:"
  dockercmd images
  echo "=========================="
  echo "Docker Containers:"
  dockercmd ps -a
  echo "=========================="

  stop_exited_containers

  rm_old_containers

  remove_untagged_images

  remove_old_tagged_images
}

## @description  Clean up our old images used for patch testing
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function cleanup_test_patch_images
{
  local images
  local imagecount
  local rmimage
  local rmi

  # we always want to leave at least one of our images
  # so that the whole thing doesn't have to be rebuilt.
  # This also let's us purge any old images so that
  # we can get fresh stuff sometimes
  images=$(dockercmd images | grep --color=none "test-patch-tp-${PROJECT_NAME}" | awk '{print $1}') 2>&1

  # shellcheck disable=SC2086
  imagecount=$(echo ${images} | tr ' ' '\n' | wc -l)
  ((imagecount = imagecount - 1 ))

  # shellcheck disable=SC2086
  rmimage=$(echo ${images} | tr ' ' '\n' | tail -${imagecount})
  for rmi in ${rmimage}
  do
    echo "Removing image ${rmi}"
    dockercmd rmi "${rmi}"
  done
}

## @description  Perform pre-run maintenance to free up
## @description  resources. With --jenkins, it is a lot
## @description  more destructive.
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function cleanup
{
  if [[ ${TESTPATCHMODE} =~ jenkins ]]; then
    cleanup_apache_jenkins_docker
  fi

  cleanup_test_patch_images
}

## @description  Deterine the user name and user id of the user
## @description  that the docker container should use
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function determine_user
{
  # On the Apache Jenkins hosts, $USER is pretty much untrustable beacuse some
  # ... person ... sets it to an account that doesn't actually exist.
  # so instead, we need to try and override it with something that's
  # probably close to reality.
  if [[ ${TESTPATCHMODE} =~ jenkins ]]; then
    USER=$(id | cut -f2 -d\( | cut -f1 -d\))
  fi

  if [[ "$(uname -s)" == "Linux" ]]; then
    USER_NAME=${SUDO_USER:=$USER}
    USER_ID=$(id -u "${USER_NAME}")
    GROUP_ID=$(id -g "${USER_NAME}")
  else # boot2docker uid and gid
    USER_NAME=${USER}
    USER_ID=1000
    GROUP_ID=50
  fi
}

## @description  Determine the revision of a dockerfile
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function getdockerfilerev
{
  grep 'TEST_PATCH_PRIVATE: gitrev=' \
        "${PATCH_DIR}/precommit/test-patch-docker/Dockerfile" \
          | cut -f2 -d=
}

## @description  Start a test patch docker container
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        args
function run_image
{
  local dockerfilerev
  local baseimagename

  dockerfilerev=$(getdockerfilerev)

  baseimagename="test-patch-base-${PROJECT_NAME}-${dockerfilerev}"

  # make a base image, if it isn't available
  dockercmd build -t "${baseimagename}" "${PATCH_DIR}/precommit/test-patch-docker"

  # using the base image, make one that is patch specific
  dockercmd build -t "test-patch-tp-${PROJECT_NAME}-${DID}" - <<PatchSpecificDocker
FROM ${baseimagename}
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME}
RUN useradd -g ${GROUP_ID} -u ${USER_ID} -m ${USER_NAME}
RUN chown -R ${USER_NAME} /home/${USER_NAME}
ENV HOME /home/${USER_NAME}
USER ${USER_NAME}
PatchSpecificDocker

  if [[ ${PATCH_DIR} =~ ^/ ]]; then
    dockercmd run --rm=true -i \
      -v "${PWD}:/testptch/${PROJECT_NAME}" \
      -v "${PATCH_DIR}:/testptch/patchprocess" \
      -v "${HOME}/.m2:${HOME}/.m2" \
      -u "${USER_NAME}" \
      -w "/testptch/${PROJECT_NAME}" \
      --env=BASEDIR="/testptch/${PROJECT_NAME}" \
      --env=DOCKER_VERSION="${DOCKER_VERSION} Image:${baseimagename}" \
      --env=JAVA_HOME="${JAVA_HOME}" \
      --env=PATCH_DIR=/testptch/patchprocess \
      --env=PROJECT_NAME="${PROJECT_NAME}" \
      --env=TESTPATCHMODE="${TESTPATCHMODE}" \
      "test-patch-tp-${PROJECT_NAME}-${DID}"
 else
    dockercmd run --rm=true -i \
      -v "${PWD}:/testptch/${PROJECT_NAME}" \
      -v "${HOME}/.m2:${HOME}/.m2" \
      -u "${USER_NAME}" \
      -w "/testptch/${PROJECT_NAME}" \
      --env=BASEDIR="/testptch/${PROJECT_NAME}" \
      --env=DOCKER_VERSION="${DOCKER_VERSION} Image:${baseimagename}" \
      --env=JAVA_HOME="${JAVA_HOME}" \
      --env=PATCH_DIR="${PATCH_DIR}" \
      --env=PROJECT_NAME="${PROJECT_NAME}" \
      --env=TESTPATCHMODE="${TESTPATCHMODE}" \
      "test-patch-tp-${PROJECT_NAME}-${DID}"
 fi
}

parse_args "$@"
cleanup
determine_user
run_image
