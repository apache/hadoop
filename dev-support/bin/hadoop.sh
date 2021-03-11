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

#
# SHELLDOC-IGNORE
#
# Override these to match Apache Hadoop's requirements
personality_plugins "all,-ant,-gradle,-scalac,-scaladoc"

## @description  Globals specific to this personality
## @audience     private
## @stability    evolving
function personality_globals
{
  # shellcheck disable=SC2034
  BUILDTOOL=maven
  #shellcheck disable=SC2034
  PATCH_BRANCH_DEFAULT=trunk
  #shellcheck disable=SC2034
  PATCH_NAMING_RULE="https://cwiki.apache.org/confluence/display/HADOOP/How+To+Contribute"
  #shellcheck disable=SC2034
  JIRA_ISSUE_RE='^(HADOOP|YARN|MAPREDUCE|HDFS)-[0-9]+$'
  #shellcheck disable=SC2034
  GITHUB_REPO_DEFAULT="apache/hadoop"

  HADOOP_HOMEBREW_DIR=${HADOOP_HOMEBREW_DIR:-$(brew --prefix 2>/dev/null)}
  if [[ -z "${HADOOP_HOMEBREW_DIR}" ]]; then
    HADOOP_HOMEBREW_DIR=/usr/local
  fi
}

function personality_parse_args
{
  declare i

  for i in "$@"; do
    case ${i} in
      --hadoop-isal-prefix=*)
        delete_parameter "${i}"
        ISAL_HOME=${i#*=}
      ;;
      --hadoop-openssl-prefix=*)
        delete_parameter "${i}"
        OPENSSL_HOME=${i#*=}
      ;;
      --hadoop-snappy-prefix=*)
        delete_parameter "${i}"
        SNAPPY_HOME=${i#*=}
      ;;
    esac
  done
}

## @description  Calculate the actual module ordering
## @audience     private
## @stability    evolving
## @param        ordering
function hadoop_order
{
  declare ordering=$1
  declare hadoopm

  if [[ ${ordering} = normal ]]; then
    hadoopm="${CHANGED_MODULES[*]}"
  elif [[ ${ordering} = union ]]; then
    hadoopm="${CHANGED_UNION_MODULES}"
  elif [[ ${ordering} = mvnsrc ]]; then
    hadoopm="${MAVEN_SRC_MODULES[*]}"
  elif [[ ${ordering} = mvnsrctest ]]; then
    hadoopm="${MAVEN_SRCTEST_MODULES[*]}"
  else
    hadoopm="${ordering}"
  fi
  echo "${hadoopm}"
}

## @description  Determine if it is safe to run parallel tests
## @audience     private
## @stability    evolving
## @param        ordering
function hadoop_test_parallel
{
  if [[ -f "${BASEDIR}/pom.xml" ]]; then
    HADOOP_VERSION=$(grep '<version>' "${BASEDIR}/pom.xml" \
        | head -1 \
        | "${SED}"  -e 's|^ *<version>||' -e 's|</version>.*$||' \
        | cut -f1 -d- )
    export HADOOP_VERSION
  else
    return 1
  fi

  hmajor=${HADOOP_VERSION%%\.*}
  hmajorminor=${HADOOP_VERSION%\.*}
  hminor=${hmajorminor##*\.}
  # ... and just for reference
  #hmicro=${HADOOP_VERSION##*\.}

  # Apache Hadoop v2.8.0 was the first one to really
  # get working parallel unit tests
  if [[ ${hmajor} -lt 3 && ${hminor} -lt 8 ]]; then
    return 1
  fi

  return 0
}

## @description  Install extra modules for unit tests
## @audience     private
## @stability    evolving
## @param        ordering
function hadoop_unittest_prereqs
{
  declare input=$1
  declare mods
  declare need_common=0
  declare building_common=0
  declare module
  declare flags
  declare fn

  # prior to running unit tests, hdfs needs libhadoop.so built
  # if we're building root, then this extra work is moot

  #shellcheck disable=SC2086
  mods=$(hadoop_order ${input})

  for module in ${mods}; do
    if [[ ${module} = hadoop-hdfs-project* ]]; then
      need_common=1
    elif [[ ${module} = hadoop-common-project/hadoop-common
      || ${module} = hadoop-common-project ]]; then
      building_common=1
    elif [[ ${module} = . ]]; then
      return
    fi
  done

  # Windows builds *ALWAYS* need hadoop-common compiled
  case ${OSTYPE} in
    Windows_NT|CYGWIN*|MINGW*|MSYS*)
      need_common=1
    ;;
  esac

  if [[ ${need_common} -eq 1
      && ${building_common} -eq 0 ]]; then
    echo "unit test pre-reqs:"
    module="hadoop-common-project/hadoop-common"
    fn=$(module_file_fragment "${module}")
    flags="$(hadoop_native_flags) $(yarn_ui2_flag)"
    pushd "${BASEDIR}/${module}" >/dev/null || return 1
    # shellcheck disable=SC2086
    echo_and_redirect "${PATCH_DIR}/maven-unit-prereq-${fn}-install.txt" \
      "${MAVEN}" "${MAVEN_ARGS[@]}" install -DskipTests ${flags}
    popd >/dev/null || return 1
  fi
}

## @description  Calculate the flags/settings for yarn-ui v2 build
## @description  based upon the OS
## @audience     private
## @stability    evolving
function yarn_ui2_flag
{

  if [[ ${BUILD_NATIVE} != true ]]; then
    return
  fi

  # Now it only tested on Linux/OSX, don't enable the profile on
  # windows until it get verified
  case ${OSTYPE} in
    Linux)
      # shellcheck disable=SC2086
      echo -Pyarn-ui
    ;;
    Darwin)
      echo -Pyarn-ui
    ;;
    *)
      # Do nothing
    ;;
  esac
}

## @description  Calculate the flags/settings for native code
## @description  based upon the OS
## @audience     private
## @stability    evolving
function hadoop_native_flags
{
  if [[ ${BUILD_NATIVE} != true ]]; then
    return
  fi

  declare -a args

  # Based upon HADOOP-11937
  #
  # Some notes:
  #
  # - getting fuse to compile on anything but Linux
  #   is always tricky.
  # - Darwin assumes homebrew is in use.
  # - HADOOP-12027 required for bzip2 on OS X.
  # - bzip2 is broken in lots of places
  #   (the shared library is considered experimental)
  #   e.g, HADOOP-12027 for OS X. so no -Drequire.bzip2
  #

  args=("-Drequire.test.libhadoop")

  if [[ -d "${ISAL_HOME}/include" ]]; then
    args=("${args[@]}" "-Disal.prefix=${ISAL_HOME}")
  fi

  if [[ -d "${OPENSSL_HOME}/include" ]]; then
    args=("${args[@]}" "-Dopenssl.prefix=${OPENSSL_HOME}")
  elif [[ -d "${HADOOP_HOMEBREW_DIR}/opt/openssl/" ]]; then
    args=("${args[@]}" "-Dopenssl.prefix=${HADOOP_HOMEBREW_DIR}/opt/openssl/")
  fi

  if [[ -d "${SNAPPY_HOME}/include" ]]; then
    args=("${args[@]}" "-Dsnappy.prefix=${SNAPPY_HOME}")
  elif [[ -d "${HADOOP_HOMEBREW_DIR}/include/snappy.h" ]]; then
    args=("${args[@]}" "-Dsnappy.prefix=${HADOOP_HOMEBREW_DIR}/opt/snappy")
  fi

  case ${OSTYPE} in
    Linux)
      # shellcheck disable=SC2086
      echo \
        -Pnative \
        -Drequire.fuse \
        -Drequire.openssl \
        -Drequire.snappy \
        -Drequire.valgrind \
        -Drequire.zstd \
        "${args[@]}"
    ;;
    Darwin)
      echo \
        "${args[@]}" \
        -Pnative \
        -Drequire.snappy  \
        -Drequire.openssl
    ;;
    Windows_NT|CYGWIN*|MINGW*|MSYS*)
      echo \
        "${args[@]}" \
        -Drequire.snappy -Drequire.openssl -Pnative-win
    ;;
    *)
      echo \
        "${args[@]}"
    ;;
  esac
}

## @description  Queue up modules for this personality
## @audience     private
## @stability    evolving
## @param        repostatus
## @param        testtype
function personality_modules
{
  declare repostatus=$1
  declare testtype=$2
  declare extra=""
  declare ordering="normal"
  declare needflags=false
  declare foundbats=false
  declare flags
  declare fn
  declare i
  declare hadoopm

  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  case ${testtype} in
    asflicense)
      # this is very fast and provides the full path if we do it from
      # the root of the source
      personality_enqueue_module .
      return
    ;;
    checkstyle)
      ordering="union"
      extra="-DskipTests"
    ;;
    compile)
      ordering="union"
      extra="-DskipTests"
      needflags=true

      # if something in common changed, we build the whole world
      if [[ "${CHANGED_MODULES[*]}" =~ hadoop-common ]]; then
        yetus_debug "hadoop personality: javac + hadoop-common = ordering set to . "
        ordering="."
      fi
    ;;
    distclean)
      ordering="."
      extra="-DskipTests"
    ;;
    javadoc)
      if [[ "${CHANGED_MODULES[*]}" =~ \. ]]; then
        ordering=.
      fi

      if [[ "${repostatus}" = patch && "${BUILDMODE}" = patch ]]; then
        echo "javadoc pre-reqs:"
        for i in hadoop-project \
          hadoop-common-project/hadoop-annotations; do
            fn=$(module_file_fragment "${i}")
            pushd "${BASEDIR}/${i}" >/dev/null || return 1
            echo "cd ${i}"
            echo_and_redirect "${PATCH_DIR}/maven-${fn}-install.txt" \
              "${MAVEN}" "${MAVEN_ARGS[@]}" install
            popd >/dev/null || return 1
        done
      fi
      extra="-Pdocs -DskipTests"
    ;;
    mvneclipse)
      if [[ "${CHANGED_MODULES[*]}" =~ \. ]]; then
        ordering=.
      fi
    ;;
    mvninstall)
      extra="-DskipTests"
      if [[ "${repostatus}" = branch || "${BUILDMODE}" = full ]]; then
        ordering=.
      fi
    ;;
    mvnsite)
      if [[ "${CHANGED_MODULES[*]}" =~ \. ]]; then
        ordering=.
      fi
    ;;
    unit)
      if [[ "${BUILDMODE}" = full ]]; then
        ordering=mvnsrc
      elif [[ "${CHANGED_MODULES[*]}" =~ \. ]]; then
        ordering=.
      fi

      if [[ ${TEST_PARALLEL} = "true" ]] ; then
        if hadoop_test_parallel; then
          extra="-Pparallel-tests"
          if [[ -n ${TEST_THREADS:-} ]]; then
            extra="${extra} -DtestsThreadCount=${TEST_THREADS}"
          fi
        fi
      fi
      needflags=true
      hadoop_unittest_prereqs "${ordering}"

      if ! verify_needed_test javac; then
        yetus_debug "hadoop: javac not requested"
        if ! verify_needed_test native; then
          yetus_debug "hadoop: native not requested"
          yetus_debug "hadoop: adding -DskipTests to unit test"
          extra="-DskipTests"
        fi
      fi

      for i in "${CHANGED_FILES[@]}"; do
        if [[ "${i}" =~ \.bats ]]; then
          foundbats=true
        fi
      done

      if ! verify_needed_test shellcheck && [[ ${foundbats} = false ]]; then
        yetus_debug "hadoop: NO shell code change detected; disabling shelltest profile"
        extra="${extra} -P!shelltest"
      else
        extra="${extra} -Pshelltest"
      fi
    ;;
    *)
      extra="-DskipTests"
    ;;
  esac

  if [[ ${needflags} = true ]]; then
    flags="$(hadoop_native_flags) $(yarn_ui2_flag)"
    extra="${extra} ${flags}"
  fi

  extra="-Ptest-patch ${extra}"
  for module in $(hadoop_order ${ordering}); do
    # shellcheck disable=SC2086
    personality_enqueue_module ${module} ${extra}
  done
}

## @description  Add tests based upon personality needs
## @audience     private
## @stability    evolving
## @param        filename
function personality_file_tests
{
  declare filename=$1

  yetus_debug "Using Hadoop-specific personality_file_tests"

  if [[ ${filename} =~ src/main/webapp ]]; then
    yetus_debug "tests/webapp: ${filename}"
    add_test shadedclient
  elif [[ ${filename} =~ \.sh
       || ${filename} =~ \.cmd
       || ${filename} =~ src/scripts
       || ${filename} =~ src/test/scripts
       || ${filename} =~ src/main/bin
       || ${filename} =~ shellprofile\.d
       || ${filename} =~ src/main/conf
       ]]; then
    yetus_debug "tests/shell: ${filename}"
    add_test mvnsite
    add_test unit
  elif [[ ${filename} =~ \.md$
       || ${filename} =~ \.md\.vm$
       || ${filename} =~ src/site
       ]]; then
    yetus_debug "tests/site: ${filename}"
    add_test mvnsite
  elif [[ ${filename} =~ \.c$
       || ${filename} =~ \.cc$
       || ${filename} =~ \.h$
       || ${filename} =~ \.hh$
       || ${filename} =~ \.proto$
       || ${filename} =~ \.cmake$
       || ${filename} =~ CMakeLists.txt
       ]]; then
    yetus_debug "tests/units: ${filename}"
    add_test compile
    add_test cc
    add_test mvnsite
    add_test javac
    add_test unit
  elif [[ ${filename} =~ build.xml$
       || ${filename} =~ pom.xml$
       || ${filename} =~ \.java$
       || ${filename} =~ src/main
       ]]; then
      yetus_debug "tests/javadoc+units: ${filename}"
      add_test compile
      add_test javac
      add_test javadoc
      add_test mvninstall
      add_test mvnsite
      add_test unit
      add_test shadedclient
  fi

  # if we change anything in here, e.g. the test scripts
  # then run the client artifact tests
  if [[ ${filename} =~ hadoop-client-modules ]]; then
    add_test shadedclient
  fi

  if [[ ${filename} =~ src/test ]]; then
    yetus_debug "tests: src/test"
    add_test unit
  fi

  if [[ ${filename} =~ \.java$ ]]; then
    add_test spotbugs
  fi
}

## @description  Image to print on success
## @audience     private
## @stability    evolving
function hadoop_console_success
{
  printf "IF9fX19fX19fX18gCjwgU3VjY2VzcyEgPgogLS0tLS0tLS0tLSAKIFwgICAg";
  printf "IC9cICBfX18gIC9cCiAgXCAgIC8vIFwvICAgXC8gXFwKICAgICAoKCAgICBP";
  printf "IE8gICAgKSkKICAgICAgXFwgLyAgICAgXCAvLwogICAgICAgXC8gIHwgfCAg";
  printf "XC8gCiAgICAgICAgfCAgfCB8ICB8ICAKICAgICAgICB8ICB8IHwgIHwgIAog";
  printf "ICAgICAgIHwgICBvICAgfCAgCiAgICAgICAgfCB8ICAgfCB8ICAKICAgICAg";
  printf "ICB8bXwgICB8bXwgIAo"
}

###################################################
# Hadoop project specific check of IT for shaded artifacts

add_test_type shadedclient

## @description check for test modules and add test/plugins as needed
## @audience private
## @stability evolving
function shadedclient_initialize
{
  maven_add_install shadedclient
}

## @description build client facing shaded artifacts and test them
## @audience private
## @stability evolving
## @param repostatus
function shadedclient_rebuild
{
  declare repostatus=$1
  declare logfile="${PATCH_DIR}/${repostatus}-shadedclient.txt"
  declare module
  declare -a modules=()

  if [[ ${OSTYPE} = Windows_NT ||
        ${OSTYPE} =~ ^CYGWIN.* ||
        ${OSTYPE} =~ ^MINGW32.* ||
        ${OSTYPE} =~ ^MSYS.* ]]; then
    echo "hadoop personality: building on windows, skipping check of client artifacts."
    return 0
  fi

  yetus_debug "hadoop personality: seeing if we need the test of client artifacts."
  for module in hadoop-client-modules/hadoop-client-check-invariants \
                hadoop-client-modules/hadoop-client-check-test-invariants \
                hadoop-client-modules/hadoop-client-integration-tests; do
    if [ -d "${module}" ]; then
      yetus_debug "hadoop personality: test module '${module}' is present."
      modules+=(-pl "${module}")
    fi
  done
  if [ ${#modules[@]} -eq 0 ]; then
    echo "hadoop personality: no test modules present, skipping check of client artifacts."
    return 0
  fi

  big_console_header "Checking client artifacts on ${repostatus}"

  echo_and_redirect "${logfile}" \
    "${MAVEN}" "${MAVEN_ARGS[@]}" verify -fae --batch-mode -am \
      "${modules[@]}" \
      -Dtest=NoUnitTests -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true

  count=$("${GREP}" -c '\[ERROR\]' "${logfile}")
  if [[ ${count} -gt 0 ]]; then
    add_vote_table -1 shadedclient "${repostatus} has errors when building and testing our client artifacts."
    return 1
  fi

  add_vote_table +1 shadedclient "${repostatus} has no errors when building and testing our client artifacts."
  return 0
}
