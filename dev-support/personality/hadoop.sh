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

# Override these to match Apache Hadoop's requirements

#shellcheck disable=SC2034
PATCH_BRANCH_DEFAULT=trunk
#shellcheck disable=SC2034
HOW_TO_CONTRIBUTE="https://wiki.apache.org/hadoop/HowToContribute"
#shellcheck disable=SC2034
ISSUE_RE='^(HADOOP|YARN|MAPREDUCE|HDFS)-[0-9]+$'
#shellcheck disable=SC2034
PYLINT_OPTIONS="--indent-string='  '"

HADOOP_MODULES=""

function hadoop_module_manipulation
{
  local startingmodules=${1:-normal}
  local module
  local hdfs_modules
  local ordered_modules
  local tools_modules
  local passed_modules
  local flags

  yetus_debug "hmm in: ${startingmodules}"

  if [[ ${startingmodules} = normal ]]; then
    startingmodules=${CHANGED_MODULES}
  elif  [[ ${startingmodules} = union ]]; then
    startingmodules=${CHANGED_UNION_MODULES}
  fi

  yetus_debug "hmm expanded to: ${startingmodules}"

  if [[ ${startingmodules} = "." ]]; then
    yetus_debug "hmm shortcut since ."
    HADOOP_MODULES=.
    return
  fi

  # ${startingmodules} is already sorted and uniq'd.
  # let's remove child modules if we're going to
  # touch their parent.
  passed_modules=${startingmodules}
  for module in ${startingmodules}; do
    yetus_debug "Stripping ${module}"
    # shellcheck disable=SC2086
    passed_modules=$(echo ${passed_modules} | tr ' ' '\n' | ${GREP} -v ${module}/ )
  done

  yetus_debug "hmm pre-ordering: ${startingmodules}"

  # yarn will almost always be after common in the sort order
  # so really just need to make sure that common comes before
  # everything else and tools comes last

  for module in ${passed_modules}; do
    yetus_debug "Personality ordering ${module}"
    if [[ ${module} = "." ]]; then
      HADOOP_MODULES=.
      break
    fi

    if [[ ${module} = hadoop-hdfs-project* ]]; then
      hdfs_modules="${hdfs_modules} ${module}"
    elif [[ ${module} = hadoop-common-project/hadoop-common
      || ${module} = hadoop-common-project ]]; then
      ordered_modules="${ordered_modules} ${module}"
    elif [[ ${module} = hadoop-tools* ]]; then
      tools_modules="${tools_modules} ${module}"
    else
      ordered_modules="${ordered_modules} ${module}"
    fi
  done

  HADOOP_MODULES="${ordered_modules} ${hdfs_modules} ${tools_modules}"

  yetus_debug "hmm out: ${HADOOP_MODULES}"
}

function hadoop_unittest_prereqs
{
  local need_common=0
  local building_common=0
  local module
  local flags
  local fn

  for module in ${HADOOP_MODULES}; do
    if [[ ${module} = hadoop-hdfs-project* ]]; then
      need_common=1
    elif [[ ${module} = hadoop-common-project/hadoop-common
      || ${module} = hadoop-common-project ]]; then
      building_common=1
    fi
  done

  if [[ ${need_common} -eq 1
      && ${building_common} -eq 0 ]]; then
    echo "unit test pre-reqs:"
    module="hadoop-common-project/hadoop-common"
    fn=$(module_file_fragment "${module}")
    flags=$(hadoop_native_flags)
    pushd "${BASEDIR}/${module}" >/dev/null
    # shellcheck disable=SC2086
    echo_and_redirect "${PATCH_DIR}/maven-unit-prereq-${fn}-install.txt" \
      "${MVN}" "${MAVEN_ARGS[@]}" install -DskipTests ${flags}
    popd >/dev/null
  fi
}

function hadoop_native_flags
{

  if [[ ${BUILD_NATIVE} != true ]]; then
    return
  fi

  # Based upon HADOOP-11937
  #
  # Some notes:
  #
  # - getting fuse to compile on anything but Linux
  #   is always tricky.
  # - Darwin assumes homebrew is in use.
  # - HADOOP-12027 required for bzip2 on OS X.
  # - bzip2 is broken in lots of places.
  #   e.g, HADOOP-12027 for OS X. so no -Drequire.bzip2
  #

  # current build servers are pretty limited in
  # what they support
  if [[ ${JENKINS} = true
      && ${DOCKERSUPPORT} = false ]]; then
    # shellcheck disable=SC2086
    echo -Pnative \
      -Drequire.snappy -Drequire.openssl -Drequire.fuse \
      -Drequire.test.libhadoop
    return
  fi

  case ${OSTYPE} in
    Linux)
      # shellcheck disable=SC2086
      echo -Pnative -Drequire.libwebhdfs \
        -Drequire.snappy -Drequire.openssl -Drequire.fuse \
        -Drequire.test.libhadoop
    ;;
    Darwin)
      JANSSON_INCLUDE_DIR=/usr/local/opt/jansson/include
      JANSSON_LIBRARY=/usr/local/opt/jansson/lib
      export JANSSON_LIBRARY JANSSON_INCLUDE_DIR
      # shellcheck disable=SC2086
      echo \
      -Pnative -Drequire.snappy  \
      -Drequire.openssl \
        -Dopenssl.prefix=/usr/local/opt/openssl/ \
        -Dopenssl.include=/usr/local/opt/openssl/include \
        -Dopenssl.lib=/usr/local/opt/openssl/lib \
      -Drequire.libwebhdfs -Drequire.test.libhadoop
    ;;
    *)
      # shellcheck disable=SC2086
      echo \
        -Pnative \
        -Drequire.snappy -Drequire.openssl \
        -Drequire.libwebhdfs -Drequire.test.libhadoop
    ;;
  esac
}

function personality_modules
{
  local repostatus=$1
  local testtype=$2
  local extra=""
  local ordering="normal"
  local needflags=false
  local flags
  local fn
  local i

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
    javac)
      ordering="union"
      extra="-DskipTests"
      needflags=true

      # if something in common changed, we build the whole world
      if [[ ${CHANGED_MODULES} =~ hadoop-common ]]; then
        yetus_debug "hadoop personality: javac + hadoop-common = ordering set to . "
        ordering="."
      fi
      ;;
    javadoc)
      if [[ ${repostatus} = patch ]]; then
        echo "javadoc pre-reqs:"
        for i in  hadoop-project \
          hadoop-common-project/hadoop-annotations; do
            fn=$(module_file_fragment "${i}")
            pushd "${BASEDIR}/${i}" >/dev/null
            echo "cd ${i}"
            echo_and_redirect "${PATCH_DIR}/maven-${fn}-install.txt" \
              "${MVN}" "${MAVEN_ARGS[@]}" install
            popd >/dev/null
        done
      fi
      extra="-Pdocs -DskipTests"
    ;;
    mvninstall)
      extra="-DskipTests"
      if [[ ${repostatus} = branch ]]; then
        ordering=.
      fi
      ;;
    unit)
      # As soon as HADOOP-11984 gets committed,
      # this code should get uncommented
      #if [[ ${TEST_PARALLEL} = "true" ]] ; then
      #  extra="-Pparallel-tests"
      #  if [[ -n ${TEST_THREADS:-} ]]; then
      #    extra="${extra} -DtestsThreadCount=${TEST_THREADS}"
      #  fi
      #fi
      needflags=true
      hadoop_unittest_prereqs

      verify_needed_test javac
      if [[ $? == 0 ]]; then
        yetus_debug "hadoop: javac not requested"
        verify_needed_test native
        if [[ $? == 0 ]]; then
          yetus_debug "hadoop: native not requested"
          yetus_debug "hadoop: adding -DskipTests to unit test"
          extra="-DskipTests"
        fi
      fi

      verify_needed_test shellcheck
      if [[ $? == 0
          && ! ${CHANGED_FILES} =~ \.bats ]]; then
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
    flags=$(hadoop_native_flags)
    extra="${extra} ${flags}"
  fi

  hadoop_module_manipulation ${ordering}

  for module in ${HADOOP_MODULES}; do
    # shellcheck disable=SC2086
    personality_enqueue_module ${module} ${extra}
  done
}

function personality_file_tests
{
  local filename=$1

  yetus_debug "Using Hadoop-specific personality_file_tests"

  if [[ ${filename} =~ src/main/webapp ]]; then
    yetus_debug "tests/webapp: ${filename}"
  elif [[ ${filename} =~ \.sh
       || ${filename} =~ \.cmd
       || ${filename} =~ src/scripts
       || ${filename} =~ src/test/scripts
       ]]; then
    yetus_debug "tests/shell: ${filename}"
    add_test unit
  elif [[ ${filename} =~ \.md$
       || ${filename} =~ \.md\.vm$
       || ${filename} =~ src/site
       ]]; then
    yetus_debug "tests/site: ${filename}"
    add_test site
  elif [[ ${filename} =~ \.c$
       || ${filename} =~ \.cc$
       || ${filename} =~ \.h$
       || ${filename} =~ \.hh$
       || ${filename} =~ \.proto$
       || ${filename} =~ \.cmake$
       || ${filename} =~ CMakeLists.txt
       ]]; then
    yetus_debug "tests/units: ${filename}"
    add_test cc
    add_test unit
    add_test javac
  elif [[ ${filename} =~ build.xml$
       || ${filename} =~ pom.xml$
       || ${filename} =~ \.java$
       || ${filename} =~ src/main
       ]]; then
      yetus_debug "tests/javadoc+units: ${filename}"
      add_test javac
      add_test javadoc
      add_test mvninstall
      add_test unit
  fi

  if [[ ${filename} =~ src/test ]]; then
    yetus_debug "tests"
    add_test unit
  fi

  if [[ ${filename} =~ \.java$ ]]; then
    add_test findbugs
  fi
}
