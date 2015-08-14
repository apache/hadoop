
if [[ -z "${GRADLE:-}" ]]; then
  GRADLE=gradle
fi

if [[ -z "${GRADLEW:-}" ]]; then
  GRADLEW=./gradlew
fi

add_build_tool gradle


declare -a GRADLE_ARGS=()

function gradle_usage
{
  echo "gradle specific:"
  echo "--gradle-cmd=<cmd>        The 'gradle' command to use (default 'gradle')"
}

function gradle_parse_args
{
  local i

  for i in "$@"; do
    case ${i} in
      --gradle-cmd=*)
        GRADLE=${i#*=}
      ;;
    esac
  done
}


function gradle_buildfile
{
  echo "gradlew"
}

function gradle_executor
{
  echo "${GRADLEW}" "${GRADLE_ARGS[@]}"
}

## @description  Bootstrap gradle
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function gradle_precheck_install
{
  local result=0

  if [[ ${BUILDTOOL} != gradle ]]; then
    return 0
  fi

  pushd "${BASEDIR}" >/dev/null
  echo_and_redirect ${PATCH_DIR}/branch-gradle-bootstrap.txt gradle -b bootstrap.gradle
  popd >/dev/null

  personality_modules branch gradleboot
  modules_workers branch gradleboot
  result=$?
  modules_messages branch gradleboot true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}


## @description  Bootstrap gradle
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function gradle_postapply_install
{
  local result=0

  if [[ ${BUILDTOOL} != gradle ]]; then
    return 0
  fi

  pushd "${BASEDIR}" >/dev/null
  echo_and_redirect "${PATCH_DIR}/patch-gradle-bootstrap.txt" gradle -b bootstrap.gradle
  popd >/dev/null

  personality_modules patch gradleboot
  modules_workers patch gradleboot
  result=$?
  modules_messages patch gradleboot true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

function gradle_count_javac_probs
{
  echo 0
}

function gradle_count_javadoc_probs
{
  local warningfile=$1

  #shellcheck disable=SC2016,SC2046
  ${GREP} -E "^[0-9]+ warnings?$" "${warningfile}" | ${AWK} '{sum+=$1} END {print sum}'
}

## @description  Helper for check_patch_javadoc
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function gradle_count_scaladoc_probs
{
  local warningfile=$1

  #shellcheck disable=SC2016,SC2046
  ${GREP} "^\[ant:scaladoc\]" "${warningfile}" | wc -l
}

function gradle_modules_worker
{
  declare branch=$1
  declare tst=$2
  shift 2

  case ${tst} in
    checkstyle)
      modules_workers ${branch} ${tst} checkstyleMain checkstyleTest
    ;;
    javac|scalac)
      modules_workers ${branch} ${tst} clean build
    ;;
    javadoc)
      modules_workers ${branch} javadoc javadoc
    ;;
    scaladoc)
      modules_workers ${branch} scaladoc scaladoc
    ;;
    unit)
      modules_workers ${branch} unit test
    ;;
    *)
      yetus_error "WARNING: ${tst} is unsupported by ${BUILDTOOL}"
      return 1
    ;;
  esac
}

function gradle_builtin_personality_file_tests
{
  local filename=$1

  yetus_debug "Using builtin gradle personality_file_tests"

  if [[ ${filename} =~ src/main/webapp ]]; then
    yetus_debug "tests/webapp: ${filename}"
  elif [[ ${filename} =~ \.sh
       || ${filename} =~ \.cmd
       || ${filename} =~ src/main/scripts
       || ${filename} =~ src/test/scripts
       ]]; then
    yetus_debug "tests/shell: ${filename}"
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
  elif [[ ${filename} =~ \.scala$ ]]; then
    add_test scalac
    add_test scaladoc
    add_test unit
  elif [[ ${filename} =~ build.xml$
       || ${filename} =~ pom.xml$
       || ${filename} =~ \.java$
       || ${filename} =~ src/main
       ]]; then
      yetus_debug "tests/javadoc+units: ${filename}"
      add_test javac
      add_test javadoc
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