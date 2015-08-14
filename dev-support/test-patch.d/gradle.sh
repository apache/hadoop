
function gradle_executor
{
  echo "${GRADLE}" "${GRADLE_ARGS[@]}"
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

  personality_modules branch gradleboot
  modules_workers branch gradleboot
  result=$?
  modules_messages branch gradleboot true
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
  echo 0
}

function gradle_modules_worker
{
  declare branch=$1
  declare tst=$2
  shift 2

  case ${tst} in
    javac)
      modules_workers ${branch} javac
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

  personality_modules patch gradleboot
  modules_workers branch gradleboot
  result=$?
  modules_messages patch gradleboot true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}