
add_plugin mvnsite
add_plugin mvneclipse

function maven_buildfile
{
  echo "pom.xml"
}

function maven_executor
{
  echo "${MAVEN}" "${MAVEN_ARGS[@]}"
}

# if it ends in an explicit .sh, then this is shell code.
# if it doesn't have an extension, we assume it is shell code too
function mvnsite_filefilter
{
  local filename=$1

  if [[ ${BUILDTOOL} = maven ]]; then
    if [[ ${filename} =~ src/site ]]; then
       yetus_debug "tests/mvnsite: ${filename}"
       add_test mvnsite
     fi
   fi
}

function maven_modules_worker
{
  declare branch=$1
  declare tst=$2

  case ${tst} in
    javac)
      modules_workers ${branch} javac clean test-compile
    ;;
    javadoc)
      modules_workers ${branch} javadoc clean javadoc:javadoc
    ;;
    unit)
      modules_workers ${branch} unit clean test -fae
    ;;
    *)
      yetus_error "WARNING: ${tst} is unsupported by ${BUILDTOOL}"
      return 1
    ;;
  esac
}

function maven_count_javac_probs
{
  local warningfile=$1

  #shellcheck disable=SC2016,SC2046
  ${GREP} '\[WARNING\]' "${warningfile}" | ${AWK} '{sum+=1} END {print sum}'
}

## @description  Helper for check_patch_javadoc
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function maven_count_javadoc_probs
{
  local warningfile=$1

  #shellcheck disable=SC2016,SC2046
  ${GREP} -E "^[0-9]+ warnings?$" "${warningfile}" | ${AWK} '{sum+=$1} END {print sum}'
}

## @description  Confirm site pre-patch
## @audience     private
## @stability    stable
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function mvnsite_preapply
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  verify_needed_test mvnsite
  if [[ $? == 0 ]];then
    return 0
  fi
  big_console_header "Pre-patch ${PATCH_BRANCH} site verification"


  personality_modules branch mvnsite
  modules_workers branch mvnsite clean site site:stage
  result=$?
  modules_messages branch mvnsite true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Make sure site still compiles
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function mvnsite_postapply
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  verify_needed_test mvnsite
  if [[ $? == 0 ]]; then
    return 0
  fi

  big_console_header "Determining number of patched site errors"

  personality_modules patch mvnsite
  modules_workers patch mvnsite clean site site:stage -Dmaven.javadoc.skip=true
  result=$?
  modules_messages patch mvnsite true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}


## @description  Make sure Maven's eclipse generation works.
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function mvneclipse_postapply
{
  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  big_console_header "Verifying mvn eclipse:eclipse still works"

  verify_needed_test javac
  if [[ $? == 0 ]]; then
    echo "Patch does not touch any java files. Skipping mvn eclipse:eclipse"
    return 0
  fi

  personality_modules patch mvneclipse
  modules_workers patch mvneclipse eclipse:eclipse
  result=$?
  modules_messages patch mvneclipse true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Verify mvn install works
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function maven_precheck_install
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  big_console_header "Verifying mvn install works"

  verify_needed_test javadoc
  retval=$?

  verify_needed_test javac
  ((retval = retval + $? ))
  if [[ ${retval} == 0 ]]; then
    echo "This patch does not appear to need mvn install checks."
    return 0
  fi

  personality_modules branch mvninstall
  modules_workers branch mvninstall -fae clean install -Dmaven.javadoc.skip=true
  result=$?
  modules_messages branch mvninstall true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Verify mvn install works
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function maven_postapply_install
{
  local result=0

  if [[ ${BUILDTOOL} != maven ]]; then
    return 0
  fi

  big_console_header "Verifying mvn install still works"

  verify_needed_test javadoc
  retval=$?

  verify_needed_test javac
  ((retval = retval + $? ))
  if [[ ${retval} == 0 ]]; then
    echo "This patch does not appear to need mvn install checks."
    return 0
  fi

  personality_modules patch mvninstall
  modules_workers patch mvninstall clean install -Dmaven.javadoc.skip=true
  result=$?
  modules_messages patch mvninstall true
  if [[ ${result} != 0 ]]; then
    return 1
  fi
  return 0
}
