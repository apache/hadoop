
function ant_buildfile
{
  echo "build.xml"
}

function ant_executor
{
  echo "${ANT}" "${ANT_ARGS[@]}"
}

function ant_modules_worker
{
  declare branch=$1
  declare tst=$2
  shift 2

  case ${tst} in
    javac)
      modules_workers ${branch} javac
    ;;
    javadoc)
      modules_workers ${branch} javadoc clean javadoc
    ;;
    unit)
      modules_workers ${branch} unit
    ;;
    *)
      yetus_error "WARNING: ${tst} is unsupported by ${BUILDTOOL}"
      return 1
    ;;
  esac
}

function ant_count_javac_probs
{
  declare warningfile=$1
  declare val1
  declare val2

  #shellcheck disable=SC2016
  val1=$(${GREP} -E "\[javac\] [0-9]+ errors?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
  #shellcheck disable=SC2016
  val2=$(${GREP} -E "\[javac\] [0-9]+ warnings?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
  echo $((val1+val2))
}

## @description  Helper for check_patch_javadoc
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function ant_count_javadoc_probs
{
  local warningfile=$1
  local val1
  local val2

  #shellcheck disable=SC2016
  val1=$(${GREP} -E "\[javadoc\] [0-9]+ errors?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
  #shellcheck disable=SC2016
  val2=$(${GREP} -E "\[javadoc\] [0-9]+ warnings?$" "${warningfile}" | ${AWK} '{sum+=$2} END {print sum}')
  echo $((val1+val2))
}