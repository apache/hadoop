
add_plugin scaladoc


function scaladoc_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.scala$ ]]; then
   yetus_debug "tests/scaladoc: ${filename}"
   add_test scaladoc
  fi
}

## @description  Confirm Javadoc pre-patch
## @audience     private
## @stability    stable
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function scaladoc_preapply
{
  local result=0

  big_console_header "Pre-patch ${PATCH_BRANCH} Javadoc verification"

  verify_needed_test scaladoc
  if [[ $? == 0 ]]; then
     echo "Patch does not appear to need scaladoc tests."
     return 0
  fi

  personality_modules branch scaladoc
  ${BUILDTOOL}_modules_worker branch scaladoc

  ((result=result + $?))
  modules_messages branch scaladoc true


  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}

## @description  Count and compare the number of JavaDoc warnings pre- and post- patch
## @audience     private
## @stability    evolving
## @replaceable  no
## @return       0 on success
## @return       1 on failure
function scaladoc_postinstall
{
  local i
  local result=0
  local fn
  declare -i numbranch=0
  declare -i numpatch=0

  big_console_header "Determining number of patched scaladoc warnings"

  verify_needed_test scaladoc
    if [[ $? == 0 ]]; then
    echo "Patch does not appear to need scaladoc tests."
    return 0
  fi

  personality_modules patch scaladoc
  ${BUILDTOOL}_modules_workers patch scaladoc

  i=0
  until [[ ${i} -eq ${#MODULE[@]} ]]; do
    if [[ ${MODULE_STATUS[${i}]} == -1 ]]; then
      ((result=result+1))
      ((i=i+1))
      continue
    fi

    fn=$(module_file_fragment "${MODULE[${i}]}")
    module_suffix=$(basename "${MODULE[${i}]}")
    if [[ ${module_suffix} == \. ]]; then
      module_suffix=root
    fi

    if [[ -f "${PATCH_DIR}/branch-scaladoc-${fn}.txt" ]]; then
      ${GREP} -i scaladoc "${PATCH_DIR}/branch-scaladoc-${fn}.txt" \
        > "${PATCH_DIR}/branch-scaladoc-${fn}-warning.txt"
    else
      touch "${PATCH_DIR}/branch-scaladoc-${fn}.txt" \
        "${PATCH_DIR}/branch-scaladoc-${fn}-warning.txt"
    fi

    if [[ -f "${PATCH_DIR}/patch-scaladoc-${fn}.txt" ]]; then
      ${GREP} -i scaladoc "${PATCH_DIR}/patch-scaladoc-${fn}.txt" \
        > "${PATCH_DIR}/patch-scaladoc-${fn}-warning.txt"
    else
      touch "${PATCH_DIR}/patch-scaladoc-${fn}.txt" \
        "${PATCH_DIR}/patch-scaladoc-${fn}-warning.txt"
    fi

    numbranch=$(${BUILDTOOL}_count_scaladoc_probs "${PATCH_DIR}/branch-scaladoc-${fn}.txt")
    numpatch=$(${BUILDTOOL}_count_scaladoc_probs "${PATCH_DIR}/patch-scaladoc-${fn}.txt")

    if [[ -n ${numbranch}
        && -n ${numpatch}
        && ${numpatch} -gt ${numbranch} ]] ; then

      ${DIFF} -u "${PATCH_DIR}/branch-scaladoc-${fn}-warning.txt" \
        "${PATCH_DIR}/patch-scaladoc-${fn}-warning.txt" \
        > "${PATCH_DIR}/scaladoc-${fn}-diff.txt"

      module_status ${i} -1  "scaladoc-${fn}-diff.txt" \
        "Patched ${module_suffix} generated "\
        "$((numpatch-numbranch)) additional warning messages."
      ((result=result+1))
    fi
    ((i=i+1))
  done

  modules_messages patch scaladoc true

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi
  return 0
}
