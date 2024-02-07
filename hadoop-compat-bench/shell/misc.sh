#!/bin/sh

ntest=1
fname="$0"

prepare() {
    BASE_URI="${HADOOP_COMPAT_BASE_URI}"
    LOCAL_URI="${HADOOP_COMPAT_LOCAL_URI}"
    SNAPSHOT_URI="${HADOOP_COMPAT_SNAPSHOT_URI}"
    STORAGE_POLICY="${HADOOP_COMPAT_STORAGE_POLICY}"
    STDOUT_DIR="${HADOOP_COMPAT_STDOUT_DIR}"
    PASS_FILE="${HADOOP_COMPAT_PASS_FILE}"
    FAIL_FILE="${HADOOP_COMPAT_FAIL_FILE}"
    SKIP_FILE="${HADOOP_COMPAT_SKIP_FILE}"

    baseDir="${BASE_URI}/${fname}"
    localDir="${LOCAL_URI}/${fname}"
    snapshotDir="${SNAPSHOT_URI}"
    storagePolicy="${STORAGE_POLICY}"
    stdoutDir="${STDOUT_DIR}/${fname}/stdout"
    stderrDir="${STDOUT_DIR}/${fname}/stderr"
    mkdir -p "${stdoutDir}"
    mkdir -p "${stderrDir}"
    mkdir -p "${localDir}"
    hadoop fs -mkdir -p "${baseDir}"
}

expect_ret() { (
    cname="${1}"
    shift
    expect="${1}"
    shift

    stdout="${stdoutDir}/${ntest}"
    stderr="${stderrDir}/${ntest}"
    "$@" 1>"${stdout}" 2>"${stderr}"
    result="$?"

    if should_skip "${stderr}"; then
        skip_case "${cname}"
    else
        if [ X"${result}" = X"${expect}" ]; then
            pass_case "${cname}"
        else
            fail_case "${cname}"
        fi
    fi
)
    ntest=$((ntest + 1))
}

expect_out() { (
    cname="${1}"
    shift
    expect="${1}"
    shift

    stdout="${stdoutDir}/${ntest}"
    stderr="${stderrDir}/${ntest}"
    "$@" 1>"${stdout}" 2>"${stderr}"

    if should_skip "${stderr}"; then
        skip_case "${cname}"
    else
        if grep -Eq '^'"${expect}"'$' "${stdout}"; then
            pass_case "${cname}"
        else
            fail_case "${cname}"
        fi
    fi
)
    ntest=$((ntest + 1))
}

expect_lines() { (
    cname="${1}"
    shift
    lineNum="${1}"
    shift
    lines=$(expect_lines_parse "${lineNum}" "$@")
    shift "${lineNum}"

    stdout="${stdoutDir}/${ntest}"
    stderr="${stderrDir}/${ntest}"
    "$@" 1>"${stdout}" 2>"${stderr}"

    if should_skip "${stderr}"; then
        skip_case "${cname}"
    else
        set -- ${lines}
        lineCount="0"
        while read -r line; do
            case "${line}" in
                *"Found"*"items"*)
                    continue
                    ;;
            esac
            if ! echo "${line}" | grep -Eq '^'"${1}"'$'; then
                lineCount="-1"
                break
            else
                lineCount=$((lineCount + 1))
                shift
            fi
        done <"${stdout}"
        if [ "${lineCount}" -eq "${lineNum}" ]; then
            pass_case "${cname}"
        else
            fail_case "${cname}"
        fi
    fi
)
    ntest=$((ntest + 1))
}

expect_lines_parse() {
    for _ in $(seq 1 "${1}"); do
        shift
        echo "${1}"
    done
}

is_hadoop_shell() {
    if [ X"${1}" = X"hadoop" ] || [ X"${1}" = X"hdfs" ]; then
        return 0
    else
        return 1
    fi
}

should_skip() {
    if grep -q "Unknown command" "${1}" || grep -q "Illegal option" "${1}"; then
        return 0
    else
        return 1
    fi
}

pass_case() {
    echo "ok ${ntest}"
    echo "${fname} - #${ntest} ${1}" >> "${PASS_FILE}"
}

fail_case() {
    echo "not ok ${ntest}"
    echo "${fname} - #${ntest} ${1}" >> "${FAIL_FILE}"
}

skip_case() {
    echo "ok ${ntest}"
    echo "${fname} - #${ntest} ${1}" >> "${SKIP_FILE}"
}

prepare
