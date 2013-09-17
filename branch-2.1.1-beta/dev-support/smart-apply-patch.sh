#!/usr/bin/env bash
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

set -e

PATCH_FILE=$1
if [ -z "$PATCH_FILE" ]; then
  echo usage: $0 patch-file
  exit 1
fi

PATCH=${PATCH:-patch} # allow overriding patch binary

# Cleanup handler for temporary files
TOCLEAN=""
cleanup() {
  rm $TOCLEAN
  exit $1
}
trap "cleanup 1" HUP INT QUIT TERM

# Allow passing "-" for stdin patches
if [ "$PATCH_FILE" == "-" ]; then
  PATCH_FILE=/tmp/tmp.in.$$
  cat /dev/fd/0 > $PATCH_FILE
  TOCLEAN="$TOCLEAN $PATCH_FILE"
fi

# Come up with a list of changed files into $TMP
TMP=/tmp/tmp.paths.$$
TOCLEAN="$TOCLEAN $TMP"

if $PATCH -p0 -E --dry-run < $PATCH_FILE 2>&1 > $TMP; then
  PLEVEL=0
  #if the patch applied at P0 there is the possability that all we are doing
  # is adding new files and they would apply anywhere. So try to guess the
  # correct place to put those files.

  TMP2=/tmp/tmp.paths.2.$$
  TOCLEAN="$TOCLEAN $TMP2"

  egrep '^patching file |^checking file ' $TMP | awk '{print $3}' | grep -v /dev/null | sort | uniq > $TMP2

  if [ ! -s $TMP2 ]; then
    echo "Error: Patch dryrun couldn't detect changes the patch would make. Exiting."
    cleanup 1
  fi

  #first off check that all of the files do not exist
  FOUND_ANY=0
  for CHECK_FILE in $(cat $TMP2)
  do
    if [[ -f $CHECK_FILE ]]; then
      FOUND_ANY=1
    fi
  done

  if [[ "$FOUND_ANY" = "0" ]]; then
    #all of the files are new files so we have to guess where the correct place to put it is.

    # if all of the lines start with a/ or b/, then this is a git patch that
    # was generated without --no-prefix
    if ! grep -qv '^a/\|^b/' $TMP2 ; then
      echo Looks like this is a git patch. Stripping a/ and b/ prefixes
      echo and incrementing PLEVEL
      PLEVEL=$[$PLEVEL + 1]
      sed -i -e 's,^[ab]/,,' $TMP2
    fi

    PREFIX_DIRS_AND_FILES=$(cut -d '/' -f 1 | sort | uniq)

    # if we are at the project root then nothing more to do
    if [[ -d hadoop-common-project ]]; then
      echo Looks like this is being run at project root

    # if all of the lines start with hadoop-common/, hadoop-hdfs/, hadoop-yarn/ or hadoop-mapreduce/, this is
    # relative to the hadoop root instead of the subproject root, so we need
    # to chop off another layer
    elif [[ "$PREFIX_DIRS_AND_FILES" =~ ^(hadoop-common-project|hadoop-hdfs-project|hadoop-yarn-project|hadoop-mapreduce-project)$ ]]; then

      echo Looks like this is relative to project root. Increasing PLEVEL
      PLEVEL=$[$PLEVEL + 1]

    elif ! echo "$PREFIX_DIRS_AND_FILES" | grep -vxq 'hadoop-common-project\|hadoop-hdfs-project\|hadoop-yarn-project\|hadoop-mapreduce-project' ; then
      echo Looks like this is a cross-subproject patch. Try applying from the project root
      cleanup 1
    fi
  fi
elif $PATCH -p1 -E --dry-run < $PATCH_FILE 2>&1 > /dev/null; then
  PLEVEL=1
elif $PATCH -p2 -E --dry-run < $PATCH_FILE 2>&1 > /dev/null; then
  PLEVEL=2
else
  echo "The patch does not appear to apply with p0 to p2";
  cleanup 1;
fi

echo Going to apply patch with: $PATCH -p$PLEVEL
$PATCH -p$PLEVEL -E < $PATCH_FILE

cleanup $?
