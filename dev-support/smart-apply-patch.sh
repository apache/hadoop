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
grep '^+++\|^---' $PATCH_FILE | cut -c '5-' | grep -v /dev/null | sort | uniq > $TMP

# Assume p0 to start
PLEVEL=0

# if all of the lines start with a/ or b/, then this is a git patch that
# was generated without --no-prefix
if ! grep -qv '^a/\|^b/' $TMP ; then
  echo Looks like this is a git patch. Stripping a/ and b/ prefixes
  echo and incrementing PLEVEL
  PLEVEL=$[$PLEVEL + 1]
  sed -i -e 's,^[ab]/,,' $TMP
fi

PREFIX_DIRS=$(cut -d '/' -f 1 $TMP | sort | uniq)

# if we are at the project root then nothing more to do
if [[ -d hadoop-common ]]; then
  echo Looks like this is being run at project root

# if all of the lines start with hadoop-common/, hdfs/, or mapreduce/, this is
# relative to the hadoop root instead of the subproject root, so we need
# to chop off another layer
elif [[ "$PREFIX_DIRS" =~ ^(hdfs|hadoop-common|mapreduce)$ ]]; then

  echo Looks like this is relative to project root. Increasing PLEVEL
  PLEVEL=$[$PLEVEL + 1]

elif ! echo "$PREFIX_DIRS" | grep -vxq 'hadoop-common\|hdfs\|mapreduce' ; then
  echo Looks like this is a cross-subproject patch. Try applying from the project root
  exit 1
fi

echo Going to apply patch with: $PATCH -p$PLEVEL
$PATCH -p$PLEVEL -E < $PATCH_FILE

cleanup 0
