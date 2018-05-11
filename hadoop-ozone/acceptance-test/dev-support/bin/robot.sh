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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#basedir is the directory of the whole hadoop project. Used to calculate the
#exact path to the hadoop-dist project
BASEDIR=${DIR}/../../../..

if [ ! "$(which robot)" ] ; then
    echo ""
    echo "robot is not on your PATH."
    echo ""
    echo "Please install it according to the documentation:"
    echo "    http://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html#installation-instructions"
    echo "    (TLDR; most of the time you need: 'pip install robotframework')"
    exit -1
fi

OZONEDISTDIR="$BASEDIR/hadoop-dist/target/ozone"
if [ ! -d "$OZONEDISTDIR" ]; then
   echo "Ozone can't be found in the $OZONEDISTDIR."
   echo "You may need a full build with -Phdds and -Pdist profiles"
   exit -1
fi
robot -v basedir:$BASEDIR $@
