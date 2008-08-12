#!/bin/bash
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

# A script to tell chukwa to monitor job history files.
# Rather simpleminded -- gets a list of what's being monitored,
# and tells chukwa to watch everything in job hist that it isn't already scanning.
#   Relies on having netcat. Also, control socket portno is currently hardcoded,
#   as are hostname and adaptor name.

if [ $# -lt 1 ]; then
         echo 1>&2 Usage: $0 '<path to job history files>'
         exit 127
    fi


JOB_HIST=`(cd $1; pwd)`  #returns an absolute path
echo "assuming job history logs live in $JOB_HIST"
JOBHISTFILES=/tmp/jobhistfiles
TAILEDFILES=/tmp/tailedhists

#Step 1 -- get a list of currently watched files
(nc localhost 9093 | grep -o "[^/]*$" | grep -o '^[^ ]*' | sort > $TAILEDFILES)  <<HERE
list
close
HERE

#step 2 -- get the list of history files
ls $JOB_HIST | grep -v '\.xml' | sort  > $JOBHISTFILES
#step 3 -- start watching each new history file
#find files that aren't being watched, and are in job history dir
#NEWHISTFILES=`cat $JOBHISTFILES`
#NEWHISTFILES=`sort /tmp/both | uniq -u > /tmp/one`| uniq -d - $JOBHISTFILES`

cat $JOBHISTFILES $TAILEDFILES | sort | uniq -u > /tmp/either  
#either not tailed, or not a history file
NEWHISTFILES=`cat /tmp/either $JOBHISTFILES | sort | uniq -d`
#better be a job history file -- hence, not being tailed

for job in $NEWHISTFILES ; do
	#new jobs are rare, safe to create socket per job hist file
nc localhost 9093 <<HERE
add LineFileTailUTF8 $JOB_HIST$job 0
close
HERE
  echo "told Chukwa agent to start watching $job"
done
