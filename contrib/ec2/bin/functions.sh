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

function getCredentialSetting {
  name=$1
  eval "val=\$$name"
  if [ -z "$val" ] ; then
    if [ -f "$bin"/credentials.sh ] ; then
      val=`cat "$bin"/credentials.sh | grep $name | awk 'BEGIN { FS="=" } { print $2; }'`
      if [ -z "$val" ] ; then
        echo -n "$name: "
        read -e val
        echo "$name=$val" >> "$bin"/credentials.sh
      fi
    else
      echo -n "$name: "
      read -e val
      echo "$name=$val" >> "$bin"/credentials.sh
    fi
    eval "$name=$val"
  fi
}
