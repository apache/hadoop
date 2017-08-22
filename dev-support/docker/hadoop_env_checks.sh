#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -------------------------------------------------------
function showWelcome {
cat <<Welcome-message

 _   _           _                    ______
| | | |         | |                   |  _  \\
| |_| | __ _  __| | ___   ___  _ __   | | | |_____   __
|  _  |/ _\` |/ _\` |/ _ \\ / _ \\| '_ \\  | | | / _ \\ \\ / /
| | | | (_| | (_| | (_) | (_) | |_) | | |/ /  __/\\ V /
\\_| |_/\\__,_|\\__,_|\\___/ \\___/| .__/  |___/ \\___| \\_(_)
                              | |
                              |_|

This is the standard Hadoop Developer build environment.
This has all the right tools installed required to build
Hadoop from source.

Welcome-message
}

# -------------------------------------------------------

function showAbort {
  cat <<Abort-message

  ___  _                _   _
 / _ \\| |              | | (_)
/ /_\\ \\ |__   ___  _ __| |_ _ _ __   __ _
|  _  | '_ \\ / _ \\| '__| __| | '_ \\ / _\` |
| | | | |_) | (_) | |  | |_| | | | | (_| |
\\_| |_/_.__/ \\___/|_|   \\__|_|_| |_|\\__, |
                                     __/ |
                                    |___/

Abort-message
}

# -------------------------------------------------------

function failIfUserIsRoot {
    if [ "$(id -u)" -eq "0" ]; # If you are root then something went wrong.
    then
        cat <<End-of-message

Apparently you are inside this docker container as the user root.
Putting it simply:

   This should not occur.

Known possible causes of this are:
1) Running this script as the root user ( Just don't )
2) Running an old docker version ( upgrade to 1.4.1 or higher )

End-of-message

    showAbort

    logout

    fi
}

# -------------------------------------------------------

# Configurable low water mark in GiB
MINIMAL_MEMORY_GiB=2

function warnIfLowMemory {
    MINIMAL_MEMORY=$((MINIMAL_MEMORY_GiB*1024*1024)) # Convert to KiB
    INSTALLED_MEMORY=$(fgrep MemTotal /proc/meminfo | awk '{print $2}')
    if [ $((INSTALLED_MEMORY)) -le $((MINIMAL_MEMORY)) ];
    then
        cat <<End-of-message

 _                    ___  ___
| |                   |  \\/  |
| |     _____      __ | .  . | ___ _ __ ___   ___  _ __ _   _
| |    / _ \\ \\ /\\ / / | |\\/| |/ _ \\ '_ \` _ \\ / _ \\| '__| | | |
| |___| (_) \\ V  V /  | |  | |  __/ | | | | | (_) | |  | |_| |
\\_____/\\___/ \\_/\\_/   \\_|  |_/\\___|_| |_| |_|\\___/|_|   \\__, |
                                                         __/ |
                                                        |___/

Your system is running on very little memory.
This means it may work but it wil most likely be slower than needed.

If you are running this via boot2docker you can simply increase
the available memory to atleast ${MINIMAL_MEMORY_GiB} GiB (you have $((INSTALLED_MEMORY/(1024*1024))) GiB )

End-of-message
    fi
}

# -------------------------------------------------------

showWelcome
warnIfLowMemory
failIfUserIsRoot

# -------------------------------------------------------
