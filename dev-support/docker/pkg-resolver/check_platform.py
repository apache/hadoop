#!/usr/bin/env python

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

"""
Checks whether the given platform is supported for building Apache Hadoop
"""

import json
import os
import sys


def get_platforms():
    """
    :return: A list of the supported platforms managed by pkg-resolver.
    """

    with open('pkg-resolver/platforms.json', mode='rb') as platforms_file:
        return json.loads(platforms_file.read().decode("UTF-8"))


def is_supported_platform(platform):
    """
    :param platform: The name of the platform
    :return: Whether the platform is supported
    """
    return platform in get_platforms()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.stderr.write('ERROR: Expecting 1 argument, {} were provided{}'.format(
            len(sys.argv) - 1, os.linesep))
        sys.exit(1)

    sys.exit(0 if is_supported_platform(sys.argv[1]) else 1)
