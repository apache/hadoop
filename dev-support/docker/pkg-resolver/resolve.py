#!/usr/bin/env python3

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
Platform package dependency resolver for building Apache Hadoop.
"""

import json
import sys
from check_platform import is_supported_platform


def get_packages(platform):
    """
    Resolve and get the list of packages to install for the given platform.

    :param platform: The platform for which the packages needs to be resolved.
    :return: A list of resolved packages to install.
    """
    with open('pkg-resolver/packages.json', encoding='utf-8', mode='r') as pkg_file:
        pkgs = json.loads(pkg_file.read())
    packages = []
    for platforms in filter(lambda x: x.get(platform) is not None, pkgs.values()):
        if isinstance(platforms.get(platform), list):
            packages.extend(platforms.get(platform))
        else:
            packages.append(platforms.get(platform))
    return packages


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('ERROR: Need at least 1 argument, {} were provided'.format(len(sys.argv) - 1),
              file=sys.stderr)
        sys.exit(1)

    platform_arg = sys.argv[1]
    if not is_supported_platform(platform_arg):
        print(
            'ERROR: The given platform {} is not supported. '
            'Please refer to platforms.json for a list of supported platforms'.format(
                platform_arg), file=sys.stderr)
        sys.exit(1)

    packages_to_install = get_packages(platform_arg)
    print(' '.join(packages_to_install))
