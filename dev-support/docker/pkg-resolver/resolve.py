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

import argparse
import json
import sys
from check_platform import is_supported_platform


def get_packages(platform, release=None):
    """
    Resolve and get the list of packages to install for the given platform.

    :param platform: The platform for which the packages needs to be resolved.
    :param release: An optional parameter that filters the packages of the given platform for the
    specified release.
    :return: A list of resolved packages to install.
    """
    with open('pkg-resolver/packages.json', encoding='utf-8', mode='r') as pkg_file:
        pkgs = json.loads(pkg_file.read())
    packages = []

    def process_package(package, in_release=False):
        """
        Processes the given package object that belongs to a platform and adds it to the packages
        list variable in the parent scope.
        In essence, this method recursively traverses the JSON structure defined in packages.json
        and performs the core filtering.

        :param package: The package object to process.
        :param in_release: A boolean that indicates whether the current travels belongs to a package
        that needs to be filtered for the given release label.
        """
        if isinstance(package, list):
            for entry in package:
                process_package(entry, in_release)
        elif isinstance(package, dict):
            if release is None:
                return
            for entry in package.get(release, []):
                process_package(entry, in_release=True)
        elif isinstance(package, str):
            # Filter out the package that doesn't belong to this release,
            # if a release label has been specified.
            if release is not None and not in_release:
                return
            packages.append(package)
        else:
            raise Exception('Unknown package of type: {}'.format(type(package)))

    for platforms in filter(lambda x: x.get(platform) is not None, pkgs.values()):
        process_package(platforms.get(platform))
    return packages


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('ERROR: Need at least 1 argument, {} were provided'.format(len(sys.argv) - 1),
              file=sys.stderr)
        sys.exit(1)

    arg_parser = argparse.ArgumentParser(
        description='Platform package dependency resolver for building Apache Hadoop')
    arg_parser.add_argument('-r', '--release', nargs=1, type=str,
                            help='The release label to filter the packages for the given platform')
    arg_parser.add_argument('platform', nargs=1, type=str,
                            help='The name of the platform to resolve the dependencies for')
    args = arg_parser.parse_args()

    if not is_supported_platform(args.platform[0]):
        print(
            'ERROR: The given platform {} is not supported. '
            'Please refer to platforms.json for a list of supported platforms'.format(
                args.platform), file=sys.stderr)
        sys.exit(1)

    packages_to_install = get_packages(args.platform[0],
                                       args.release[0] if args.release is not None else None)
    print(' '.join(packages_to_install))
