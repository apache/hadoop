#!/usr/bin/env python3

import json
import sys


def get_packages(platform):
    with open('pkg-resolver/packages.json', encoding='utf-8', mode='r') as pkg_file:
        pkgs = json.loads(pkg_file.read())
    packages = []
    for platforms in pkgs.values():
        if platforms.get(platform) is None:
            continue
        else:
            if type(platforms.get(platform)) == list:
                for p in platforms.get(platform):
                    packages.append(p)
            else:
                packages.append(platforms.get(platform))
    return packages


def get_platforms():
    with open('pkg-resolver/platforms.json', encoding='utf-8', mode='r') as platforms_file:
        return json.loads(platforms_file.read())


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('ERROR: Need at least 1 argument, {} were provided'.format(len(sys.argv) - 1), file=sys.stderr)
        sys.exit(1)

    supported_platforms = get_platforms()
    platform = sys.argv[1]
    if platform not in supported_platforms:
        print('ERROR: The given platform {} is not supported. '
              'Please refer to platforms.json for a list of supported platforms'.format(platform), file=sys.stderr)
        sys.exit(1)

    packages = get_packages(platform)
    print(' '.join(packages))
