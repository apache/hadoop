<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Docker images for building Hadoop

This folder contains the Dockerfiles for building Hadoop on various platforms.

# Dependency management

The mode of installation of the dependencies needed for building Hadoop varies from one platform to
the other. Different platforms have different toolchains. Some packages tend to be polymorphic
across platforms and most commonly, a package that's readily available in one platform's toolchain
isn't available on another. We thus, resort to building and installing the package from source,
causing duplication of code since this needs to be done for all the Dockerfiles pertaining to all
the platforms. We need a system to track a dependency - for a package - for a platform

- (and optionally) for a release. Thus, there's a lot of diversity that needs to be handled for
  managing package dependencies and
  `pkg-resolver` caters to that.

## Supported platforms

`pkg-resolver/platforms.json` contains a list of the supported platforms for dependency management.

## Package dependencies

`pkg-resolver/packages.json` maps a dependency to a given platform. Here's the schema of this JSON.

```json
{
  "dependency_1": {
    "platform_1": "package_1",
    "platform_2": [
      "package_1",
      "package_2"
    ]
  },
  "dependency_2": {
    "platform_1": [
      "package_1",
      "package_2",
      "package_3"
    ]
  },
  "dependency_3": {
    "platform_1": {
      "release_1": "package_1_1_1",
      "release_2": [
        "package_1_2_1",
        "package_1_2_2"
      ]
    },
    "platform_2": [
      "package_2_1",
      {
        "release_1": "package_2_1_1"
      }
    ]
  }
}
```

The root JSON element contains unique _dependency_ children. This in turn contains the name of the _
platforms_ and the list of _packages_ to be installed for that platform. Just to give an example of
how to interpret the above JSON -

1. For `dependency_1`, `package_1` needs to be installed for `platform_1`.
2. For `dependency_2`, `package_1` and `package_2` needs to be installed for `platform_2`.
3. For `dependency_2`, `package_1`, `package_3` and `package_3` needs to be installed for
   `platform_1`.
4. For `dependency_3`, `package_1_1_1` gets installed only if `release_1` has been specified
   for `platform_1`.
5. For `dependency_3`, the packages `package_1_2_1` and `package_1_2_2` gets installed only
   if `release_2` has been specified for `platform_1`.
6. For `dependency_3`, for `platform_2`, `package_2_1` is always installed, but `package_2_1_1` gets
   installed only if `release_1` has been specified.

### Tool help

```shell
$ pkg-resolver/resolve.py -h
usage: resolve.py [-h] [-r RELEASE] platform

Platform package dependency resolver for building Apache Hadoop

positional arguments:
  platform              The name of the platform to resolve the dependencies for

optional arguments:
  -h, --help            show this help message and exit
  -r RELEASE, --release RELEASE
                        The release label to filter the packages for the given platform
```

## Standalone packages

Most commonly, some packages are not available across the toolchains in various platforms. Thus, we
would need to build and install them. Since we need to do this across all the Dockerfiles for all
the platforms, it could lead to code duplication and managing them becomes a hassle. Thus, we put
the build steps in a `pkg-resolver/install-<package>.sh` and invoke this in all the Dockerfiles.