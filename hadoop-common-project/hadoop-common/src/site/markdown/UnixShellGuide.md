<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Unix Shell Guide

Much of Hadoop's functionality is controlled via [the shell](CommandsManual.html).  There are several ways to modify the default behavior of how these commands execute.

## Important End-User Environment Variables

Hadoop has many environment variables that control various aspects of the software.  (See `hadoop-env.sh` and related files.)  Some of these environment variables are dedicated to helping end users manage their runtime.

### `HADOOP_CLIENT_OPTS`

This environment variable is used for almost all end-user operations.  It can be used to set any Java options as well as any Hadoop options via a system property definition. For example:

```bash
HADOOP_CLIENT_OPTS="-Xmx1g -Dhadoop.socks.server=localhost:4000" hadoop fs -ls /tmp
```

will increase the memory and send this command via a SOCKS proxy server.

### `HADOOP_USER_CLASSPATH`

The Hadoop scripts have the capability to inject more content into the classpath of the running command by setting this environment variable.  It should be a colon delimited list of directories, files, or wildcard locations.

```bash
HADOOP_USER_CLASSPATH=${HOME}/lib/myjars/*.jar hadoop classpath
```

A user can provides hints to the location of the paths via the `HADOOP_USER_CLASSPATH_FIRST` variable.  Setting this to any value will tell the system to try and push these paths near the front.

### Auto-setting of Variables

If a user has a common set of settings, they can be put into the `${HOME}/.hadooprc` file.  This file is always read to initialize and override any variables that the user may want to customize.  It uses bash syntax, similar to the `.bashrc` file:

For example:

```bash
#
# my custom Hadoop settings!
#

HADOOP_USER_CLASSPATH=${HOME}/hadoopjars/*
HADOOP_USER_CLASSPATH_FIRST=yes
HADOOP_CLIENT_OPTS="-Xmx1g"
```

The `.hadooprc` file can also be used to extend functionality and teach Hadoop new tricks.  For example, to run hadoop commands accessing the server referenced in the environment variable `${HADOOP_SERVER}`, the following in the `.hadooprc` will do just that:

```bash

if [[ -n ${HADOOP_SERVER} ]]; then
  HADOOP_CONF_DIR=/etc/hadoop.${HADOOP_SERVER}
fi
```

## Administrator Environment

There are many environment variables that impact how the system operates.  By far, the most important are the series of `_OPTS` variables that control how daemons work.  These variables should contain all of the relevant settings for those daemons.

More, detailed information is contained in `hadoop-env.sh` and the other env.sh files.

Advanced administrators may wish to supplement or do some platform-specific fixes to the existing scripts.  In some systems, this means copying the errant script or creating a custom build with these changes.  Hadoop provides the capabilities to do function overrides so that the existing code base may be changed in place without all of that work.  Replacing functions is covered later under the Shell API documentation.

## Developer and Advanced Administrator Environment

### Shell Profiles

Apache Hadoop allows for third parties to easily add new features through a variety of pluggable interfaces.  This includes a shell code subsystem that makes it easy to inject the necessary content into the base installation.

Core to this functionality is the concept of a shell profile.  Shell profiles are shell snippets that can do things such as add jars to the classpath, configure Java system properties and more.

Shell profiles may be installed in either `${HADOOP_CONF_DIR}/shellprofile.d` or `${HADOOP_PREFIX}/libexec/shellprofile.d`.  Shell profiles in the `libexec` directory are part of the base installation and cannot be overriden by the user.  Shell profiles in the configuration directory may be ignored if the end user changes the configuration directory at runtime.

An example of a shell profile is in the libexec directory.

## Shell API

Hadoop's shell code has a [function library](./UnixShellAPI.html) that is open for administrators and developers to use to assist in their configuration and advanced feature management.  These APIs follow the standard [Hadoop Interface Classification](./InterfaceClassification.html), with one addition: Replaceable.

The shell code allows for core functions to be overridden. However, not all functions can be or are safe to be replaced.  If a function is not safe to replace, it will have an attribute of Replaceable: No.  If a function is safe to replace, it will have the attribute of Replaceable: Yes.

In order to replace a function, create a file called `hadoop-user-functions.sh` in the `${HADOOP_CONF_DIR}` directory.  Simply define the new, replacement function in this file and the system will pick it up automatically.  There may be as many replacement functions as needed in this file.  Examples of function replacement are in the `hadoop-user-functions.sh.examples` file.


Functions that are marked Public and Stable are safe to use in shell profiles as-is.  Other functions may change in a minor release.

