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

Hadoop Archive Logs Guide
=========================

 - [Overview](#Overview)
 - [How to Archive Logs](#How_to_Archive_Logs)

Overview
--------

For clusters with a lot of YARN aggregated logs, it can be helpful to combine
them into hadoop archives in order to reduce the number of small files, and
hence the stress on the NameNode.  This tool provides an easy way to do this.
Aggregated logs in hadoop archives can still be read by the Job History Server
and by the `yarn logs` command.

For more on hadoop archives, see
[Hadoop Archives Guide](../hadoop-archives/HadoopArchives.html).

How to Archive Logs
-------------------

    usage: mapred archive-logs
    -force                         Force recreating the working directory if
                                   an existing one is found. This should
                                   only be used if you know that another
                                   instance is not currently running
    -help                          Prints this message
    -maxEligibleApps <n>           The maximum number of eligible apps to
                                   process (default: -1 (all))
    -maxTotalLogsSize <megabytes>  The maximum total logs size (in
                                   megabytes) required to be eligible
                                   (default: 1024)
    -memory <megabytes>            The amount of memory (in megabytes) for
                                   each container (default: 1024)
    -minNumberLogFiles <n>         The minimum number of log files required
                                   to be eligible (default: 20)
    -noProxy                       When specified, all processing will be
                                   done as the user running this command (or
                                   the YARN user if DefaultContainerExecutor
                                   is in use). When not specified, all
                                   processing will be done as the user who
                                   owns that application; if the user
                                   running this command is not allowed to
                                   impersonate that user, it will fail
    -verbose                       Print more details.

The tool only supports running one instance on a cluster at a time in order
to prevent conflicts. It does this by checking for the existance of a
directory named ``archive-logs-work`` under
``yarn.nodemanager.remote-app-log-dir`` in HDFS
(default: ``/tmp/logs/archive-logs-work``). If for some reason that
directory was not cleaned up properly, and the tool refuses to run, you can
force it with the ``-force`` option.

The ``-help`` option prints out the usage information.

The tool works by performing the following procedure:

 1. Determine the list of eligible applications, based on the following
    criteria:
    - is not already archived
    - its aggregation status has successfully completed
    - has at least ``-minNumberLogFiles`` log files
    - the sum of its log files size is less than ``-maxTotalLogsSize`` megabytes
 2. If there are are more than ``-maxEligibleApps`` applications found, the
    newest applications are dropped. They can be processed next time.
 3. A shell script is generated based on the eligible applications
 4. The Distributed Shell program is run with the aformentioned script. It
    will run with ``-maxEligibleApps`` containers, one to process each
    application, and with ``-memory`` megabytes of memory. Each container runs
    the ``hadoop archives`` command for a single application and replaces
    its aggregated log files with the resulting archive.

The ``-noProxy`` option makes the tool process everything as the user who is
currently running it, or the YARN user if DefaultContainerExecutor is in use.
When not specified, all processing will be done by the user who owns that
application; if the user running this command is not allowed to impersonate that
user, it will fail.  This is useful if you want an admin user to handle all
aggregation without enabling impersonation.  With ``-noProxy`` the resulting
HAR files will be owned by whoever ran the tool, instead of whoever originally
owned the logs.

The ``-verbose`` option makes the tool print more details about what it's
doing.

The end result of running the tool is that the original aggregated log files for
a processed application will be replaced by a hadoop archive containing all of
those logs.
