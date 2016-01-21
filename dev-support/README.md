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

This directory contains tools to help in the development and release of Apache Hadoop.

* bin

  * releasedocmaker

    Build release notes for a given Hadoop project or subproject.  This is called from Maven when -Preleasedocs is used.  See BUILDING.txt for more information.

  * shelldocs

    Create documentation for the Unix Shell API.  This is called from Maven when -Pshelldocs is used.

  * smart-apply-patch

    Intelligently apply a patch file to a source tree.

  * test-patch

    Test a patch against a source tree.

* create-release.sh

  Helps REs create a release of Apache Hadoop for distribution.

* determine-flaky-tests-hadoop.py

  Given a jenkins test job, this script examines all runs of the job done within specified period of time (number of days prior to the execution time of this script), and reports all failed tests.

* docker

  Various helpers for the start-build-env.sh script, including the Dockerfile itself. See parent BUILDING.txt for more information.

* findHangingTest.sh

  Finds hanging test from Jenkins build output.


Previously, the scripts test-patch.sh, smart-apply-patch.sh, releasedocmaker.py, and shelldocs.py were in this directory.  They have been moved to the Apache Yetus project (https://yetus.apache.org).  These scripts have been replaced with wrapper scripts located in the bin directory. Command line options are generally different than the previous versions that shipped with older versions of Apache Hadoop.

The wrapper scripts will download, verify (if GPG is installed), and cache a local copy of Apache Yetus in the hadoop/patchprocess directory. The version that is used may be overridden by setting the HADOOP\_YETUS\_VERSION environment variable.  The cache directory may be overwritten by setting the HADOOP\_PATCHPROCESS directory.  If a local version of Apache Yetus is already installed, it may be used instead by setting the YETUS\_HOME environment variable to point to that directory.