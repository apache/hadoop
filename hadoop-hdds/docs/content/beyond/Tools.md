---
title: "Tools"
date: "2017-10-10"
summary: Ozone supports a set of tools that are handy for developers.Here is a quick list of command line tools.
weight: 3
---
<!---
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

Ozone has a set of command line tools that can be used to manage ozone.

All these commands are invoked via the ```ozone``` script.

The commands supported by ozone are:

   * **classpath** - Prints the class path needed to get the hadoop jar and the
    required libraries.
   * **dtutil**    - Operations related to delegation tokens
   * **fs** - Runs a command on ozone file system.
   * **datanode** - Via daemon command, the HDDS data nodes can be started or
   stopped.
   * **envvars** - Display computed Hadoop environment variables.
   * **freon** -  Runs the ozone load generator.
   * **genesis**  - Developer Only, Ozone micro-benchmark application.
   * **getconf** -  Reads ozone config values from configuration.
   * **jmxget**  - Get JMX exported values from NameNode or DataNode.
   * **om** -   Ozone Manager, via daemon command can be started or stopped.
   * **sh** -  Primary command line interface for ozone.
   * **scm** -  Storage Container Manager service, via daemon can be
   stated or stopped.
   * **scmcli** -  Developer only, Command Line Interface for the Storage
   Container Manager.
   * **version** - Prints the version of Ozone and HDDS.
   * **genconf** -  Generate minimally required ozone configs and output to
   ozone-site.xml.
