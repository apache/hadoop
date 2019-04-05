---
title: Ozone CLI
menu:
   main:
      parent: Client
      weight: 1
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

## Understanding Ozone command shell
The most used command when working with Ozone is the Ozone command shell.
Ozone command shell gives a command shell interface to work against
Ozone.

The Ozone shell commands take the following format.

> _ozone sh object action url_

**ozone** script is used to invoke all Ozone sub-commands. The ozone shell is
invoked via ```sh``` command.

The object can be a volume, bucket or a key. The action is various verbs like
 create, list, delete etc.


Ozone URL can point to a volume, bucket or keys in the following format:

_\[scheme\]\[server:port\]/volume/bucket/key_


Where,

1. Scheme - This should be `o3` which is the native RPC protocol to access 
  Ozone API. The usage of the schema is optional.

2. Server:Port - This is the address of the Ozone Manager. This can be server
 only, in that case, the default port is used. If this value is omitted
then the defaults specified in the ozone-site.xml will be used for Ozone
Manager address.

Depending on the call, the volume/bucket/key names will be part of the URL.
Please see volume commands, bucket commands, and key commands section for more
detail.

## Invoking help

Ozone shell help can be invoked at _object_ level or at _action_ level.
For example:

{{< highlight bash >}}
ozone sh volume --help
{{< /highlight >}}

This will show all possible actions for volumes.

or it can be invoked to explain a specific action like
{{< highlight bash >}}
ozone sh volume create --help
{{< /highlight >}}
This command will give you command line options of the create command.
