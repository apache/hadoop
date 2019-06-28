---
title: Shell Overview
summary: Explains the command syntax used by shell command.
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

</p>


### General Command Format

The Ozone shell commands take the following format.

> _ozone sh object action url_

**ozone** script is used to invoke all Ozone sub-commands. The ozone shell is
invoked via ```sh``` command.

The object can be a volume, bucket or a key. The action is various verbs like
create, list, delete etc.


Ozone URL can point to a volume, bucket or keys in the following format:

_\[scheme\]\[server:port\]/volume/bucket/key_


Where,

1. **Scheme** - This should be `o3` which is the native RPC protocol to access
  Ozone API. The usage of the schema is optional.

2. **Server:Port** - This is the address of the Ozone Manager. If the port is
omitted the default port from ozone-site.xml will be used.

Depending on the call, the volume/bucket/key names will be part of the URL.
Please see volume commands, bucket commands, and key commands section for more
detail.
