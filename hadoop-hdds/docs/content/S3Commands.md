---
title: S3 Commands
menu:
   main:
      parent: Client
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

Ozone shell supports the following S3 commands.

  * [getsecret](#get secret)

### Get Secret

User should get the kerberos ticket before using this option.


{{< highlight bash >}}
ozone s3 getsecret
{{< /highlight >}}
Prints the AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID for the current user.


You can try out these commands from the docker instance of the [Alpha
Cluster](runningviadocker.html).
