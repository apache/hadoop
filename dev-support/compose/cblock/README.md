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


# CBlock dozone configuration

This directory contains example cluster definition for CBlock/jscsi servers.

## How to use

1. First of all Start the servers with `docker-compose up -d`

2. Wait until the servers are up and running (check http://localhost:9876 and wait until you have a healthy node)

3. Create a volume: `docker-compose exec cblock hdfs cblock -c bilbo volume2 1GB 4`

4. Mount the iscsi volume (from host machine):

```
sudo iscsiadm -m node -o new -T bilbo:volume2 -p 127.0.0.1
sudo iscsiadm -m node -T bilbo:volume2 --login
```

5. Check the device name from `dmesg` or `lsblk` (eg /dev/sdd). Errors in dmesg could be ignored: jscsi doesn't implement all the jscsi commands.

6. Format the device (`mkfs.ext4 /dev/sdd`). (Yes, format the while device, not just a partition).

7. Mount it (`mount /dev/sdd /mnt/target`).
