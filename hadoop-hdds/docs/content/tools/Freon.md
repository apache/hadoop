---
title: Freon
date: "2017-09-02T23:58:17-07:00"

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

Overview
--------

Freon is a load-generator for Ozone. This tool is used for testing the functionality of ozone.

### Random keys

In randomkeys mode, the data written into ozone cluster is randomly generated.
Each key will be of size 10 KB.

The number of volumes/buckets/keys can be configured. The replication type and
factor (eg. replicate with ratis to 3 nodes) Also can be configured.

For more information use

`bin/ozone freon --help`

### Example

{{< highlight bash >}}
ozone freon randomkeys --numOfVolumes=10 --numOfBuckets 10 --numOfKeys 10  --replicationType=RATIS --factor=THREE
{{< /highlight >}}

{{< highlight bash >}}
***************************************************
Status: Success
Git Base Revision: 48aae081e5afacbb3240657556b26c29e61830c3
Number of Volumes created: 10
Number of Buckets created: 100
Number of Keys added: 1000
Ratis replication factor: THREE
Ratis replication type: RATIS
Average Time spent in volume creation: 00:00:00,035
Average Time spent in bucket creation: 00:00:00,319
Average Time spent in key creation: 00:00:03,659
Average Time spent in key write: 00:00:10,894
Total bytes written: 10240000
Total Execution time: 00:00:16,898
***********************
{{< /highlight >}}
