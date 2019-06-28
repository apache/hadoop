---
title: Overview
menu: main
weight: -10
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

# Apache Hadoop Ozone

<img src="ozone-usage.png" style="max-width: 60%;"/>

*_Ozone is a scalable, redundant, and distributed object store for Hadoop. <p>
Apart from scaling to billions of objects of varying sizes,
Ozone can function effectively in containerized environments
like Kubernetes._* <p>

Applications like Apache Spark, Hive and YARN, work without any modifications when using Ozone. Ozone comes with a [Java client library]({{<
ref "JavaApi.md"
>}}), [S3 protocol support] ({{< ref "S3.md" >}}), and a [command line interface]
({{< ref "shell/_index.md" >}})  which makes it easy to use Ozone.

Ozone consists of volumes, buckets, and keys:

* Volumes are similar to user accounts. Only administrators can create or delete volumes.
* Buckets are similar to directories. A bucket can contain any number of keys, but buckets cannot contain other buckets.
* Keys are similar to files.

 <a href="{{< ref "start/_index.md" >}}"> <button type="button"
 class="btn  btn-success btn-lg">Next >></button>
</div>
