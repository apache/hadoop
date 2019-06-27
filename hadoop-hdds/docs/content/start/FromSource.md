---
title: From Source
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

{{< requirements >}}
 * Java 1.8
 * Maven
 * Protoc (2.5)
{{< /requirements >}}

<div class="alert alert-info" role="alert">This is a guide on how to build the ozone sources.  If you are <font
color="red">not</font>
planning to build sources yourself, you can safely skip this page.</div>

If you are a Hadoop ninja, and wise in the ways of Apache, you already know
that a real Apache release is a source release.

If you want to build from sources, Please untar the source tarball and run
the ozone build command. This instruction assumes that you have all the
dependencies to build Hadoop on your build machine. If you need instructions
on how to build Hadoop, please look at the Apache Hadoop Website.

```bash
mvn -f pom.ozone.xml clean package -DskipTests=true
```

This will build an ozone-\<version\>.tar.gz in your `hadoop-ozone/dist/target` directory.

You can copy this tarball and use this instead of binary artifacts that are
provided along with the official release.

## How to test the build

You can run the acceptance tests in the hadoop-ozone directory to make sure
that  your build is functional. To launch the acceptance tests, please follow
 the instructions in the **README.md** in the `smoketest` directory.

```bash
cd smoketest
./test.sh
```

 You can also execute only a minimal subset of the tests:

```bash
cd smoketest
./test.sh --env ozone basic
```

Acceptance tests will start a small ozone cluster and verify that ozone shell and ozone file
 system is fully functional.
