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

# Hadoop-Qiniu module: Integration with Qiniu Kodo Storage Services

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

See also:

* [User](./user.html)
* [Testing](./test.html)
* [config](./config.html)
* [private-cloud](./private-cloud.html)

* [User zh](./user_zh.html)
* [Testing zh](./test_zh.html)
* [Config zh](./config_zh.html)
* [Private Cloud zh](./private-cloud_zh.html)

## Overview

The `hadoop-qiniu` module provides support for Qiniu integration with
[Qiniu Kodo Storage Service](https://www.qiniu.com/products/kodo).
The generated JAR file, `hadoop-aliyun.jar` also declares a transitive
dependency on all external artifacts which are needed for this support — enabling
downstream applications to easily use this support.

To make it part of Apache Hadoop's default classpath, simply make sure
that HADOOP_OPTIONAL_TOOLS in hadoop-env.sh has 'hadoop-qiniu' in the list.
