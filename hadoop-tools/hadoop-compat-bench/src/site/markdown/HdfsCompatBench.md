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

# Compatibility Benchmark over HCFS Implementations

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

## <a name="Overview"></a> Overview

Hadoop-Compatible File System (HCFS) is a core conception in big data storage ecosystem,
providing unified interfaces and generally clear semantics,
and has become the de-factor standard for industry storage systems to follow and conform with.
There have been a series of HCFS implementations in Hadoop,
such as S3AFileSystem for Amazon's S3 Object Store,
WASB for Microsoft's Azure Blob Storage, OSS connector for Alibaba Cloud Object Storage,
and more from storage service's providers on their own.

Meanwhile, Hadoop is also developing and new features are continuously contributing to HCFS interfaces
for existing implementations to follow and update.
However, we need a tool to check whether the features are supported by a specific implementation.

This module defines an HCFS compatibility benchmark and provides a corresponding tool
to do the compatibility assessment for an HCFS storage system.
The tool is a jar file which is executable by `hadoop jar`,
after which an HCFS compatibility report is generated showing an overall score,
and a detailed list of passed and failed cases (optional).

## <a name="Prepare"></a> Prepare

First of all, there must be a properly installed Hadoop environment to run `hadoop jar` command.
See [HdfsUserGuide](./HdfsUserGuide.html) for more information about how to set up a Hadoop environment.
Then, two things should be done before a quick benchmark assessment.

#### FileSystem implementation

There must be a Java FileSystem implementation.
The FS is known to Hadoop by config key `fs.<scheme>impl`. `org.apache.hadoop.fs.s3a.S3AFileSystem`
is an example, while for implementations that has not been directly supported by Hadoop community,
an extra jar file is needed.

The jar file should be placed at the last part of hadoop classpath.
A common practice is to modify `hadoop-env.sh` to append an extra classpath like:
```shell
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/hadoop/extra/classpath/*
```
Then we are able to place extra jar files to `/hadoop/extra/classpath/`

#### Optional configuration

Some FS APIs may need additional information to run.
Additional information can be passed via optional configurations. There is an example:
```xml
<property>
  <name>fs.{scheme}.compatibility.storage.policies</name>
  <value>Hot,WARM,COLD</value>
  <description>
    Storage policy names used by HCFS compatibility benchmark tool.
    The config key is fs.{scheme}.compatibility.storage.policies.
    The config value is Comma-separated storage policy names for the FS.
  </description>
</property>
```
Optional configurations are defined in `org.apache.hadoop.fs.compat.common.HdfsCompatEnvironment`.

## Usage

```shell
hadoop jar hadoop-compat-bench.jar -uri <uri> [-suite <suite-name>] [-output <output-file>]
```
This command generates a report with text format, showing an overall score
and optionally passed and failed case lists.

#### uri

`uri` points to the target storage service path that you'd like to evaluate.
Some new files or directories would be created under the path.

#### suite

`suite-name` corresponds to a subset of all test cases.
For example, a suite with name 'shell' contains only shell command cases.
There are three default suites of the tool:
* ALL: run all test cases of the benchmark. This is the default suite if `-suite` is absent.
* Shell: run only shell command cases.
* TPCDS: run cases for APIs that a TPC-DS program may require.

#### output

`output-file` points to a local file to save a detailed compatibility report document.
The detailed report contains not only an overall score but also passed and failed case lists.
