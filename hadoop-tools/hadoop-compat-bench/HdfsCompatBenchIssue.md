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

## <a name="Background"></a> Background

Hadoop-Compatible File System (HCFS) is a core conception in big data storage ecosystem,
providing unified interfaces and generally clear semantics,
and has become the de-factor standard for industry storage systems to follow and conform with.
There have been a series of HCFS implementations in Hadoop,
such as S3AFileSystem for Amazon's S3 Object Store,
WASB for Microsoft's Azure Blob Storage and OSS connector for Alibaba Cloud Object Storage,
and more from storage service's providers on their own.

## <a name="Problems"></a> Problems

However, as indicated by [`HCFS Introduction`](hadoop-common-project/hadoop-common/src/site/markdown/filesystem/introduction.md),
there is no formal suite to do compatibility assessment of a file system for all such HCFS implementations.
Thus, whether the functionality is well accomplished and meets the core compatible expectations
mainly relies on service provider's own report.
Meanwhile, Hadoop is also developing and new features are continuously contributing to HCFS interfaces
for existing implementations to follow and update, in which case,
Hadoop also needs a tool to quickly assess if these features are supported or not for a specific HCFS implementation.
Besides, the known hadoop command line tool or hdfs shell is used to directly interact with a HCFS storage system,
where most commands correspond to specific HCFS interfaces and work well.
Still, there are cases that are complicated and may not work, like expunge command.
To check such commands for an HCFS, we also need an approach to figure them out.

## <a name="Proposal"></a> Proposal

Accordingly, we propose to define a formal HCFS compatibility benchmark and provide corresponding tool
to do the compatibility assessment for an HCFS storage system.
The benchmark and tool should consider both HCFS interfaces and hdfs shell commands.
Different scenarios require different kinds of compatibilities.
For such consideration, we could define different suites in the benchmark.

## <a name="Benefits"></a> Benefits

We intend the benchmark and tool to be useful for both storage providers and storage users.
For end users, it can be used to evalute the compatibility level and
determine if the storage system in question is suitable for the required scenarios.
For storage providers, it helps to quickly generate an objective and reliable report
about core functioins of the storage service.
As an instance, if the HCFS got a 100% on a suite named 'tpcds',
it is demonstrated that all functions needed by a tpcds program have been well achieved.
It is also a guide indicating how storage service abilities can map to HCFS interfaces, such as storage class on S3.
