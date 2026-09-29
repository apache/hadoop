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

MR Task-Level Security Enforcement
==================

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

Overview
-------
The goal of this feature tp provide a configurable mechanism to control which users are allowed to execute specific MapReduce jobs.
This feature aims to prevent unauthorized or potentially harmful mapper/reducer implementations from running within the Hadoop cluster.

In the standard Hadoop MapReduce execution flow:
1) A MapReduce job is submitted by a user.
2) The job is registered with the Resource Manager (RM).
3) The RM assigns the job to a Node Manager (NM), where the Application Master (AM) for the job is launched.
4) The AM requests additional containers from the cluster, to be able to start tasks.
5) The NM launches those containers, and the containers execute the mapper/reducer tasks defined by the job.

This feature introduces a security filtering mechanism inside the Application Master.
Before mapper or reducer tasks are launched, the AM will verify that the user-submitted MapReduce code complies with a cluster-defined security policy.
This ensures that only approved classes or packages can be executed inside the containers.
The goal is to protect the cluster from unwanted or unsafe task implementations, such as custom code that may introduce performance, stability, or security risks.

Upon receiving job metadata, the Application Master will:
1) Check the feature is enabled.
2) Check the user who submitted the job is allowed to bypass the security check.
3) Compare classes in job config against the denied task list.
4) If job is not authorised an exception will be thrown and AM will fail.

Configurations
-------

#### Enables MapReduce Task-Level Security Enforcement
When enabled, the Application Master performs validation of user-submitted mapper, reducer, and other task-related classes before launching containers.
This mechanism protects the cluster from running disallowed or unsafe task implementations as defined by administrator-controlled policies.
- Property name: mapreduce.security.enabled
- Property type: boolean
- Default: false (security disabled)


#### MapReduce Task-Level Security Enforcement: Property Domain
Defines the set of MapReduce configuration keys that represent user-supplied class names involved in task execution (e.g., mapper, reducer, partitioner).
The Application Master examines the values of these properties and checks whether any referenced class is listed in denied tasks.
Administrators may override this list to expand or restrict the validation domain.
- Property name: mapreduce.security.property-domain
- Property type: list of configuration keys
- Default:
  - mapreduce.job.combine.class
  - mapreduce.job.combiner.group.comparator.class
  - mapreduce.job.end-notification.custom-notifier-class
  - mapreduce.job.inputformat.class
  - mapreduce.job.map.class
  - mapreduce.job.map.output.collector.class
  - mapreduce.job.output.group.comparator.class
  - mapreduce.job.output.key.class
  - mapreduce.job.output.key.comparator.class
  - mapreduce.job.output.value.class
  - mapreduce.job.outputformat.class
  - mapreduce.job.partitioner.class
  - mapreduce.job.reduce.class
  - mapreduce.map.output.key.class
  - mapreduce.map.output.value.class
  - mapreduce.outputcommitter.factory.scheme.s3a
  - mapreduce.outputcommitter.factory.scheme.abfs
  - mapreduce.outputcommitter.factory.scheme.gs
  - mapreduce.outputcommitter.factory.scheme.hdfs
  - mapreduce.outputcommitter.named.classname
  - mapred.mapper.class
  - mapred.map.runner.class
  - mapred.reducer.class

#### MapReduce Task-Level Security Enforcement: Denied Tasks
Specifies the list of disallowed task implementation classes or packages.
If a user submits a job whose mapper, reducer, or other task-related classes match any entry in this blacklist.
- Property name: mapreduce.security.denied-tasks
- Property type: list of class name or package patterns
- Default: empty
- Example: org.apache.hadoop.streaming,org.apache.hadoop.examples.QuasiMonteCarlo

#### MapReduce Task-Level Security Enforcement: Allowed Users
Specifies users who may bypass the blacklist defined in denied tasks.
This whitelist is intended for trusted or system-level workflows that may legitimately require the use of restricted task implementations.
If the submitting user is listed here, blacklist enforcement is skipped, although standard Hadoop authentication and ACL checks still apply.
- Property name: mapreduce.security.allowed-users
- Property type: list of usernames
- Default: empty
- Example: alice,bob