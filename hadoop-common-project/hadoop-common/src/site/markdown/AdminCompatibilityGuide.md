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

Apache Hadoop Compatibility
===========================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Purpose
-------

This purpose of this document is to distill down the
[Hadoop Compatibility Guidelines](./Compatibility.html) into the information
relevant for a system administrator.

### Target Audience

The target audience is administrators who are responsible for maintaining
Apache Hadoop clusters and who must plan for and execute cluster upgrades.

Hadoop Releases
---------------

The Hadoop development community periodically produces new Hadoop releases to
introduce new functionality and fix existing issues. Realeses fall into three
categories:

* Major: a major release will typically include significant new functionality and generally represents the largest upgrade compatibility risk. A major release increments the first number of the release version, e.g. going from 2.8.2 to 3.0.0.
* Minor: a minor release will typically include some new functionality as well as fixes for some notable issues. A minor release should not pose much upgrade risk in most cases. A minor release increments the middle number of release version, e.g. going from 2.8.2 to 2.9.0.
* Maintenance: a maintenance release should not include any new functionality. The purpose of a maintenance release is to resolve a set of issues that are deemed by the developer community to be significant enough to be worth pushing a new release to address them. Maintenance releases should pose very little upgrade risk. A maintenance release increments the final number in the release version, e.g. going from 2.8.2 to 2.8.3.

Platform Dependencies
---------------------

The set of native components on which Hadoop depends is considered part of the
Hadoop ABI. The Hadoop development community endeavors to maintain ABI
compatibility to the fullest extent possible. Between minor releases the
minimum supported version numbers for Hadoop's native dependencies will not
be increased unless necessary, such as for security or licensing issues. When
such changes occur, the Hadoop developer community to try to keep the same
major version and only update the minor version.

Hadoop depends on the Java virtual machine. The minimum supported
version of the JVM will not change between major releases of Hadoop. In the
event that the current minimum supported JVM version becomes unsupported
between major releases, the minimum supported JVM version may be changed in a
minor release.

Network
-------

Hadoop has dependencies on some transport level technologies, such as SSL. The
minimum supported version of these dependencies will not
be increased unless necessary, such as for security or licensing issues. When
such changes occur, the Hadoop developer community to try to keep the same
major version and only update the minor version.

Service port numbers for Hadoop will remain the same within a major version,
though may be changed in a major release.

Hadoop's internal wire protocols will be maintained as backward and forward
compatible across minor releases within the same major version, both between
clients and servers and between servers, with the intent of enabling rolling
upgrades. Forward and backward compatibility of wire protocols across major
releases may be possible and may allow for rolling upgrades under certain
conditions, but no guarantees are made.

Scripting and Automation
------------------------

### REST APIs

The Hadoop REST APIs provide an easy mechanism for collecting information about
the state of the Hadoop system. To support REST clients, the Hadoop
REST APIs are versioned and will not change incompatibly within a version.
Both the endpoint itself along with the list of supported parameters and the
output from the endpoint are prohibited from changing incompatibly within a
REST endpoint version. Note, however, that introducing new fields and other
additive changes are considered compatible changes, so any consumer of the
REST API should be flexible enough to ignore unknown fields.

The REST API version is a single number and has no relationship with the Hadoop
version number. The version number is encoded in the endpoint URL prefixed
with a 'v', for example 'v1'. A new REST endpoint version may only be
introduced with a minor or major release. A REST endpoint version may only be
removed after being labeled as deprecated for a full major release.

### Parsing Hadoop Output

Hadoop produces a variety of outputs that could conceivably parsed by automated
tools. When consuming output from Hadoop, please consider the following:

* Hadoop log output is not expected to change with a maintenance release unless it resolves a correctness issue. While log output can be consumed by software directly, it is intended primarily for a human reader.
* Hadoop produces audit logs for a variety of operations. The audit logs are intended to be machine readable, though the addition of new records and fields are considered to be compatible changes. Any consumer of the audit logs should allow for unexpected records and fields. The audit log format may not change incompatibly between major releases.
* Metrics data produced by Hadoop is mostly intended for automated consumption. The metrics format may not change in an incompatible way between major releases, but new records and fields can be compatibly added at any time. Consumers of the metrics data should allow for unknown records and fields.

### CLIs

Hadoop's set of CLIs provide the ability to manage various aspects of the
system as well as discover information about the system's state.
Between major releases, no CLI tool options will be removed or
change semantically. The exception to that rule is CLI tools and tool options
that are explicitly labeled as experimental and subject to change.
The output from CLI tools will likewise remain the same
within a major version number unless otherwise documented.

Note that any change to CLI tool output is
considered an incompatible change, so between major versions, the CLI output
will not change. Note that the CLI tool output is distinct from
the log output produced by the CLI tools. Log output is not intended for
automated consumption and may change at any time.

### Web UI

The web UIs that are exposed by Hadoop are for human consumption only. Scraping
the UIs for data is not a supported use. No effort is made to ensure any
kind of compatibility between the data displayed in any of the web UIs
across releases.

Hadoop State Data
-----------------

Hadoop's internal system state is private and should not be modified directly.
The following policies govern the upgrade characteristics of the various
internal state stores:

* The internal MapReduce state data will remain compatible across minor releases within the same major version to facilitate rolling upgrades while MapReduce workloads execute.
* HDFS maintains metadata about the data stored in HDFS in a private, internal format that is versioned. In the event of an incompatible change, the store's version number will be incremented. When upgrading an existing cluster, the metadata store will automatically be upgraded if possible. After the metadata store has been upgraded, it is always possible to reverse the upgrade process.
* The AWS S3A guard keeps a private, internal metadata store that is versioned. Incompatible changes will cause the version number to be incremented. If an upgrade requires reformatting the store, it will be indicated in the release notes.
* The YARN resource manager keeps a private, internal state store of application and scheduler information that is versioned. Incompatible changes will cause the version number to be incremented. If an upgrade requires reformatting the store, it will be indicated in the release notes.
* The YARN node manager keeps a private, internal state store of application information that is versioned. Incompatible changes will cause the version number to be incremented. If an upgrade requires reformatting the store, it will be indicated in the release notes.
* The YARN federation service keeps a private, internal state store of application and cluster information that is versioned. Incompatible changes will cause the version number to be incremented. If an upgrade requires reformatting the store, it will be indicated in the release notes.

Hadoop Configurations
---------------------

Hadoop uses two primary forms of configuration files: XML configuration files
and logging configuration files.

### XML Configuration Files

The XML configuration files contain a set of properties as name-value pairs.
The names and meanings of the properties are defined by Hadoop and are
guaranteed to be stable across minor releases. A property can only be removed
in a major release and only if it has been marked as deprecated for at least a
full major release. Most properties have a default value that will be used if
the property is not explicitly set in the XML configuration files. The default
property values will not be changed during a maintenance releas.  For details
about the properties supported by the various Hadoop components,
see the component documentation.

Downstream projects and users can add their own properties into the XML
configuration files for use by their tools and applications. While Hadoop
makes no formal restrictions about defining new properties, a new property
that conflicts with a property defined by Hadoop can lead to unexpected and
undesirable results. Users are encouraged to avoid using custom configuration
property names that conflict with the namespace of Hadoop-defined properties
and thus should avoid using any prefixes used by Hadoop,
e.g. hadoop, io, ipc, fs, net, file, ftp, kfs, ha, file, dfs, mapred,
mapreduce, and yarn.

### Logging Configuration Files

The log output produced by Hadoop daemons and CLIs is governed by a set of
configuration files. These files control the minimum level of log message that
will be output by the various components of Hadoop, as well as where and how
those messages are stored. Between minor releases no changes will be made to
the log configuration that reduce, eliminate, or redirect the log messages.

### Other Configuration Files

Hadoop makes use of a number of other types of configuration files in a variety
of formats, such as the JSON resource profiles configuration or the XML fair
scheduler configuration. No incompatible changes will be introduced to the
configuration file formats within a minor release. Even between minor releases
incompatible configuration file format changes will be avoided if possible.

Hadoop Distribution
-------------------

### Configuration Files

The location and general structure of the Hadoop configuration files, job
history information (as consumed by the job history server), and logs files
generated by Hadoop will be maintained across maintenance releases.

### JARs, etc.

The contents of the Hadoop distribution, e.g. JAR files, are
subject to change at any time and should not be treated as reliable, except
for the client artifacts. Client artifacts and their contents will remain
compatible within a major release. It is the goal of the Hadoop development
community to allow application code to continue to function unchanged across
minor releases and, whenever possible, across major releases. The current list
of client artifacts is as follows:

* hadoop-client
* hadoop-client-api
* hadoop-client-minicluster
* hadoop-client-runtime
* hadoop-hdfs-client
* hadoop-hdfs-native-client
* hadoop-mapreduce-client-app
* hadoop-mapreduce-client-common
* hadoop-mapreduce-client-core
* hadoop-mapreduce-client-jobclient
* hadoop-mapreduce-client-nativetask
* hadoop-yarn-client

### Environment Variables

Some Hadoop components receive information through environment variables. For
example, the ```HADOOP_OPTS``` environment variable is interpreted by most
Hadoop processes as a string of additional JVM arguments to be used when
starting a new JVM. Between minor releases the way Hadoop interprets environment
variables will not change in an incompatible way. In other words, the same value
placed into the same variable should produce the same result for all Hadoop
releases within the same major version.

### Library Dependencies

Hadoop relies on a large number of third-party libraries for its operation. As
much as possible the Hadoop developer community works to hide these dependencies
from downstream developers. Nonetheless Hadoop does expose some
of its dependencies, especially prior to Hadoop 3. No new dependency
will be exposed by Hadoop via the client artifacts between major releases.

A common downstream anti-pattern is to use the output of ```hadoop classpath```
to set the downstream application's classpath or add all third-party JARs
included with Hadoop to the downstream application's classpath. This practice
creates a tight coupling between the downstream application and Hadoop's
third-party dependencies, which leads to a fragile application that is hard to
maintain as Hadoop's dependencies change. This practice is strongly discouraged.

Hadoop also includes several native components, including compression, the
container executor binary, and various native integrations. These native
components introduce a set of native dependencies for Hadoop. The set of
native dependencies can change in a minor release, but the Hadoop developer
community will try to limit any dependency version changes to minor version
changes as much as possible.

### Hardware and OS Dependencies

Hadoop is currently supported by the Hadoop developer community on Linux and
Windows running on x86 and AMD processors. These OSes and processors are likely
to remain supported for the foreseeable future. In the event that support plans
change, the OS or processor to be dropped will be documented as deprecated
for at least a full minor release, but ideally a full major release, before
actually being dropped. Hadoop may function on other OSes and processor
architectures, but the community may not be able to provide assistance in the
event of issues.

There are no guarantees on how the minimum resources required by Hadoop daemons
will change between releases, even maintenance releases. Nonetheless, the
Hadoop developer community will try to avoid increasing the requirements within
a minor release.

Any file systems supported Hadoop, such as through the FileSystem API, will
in most cases continue to be supported throughout a major release. The only
case where support for a file system can be dropped within a major version is
if a clean migration path to an alternate client implementation is provided.

Questions
---------

For question about developing applications and projects against Apache Hadoop,
please contact the [user mailing list](mailto:user@hadoop.apache.org).
