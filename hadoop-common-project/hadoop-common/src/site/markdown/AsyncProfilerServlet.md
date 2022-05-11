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

Async Profiler Servlet for Hadoop
========================================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Purpose
-------

This document describes how to configure and use async profiler
with Hadoop applications.
Async profiler is a low overhead sampling profiler for Java that
does not suffer from Safepoint bias problem. It features
HotSpot-specific APIs to collect stack traces and to track memory
allocations. The profiler works with OpenJDK, Oracle JDK and other
Java runtimes based on the HotSpot JVM.

Hadoop profiler servlet supports Async Profiler major versions
1.x and 2.x.

Prerequisites
-------------

Make sure Hadoop is installed, configured and setup correctly.
For more information see:

* [Single Node Setup](./SingleCluster.html) for first-time users.
* [Cluster Setup](./ClusterSetup.html) for large, distributed clusters.

Go to https://github.com/jvm-profiling-tools/async-profiler,
download a release appropriate for your platform, and install
on every cluster host.

Set `ASYNC_PROFILER_HOME` in the environment (put it in hadoop-env.sh)
to the root directory of the async-profiler install location, or pass
it on the Hadoop daemon's command line as a system property as
`-Dasync.profiler.home=/path/to/async-profiler`.


Usage
--------

Once the prerequisites have been satisfied, access to the async-profiler
is available by using Namenode or ResourceManager UI.

Following options from async-profiler can be specified as query paramater.
*  `-e event`          profiling event: cpu|alloc|lock|cache-misses etc.
*  `-d duration`       run profiling for 'duration' seconds (integer)
*  `-i interval`       sampling interval in nanoseconds (long)
*  `-j jstackdepth`    maximum Java stack depth (integer)
*  `-b bufsize`        frame buffer size (long)
*  `-t`                profile different threads separately
*  `-s`                simple class names instead of FQN
*  `-o fmt[,fmt...]`   output format: summary|traces|flat|collapsed|svg|tree|jfr|html
*  `--width px`        SVG width pixels (integer)
*  `--height px`       SVG frame height pixels (integer)
*  `--minwidth px`     skip frames smaller than px (double)
*  `--reverse`         generate stack-reversed FlameGraph / Call tree


Example:
If Namenode http address is localhost:9870, and ResourceManager http
address is localhost:8088, ProfileServlet running with async-profiler
setup can be accessed with http://localhost:9870/prof and
http://localhost:8088/prof for Namenode and ResourceManager processes
respectively.

Diving deep into some params:

* To collect 10 second CPU profile of current process
  (returns FlameGraph svg)
  * `curl http://localhost:9870/prof` (FlameGraph svg for Namenode)
  * `curl http://localhost:8088/prof` (FlameGraph svg for ResourceManager)
* To collect 10 second CPU profile of pid 12345 (returns FlameGraph svg)
  * `curl http://localhost:9870/prof?pid=12345` (For instance, provide
   pid of Datanode here)
* To collect 30 second CPU profile of pid 12345 (returns FlameGraph svg)
  * `curl http://localhost:9870/prof?pid=12345&duration=30`
* To collect 1 minute CPU profile of current process and output in tree
  format (html)
  * `curl http://localhost:9870/prof?output=tree&amp;duration=60`
* To collect 10 second heap allocation profile of current process
  (returns FlameGraph svg)
  * `curl http://localhost:9870/prof?event=alloc`
* To collect lock contention profile of current process
  (returns FlameGraph svg)
  * `curl http://localhost:9870/prof?event=lock`


The following event types are supported by async-profiler.
Use the 'event' parameter to specify. Default is 'cpu'.
Not all operating systems will support all types.

Perf events:

* cpu
* page-faults
* context-switches
* cycles
* instructions
* cache-references
* cache-misses
* branches
* branch-misses
* bus-cycles
* L1-dcache-load-misses
* LLC-load-misses
* dTLB-load-misses

Java events:

* alloc
* lock

The following output formats are supported.
Use the 'output' parameter to specify. Default is 'flamegraph'.

Output formats:

* summary: A dump of basic profiling statistics.
* traces: Call traces.
* flat: Flat profile (top N hot methods).
* collapsed: Collapsed call traces in the format used by FlameGraph
  script. This is a collection of call stacks, where each line is a
  semicolon separated list of frames followed by a counter.
* svg: FlameGraph in SVG format.
* tree: Call tree in HTML format.
* jfr: Call traces in Java Flight Recorder format.

The 'duration' parameter specifies how long to collect trace data
before generating output, specified in seconds. The default is 10 seconds.

