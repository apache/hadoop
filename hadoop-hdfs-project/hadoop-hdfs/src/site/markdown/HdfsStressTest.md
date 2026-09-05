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

HDFS Stress Test
================

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->


Overview
--------

`HdfsStressTest` is a compact, self-contained HDFS read/write load generator for
stress-testing a targeted set of DataNodes. It complements `TestDFSIO`.

`TestDFSIO` is the standard HDFS I/O benchmark, but it runs as a MapReduce job
scheduled by YARN across the whole cluster. That makes it hard to:

* generate a *controlled* QPS / throughput,
* target a specific subset of DataNodes (for example a single replica set),
* obtain client-side latency *distributions* (p50/p95/p99), and
* saturate just a subset of nodes quickly (it loads the entire cluster and can
  take a long time to exhaust it).

`HdfsStressTest` fills that gap with a single-process, no-MapReduce tool that:

* drives a configurable, steady read and/or write throughput (MB/s) with a
  global rate limiter, optionally ramping throughput up for acceleration tests;
* targets specific DataNodes via HDFS favored-nodes hints, so load lands on a
  chosen replica set;
* uses a configurable block/file size;
* pre-generates a cold-read corpus so measured reads bypass the OS page cache
  (see [Cold reads](#Cold_reads:_avoiding_page-cache_hits) below); and
* records client-side read/write latency distributions
  (p50, p75, p95, p99, min, max, mean, stddev) and effective QPS.

Because it is a plain `Tool`, it needs no YARN and only the HDFS client
configuration on the classpath.


How to run
----------

The tool ships in the `hadoop-hdfs` test jar. From a client host that has the
target cluster's HDFS configuration on its classpath:

```bash
hadoop jar hadoop-hdfs-<version>-tests.jar \
    org.apache.hadoop.hdfs.HdfsStressTest /path/to/stress.properties
```

The single argument is a Java properties file describing the workload (see
[Configuration](#Configuration) below). Any property can also be overridden on
the command line with a `-Dkey=value` ToolRunner option, which takes precedence
over the file:

```bash
hadoop jar hadoop-hdfs-<version>-tests.jar \
    org.apache.hadoop.hdfs.HdfsStressTest \
    -DwriteThroughputMB=400 /path/to/stress.properties
```

The tool runs two phases:

1. **Pre-test** &ndash; if a read workload is configured, it first writes the
   cold-read corpus (see [Cold reads](#Cold_reads:_avoiding_page-cache_hits)).
2. **Measurement** &ndash; it runs the write and/or read workloads concurrently
   for `testDurationSeconds` and prints throughput, effective QPS and latency
   percentiles for each.


Configuration
-------------

All keys are read from the properties file (or overridden with `-Dkey=value`).
Sizes are plain numbers in the unit named by the key.

| Property | Default | Description |
|:---------|:--------|:------------|
| `favoredDataNodes` | (none) | Comma-separated `host:port` list of DataNode transfer ports. Written blocks are pinned to these nodes via HDFS favored-nodes hints, so load lands on a chosen replica set instead of the whole cluster. |
| `replication` | `3` | Replication factor for files created by the tool. |
| `blockSizeMB` | `128` | Block/file size in MB. Each write and each read operation moves one block-sized file, so this also sets the I/O unit for the latency stats. |
| `testWriteDirectory` | (none) | HDFS directory for the write workload. Omit to disable writes. |
| `writeThroughputMB` | `0` | Target sustained write throughput in MB/s (`0` disables writes). |
| `endWriteThroughputMB` | `0` (no ramp) | If greater than `writeThroughputMB`, throughput ramps linearly from `writeThroughputMB` to this value over the run (acceleration / find-the-knee test); otherwise it stays constant. |
| `writeThreads` | `-1` (auto) | Writer worker threads; `-1` uses a fixed pool and lets the rate limiter govern throughput. |
| `testReadDirectories` | (none) | Comma-separated HDFS directories for the read workload and for the pre-test cold-read corpus. Omit to disable reads. |
| `readThroughputMB` | `0` | Target sustained read throughput in MB/s (`0` disables reads). |
| `endReadThroughputMB` | `0` (no ramp) | Optional linear read-throughput ramp end value; ramps only when greater than `readThroughputMB`. |
| `readThreads` | `-1` (auto) | Reader worker threads; `-1` uses a fixed pool. |
| `testReadFileSizeGB` | `0` | Total size of the cold-read corpus to pre-create, in GB. Set larger than the aggregate OS page cache of the target DataNodes (rule of thumb: ~2x their RAM) so reads cannot be served from cache. |
| `preTestWriteThroughputMB` | `0` (unlimited) | Pacing (MB/s) applied while generating the cold-read corpus. `0` (or any non-positive value) builds the corpus as fast as the client can, which is usually what you want; set a positive value to keep the pre-test from itself saturating the cluster. |
| `preTestWriteDurationSeconds` | `0` (unbounded) | Optional wall-clock safety cap on the pre-test phase; it stops when either the corpus reaches `testReadFileSizeGB` or, if this is positive, when this many seconds elapse. `0` (or any non-positive value) means no time cap. |
| `testDurationSeconds` | `60` | Length of the measured read/write window. |


### Example `stress.properties`

```properties
# Target a specific replica set (host:port of the DataNode xfer port).
favoredDataNodes=dn1.example.com:9866,dn2.example.com:9866,dn3.example.com:9866
replication=3
blockSizeMB=128

# Write workload.
testWriteDirectory=/tmp/hdfs-stress/write
writeThroughputMB=200

# Read workload (cold reads from files created in the pre-test phase).
testReadDirectories=/tmp/hdfs-stress/read
readThroughputMB=200

# Pre-test: create enough read data to exceed the DataNode page cache
# (typically ~2x the DataNode memory) so reads are served from disk.
testReadFileSizeGB=64
preTestWriteThroughputMB=400
preTestWriteDurationSeconds=600

# Main measurement window.
testDurationSeconds=300

# Optional acceleration stress test: linearly ramp throughput from the
# start value above to these end values over the test duration.
endWriteThroughputMB=600
endReadThroughputMB=600

# -1 => auto (a fixed worker pool; the rate limiter enforces throughput).
writeThreads=-1
readThreads=-1
```


Cold reads: avoiding page-cache hits
------------------------------------

A read benchmark is only meaningful if it exercises the DataNode disks rather
than the operating-system page cache. If reads keep hitting the same small set
of recently written blocks, the DataNodes serve them straight from RAM and the
numbers reflect memory bandwidth, not HDFS/disk performance.

To force cold reads, the **pre-test phase writes a corpus of size
`testReadFileSizeGB` once**, sized deliberately **larger than the combined page
cache (main memory) of the target DataNodes**. Because the corpus does not fit
in memory:

* the kernel continuously evicts older pages as newer blocks are written, so
* by the time the measured phase reads a given file again, its pages have
  already been evicted and are **no longer in the page cache**, and
* every measured read therefore falls through to disk &ndash; a true cold read.

The read workload also spreads its picks across the whole corpus (rather than
replaying a hot subset), keeping the page-cache hit rate near zero. Choose
`testReadFileSizeGB` at roughly **twice the target DataNode RAM** for a
comfortable margin. By default the pre-test builds that corpus as fast as the
client can (`preTestWriteThroughputMB=0`, `preTestWriteDurationSeconds=0` &ndash;
no rate or time cap); set `preTestWriteThroughputMB` to pace corpus creation, or
`preTestWriteDurationSeconds` to time-box it, if you want to bound the pre-test.

If you time-box the pre-test with `preTestWriteDurationSeconds`, the corpus may
stop short of `testReadFileSizeGB`. When that happens the tool prints a
`WARNING` that the corpus is smaller than requested and the measured reads may
be served from the page cache (so the numbers can be optimistic); raise or
remove the cap so the corpus reaches `testReadFileSizeGB`. Because reads are
served only from files created in this phase, enabling the read workload
(`readThroughputMB` &gt; 0) **requires** a positive `testReadFileSizeGB`; the
tool fails fast on startup otherwise rather than silently running no readers.
`blockSizeMB` must likewise be a positive number of MB.


Distributing load across multiple clients
-----------------------------------------

A single client process is limited by its own CPU, NIC and JVM. To drive higher
aggregate load, or to model many real writers/readers, **run the tool on several
client hosts at once** against the same cluster. The total offered load is the
sum of the per-client `writeThroughputMB` / `readThroughputMB`.

Guidelines when running multiple clients:

* Give each client a **distinct `testWriteDirectory`** (and, if pre-generating
  separate corpora, distinct `testReadDirectories`) so clients do not collide on
  paths.
* Point all clients at the **same `favoredDataNodes`** to concentrate load on
  one replica set, or at different sets to spread it.
* **Start the clients together** so their measurement windows overlap.
* Aggregate the per-client latency distributions and throughput to obtain the
  cluster-wide result.
