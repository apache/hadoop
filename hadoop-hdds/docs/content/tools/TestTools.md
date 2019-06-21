---
title: "Testing tools"

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

Testing is one of the most important part during the development of a distributed system. We have the following type of test.

This page includes our existing test tool which are part of the Ozone source base.

Note: we have more tests (like TCP-DS, TCP-H tests via Spark or Hive) which are not included here because they use external tools only.

## Unit test

As every almost every java project we have the good old unit tests inside each of our projects.

## Integration test (JUnit)

Traditional unit tests are supposed to test only one unit, but we also have higher level unit tests. They use `MiniOzoneCluster` which is a helper method to start real daemons (scm,om,datanodes) during the unit test.

From maven/java point of view they are just simple unit tests (JUnit library is used) but to separate them (and solve some dependency problems) we moved all of these tests to `hadoop-ozone/integration-test`

## Smoketest

We use docker-compose based pseudo-cluster to run different configuration of Ozone. To be sure that the different configuration can be started we implemented _acceptance_ tests with the help of https://robotframework.org/.

The smoketests are available from the distribution (`./smoketest`) but the robot files defines only the tests: usually they start CLI and check the output.

To run the tests in different environmente (docker-compose, kubernetes) you need a definition to start the containers and execute the right tests in the right containers.

These definition of the tests are included in the `compose` directory (check `./compose/*/test.sh` or `./compose/test-all.sh`).

For example a simple way to test the distribution packege:

```
cd compose/ozonze
./test.sh
```

## Blockade

[Blockade](https://github.com/worstcase/blockade) is a tool to test network failures and partitions (it's inspired by the legendary [Jepsen tests](https://jepsen.io/analyses)).

Blockade tests are implemented with the help of tests and can be started from the `./blockade` directory of the distrubution.

```
cd blocakde
pip install pytest==2.8.7,blockade
python -m pytest -s .
```

See the README in the blockade directory for more details.

## MiniChaosOzoneCluster

This is a way to get [chaos](https://en.wikipedia.org/wiki/Chaos_engineering) in your machine. It can be started from the source code and a MiniOzoneCluster (which starts real daemons) will be started and killed randomly.

## Freon

Freon is a command line application which is included in the Ozone distribution. It's a load generator which is used in our stress tests.

For example:

```
ozone freon randomkeys --numOfVolumes=10 --numOfBuckets 10 --numOfKeys 10  --replicationType=RATIS --factor=THREE
```

```
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
```

For more information check the [documentation page](https://hadoop.apache.org/ozone/docs/0.4.0-alpha/freon.html)

## Genesis

Genesis is a microbenchmarking tool. It's also included in the distribution (`ozone genesis`) but it doesn't require real cluster. It measures different part of the code in an isolated way (eg. the code which saves the data to the local RocksDB based key value stores)

Example run:

```
 ozone genesis -benchmark=BenchMarkRocksDbStore
# JMH version: 1.19
# VM version: JDK 11.0.1, VM 11.0.1+13-LTS
# VM invoker: /usr/lib/jvm/java-11-openjdk-11.0.1.13-3.el7_6.x86_64/bin/java
# VM options: -Dproc_genesis -Djava.net.preferIPv4Stack=true -Dhadoop.log.dir=/var/log/hadoop -Dhadoop.log.file=hadoop.log -Dhadoop.home.dir=/opt/hadoop -Dhadoop.id.str=hadoop -Dhadoop.root.logger=INFO,console -Dhadoop.policy.file=hadoop-policy.xml -Dhadoop.security.logger=INFO,NullAppender
# Warmup: 2 iterations, 1 s each
# Measurement: 20 iterations, 1 s each
# Timeout: 10 min per iteration
# Threads: 4 threads, will synchronize iterations
# Benchmark mode: Throughput, ops/time
# Benchmark: org.apache.hadoop.ozone.genesis.BenchMarkRocksDbStore.test
# Parameters: (backgroundThreads = 4, blockSize = 8, maxBackgroundFlushes = 4, maxBytesForLevelBase = 512, maxOpenFiles = 5000, maxWriteBufferNumber = 16, writeBufferSize = 64)

# Run progress: 0.00% complete, ETA 00:00:22
# Fork: 1 of 1
# Warmup Iteration   1: 213775.360 ops/s
# Warmup Iteration   2: 32041.633 ops/s
Iteration   1: 196342.348 ops/s
                 ?stack: <delayed till summary>

Iteration   2: 41926.816 ops/s
                 ?stack: <delayed till summary>

Iteration   3: 210433.231 ops/s
                 ?stack: <delayed till summary>

Iteration   4: 46941.951 ops/s
                 ?stack: <delayed till summary>

Iteration   5: 212825.884 ops/s
                 ?stack: <delayed till summary>

Iteration   6: 145914.351 ops/s
                 ?stack: <delayed till summary>

Iteration   7: 141838.469 ops/s
                 ?stack: <delayed till summary>

Iteration   8: 205334.438 ops/s
                 ?stack: <delayed till summary>

Iteration   9: 163709.519 ops/s
                 ?stack: <delayed till summary>

Iteration  10: 162494.608 ops/s
                 ?stack: <delayed till summary>

Iteration  11: 199155.793 ops/s
                 ?stack: <delayed till summary>

Iteration  12: 209679.298 ops/s
                 ?stack: <delayed till summary>

Iteration  13: 193787.574 ops/s
                 ?stack: <delayed till summary>

Iteration  14: 127004.147 ops/s
                 ?stack: <delayed till summary>

Iteration  15: 145511.080 ops/s
                 ?stack: <delayed till summary>

Iteration  16: 223433.864 ops/s
                 ?stack: <delayed till summary>

Iteration  17: 169752.665 ops/s
                 ?stack: <delayed till summary>

Iteration  18: 165217.191 ops/s
                 ?stack: <delayed till summary>

Iteration  19: 191038.476 ops/s
                 ?stack: <delayed till summary>

Iteration  20: 196335.579 ops/s
                 ?stack: <delayed till summary>



Result "org.apache.hadoop.ozone.genesis.BenchMarkRocksDbStore.test":
  167433.864 ?(99.9%) 43530.883 ops/s [Average]
  (min, avg, max) = (41926.816, 167433.864, 223433.864), stdev = 50130.230
  CI (99.9%): [123902.981, 210964.748] (assumes normal distribution)

Secondary result "org.apache.hadoop.ozone.genesis.BenchMarkRocksDbStore.test:?stack":
Stack profiler:

....[Thread state distributions]....................................................................
 78.9%         RUNNABLE
 20.0%         TIMED_WAITING
  1.1%         WAITING

....[Thread state: RUNNABLE]........................................................................
 59.8%  75.8% org.rocksdb.RocksDB.put
 16.5%  20.9% org.rocksdb.RocksDB.get
  0.7%   0.9% java.io.UnixFileSystem.delete0
  0.7%   0.9% org.rocksdb.RocksDB.disposeInternal
  0.3%   0.4% java.lang.Long.formatUnsignedLong0
  0.1%   0.2% org.apache.hadoop.ozone.genesis.BenchMarkRocksDbStore.test
  0.1%   0.1% java.lang.Long.toUnsignedString0
  0.1%   0.1% org.apache.hadoop.ozone.genesis.generated.BenchMarkRocksDbStore_test_jmhTest.test_thrpt_jmhStub
  0.0%   0.1% java.lang.Object.clone
  0.0%   0.0% java.lang.Thread.currentThread
  0.4%   0.5% <other>

....[Thread state: TIMED_WAITING]...................................................................
 20.0% 100.0% java.lang.Object.wait

....[Thread state: WAITING].........................................................................
  1.1% 100.0% jdk.internal.misc.Unsafe.park



# Run complete. Total time: 00:00:38

Benchmark                          (backgroundThreads)  (blockSize)  (maxBackgroundFlushes)  (maxBytesForLevelBase)  (maxOpenFiles)  (maxWriteBufferNumber)  (writeBufferSize)   Mode  Cnt       Score       Error  Units
BenchMarkRocksDbStore.test                           4            8                       4                     512            5000                      16                 64  thrpt   20  167433.864 ? 43530.883  ops/s
BenchMarkRocksDbStore.test:?stack                    4            8                       4                     512            5000                      16                 64  thrpt              NaN                ---
```
