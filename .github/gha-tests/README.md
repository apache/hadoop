<!--
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

### Excluded tests for GitHub Actions (GHA)

Initial excluded tests: run the GHA workflow, if a test fails or aborts, add it
to `exclude-tests.txt`. Repeat until 5 consecutive successes.

Contributors are encouraged to diagnose and improve the excluded tests, and remove
them from the excluded list once they are stable. Stability assessment: when deleting
tests from `exclude-tests.txt`, the GHA workflow was successfully executed 5 times
consecutively.

### Slow tests

Test classes takes more than 60s to complete in module `hadoop-hdfs-project/hadoop-hdfs`
are marked as slow tests, by adding JUnit5 annotation `@Tag("slow")` to the test class.

Slow tests are executed in a dedicated GHA job and roughly take 2.5 hours to complete.
Contributors are encouraged to diagnose and improve the slow tests to speed up the CI.

### Run test locally

Create a standard build environment using Docker.
```
$ cd <hadoop source code directory>
$ ./start-build-env.sh
... (wait for the container to start)
```

Run single test suite inside container
```
$ export MAVEN_ARGS="-Pnative -Drequire.fuse -Drequire.openssl -Drequire.snappy -Drequire.valgrind -Drequire.test.libhadoop"
$ ./mvnw $MAVEN_ARGS -pl :hadoop-common -am clean install -DskipTests
$ ./mvnw $MAVEN_ARGS -pl :hadoop-common test -Dtest=TestIPC
```

Run all tests inside container and save the log to a file, then extract the failed
test cases from the log file. This might take a dozen of hours, be patient.
```
$ export MAVEN_ARGS="-Pnative -Drequire.fuse -Drequire.openssl -Drequire.snappy -Drequire.valgrind -Drequire.test.libhadoop"
$ ./mvnw $MAVEN_ARGS clean install -DskipTests
$ ./mvnw $MAVEN_ARGS test --fail-at-end -Dmaven.test.failure.ignore=true \
    -Dsurefire.excludesFile=$PWD/.github/gha-tests/exclude-tests.txt \
    2>&1 | tee ~/hadoop-test.`date '+%Y%m%d'`.log
$ cat hadoop-test.`date '+%Y%m%d'`.log | \
    grep -E 'surefire:3.5.3:test|<<< FAILURE! - in' | \
    grep -o -E 'surefire:3.5.3:test.*|org.apache.hadoop.*'
```
