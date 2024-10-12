### Initial test list for GitHub Actions (GHA)

Run GHA workflow repeatedly, and if a test fail or abort one time, add it into `exclude-tests.txt`.

Contributors are encouraged to diagnose and improve the excluded test cases, and remove them from the excluded list once they are stable.

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

Run all tests inside container and save the log to a file, then extract the failed test cases from the log file.
This might take a dozen of hours, be patient.
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
