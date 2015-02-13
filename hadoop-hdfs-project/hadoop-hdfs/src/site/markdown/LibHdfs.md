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

C API libhdfs
=============

* [C API libhdfs](#C_API_libhdfs)
    * [Overview](#Overview)
    * [The APIs](#The_APIs)
    * [A Sample Program](#A_Sample_Program)
    * [How To Link With The Library](#How_To_Link_With_The_Library)
    * [Common Problems](#Common_Problems)
    * [Thread Safe](#Thread_Safe)

Overview
--------

libhdfs is a JNI based C API for Hadoop's Distributed File System (HDFS). It provides C APIs to a subset of the HDFS APIs to manipulate HDFS files and the filesystem. libhdfs is part of the Hadoop distribution and comes pre-compiled in `$HADOOP_HDFS_HOME/lib/native/libhdfs.so` . libhdfs is compatible with Windows and can be built on Windows by running `mvn compile` within the `hadoop-hdfs-project/hadoop-hdfs` directory of the source tree.

The APIs
--------

The libhdfs APIs are a subset of the [Hadoop FileSystem APIs](../../api/org/apache/hadoop/fs/FileSystem.html).

The header file for libhdfs describes each API in detail and is available in `$HADOOP_HDFS_HOME/include/hdfs.h`.

A Sample Program
----------------
```c
#include "hdfs.h"

int main(int argc, char **argv) {

    hdfsFS fs = hdfsConnect("default", 0);
    const char* writePath = "/tmp/testfile.txt";
    hdfsFile writeFile = hdfsOpenFile(fs, writePath, O_WRONLY |O_CREAT, 0, 0, 0);
    if(!writeFile) {
          fprintf(stderr, "Failed to open %s for writing!\n", writePath);
          exit(-1);
    }
    char* buffer = "Hello, World!";
    tSize num_written_bytes = hdfsWrite(fs, writeFile, (void*)buffer, strlen(buffer)+1);
    if (hdfsFlush(fs, writeFile)) {
           fprintf(stderr, "Failed to 'flush' %s\n", writePath);
          exit(-1);
    }
    hdfsCloseFile(fs, writeFile);
}
```

How To Link With The Library
----------------------------

See the CMake file for `test_libhdfs_ops.c` in the libhdfs source directory (`hadoop-hdfs-project/hadoop-hdfs/src/CMakeLists.txt`) or something like: `gcc above_sample.c -I$HADOOP_HDFS_HOME/include -L$HADOOP_HDFS_HOME/lib/native -lhdfs -o above_sample`

Common Problems
---------------

The most common problem is the `CLASSPATH` is not set properly when calling a program that uses libhdfs. Make sure you set it to all the Hadoop jars needed to run Hadoop itself as well as the right configuration directory containing `hdfs-site.xml`. It is not valid to use wildcard syntax for specifying multiple jars. It may be useful to run `hadoop classpath --glob` or `hadoop classpath --jar <path`\> to generate the correct classpath for your deployment. See [Hadoop Commands Reference](../hadoop-common/CommandsManual.html#classpath) for more information on this command.

Thread Safe
-----------

libdhfs is thread safe.

*   Concurrency and Hadoop FS "handles"

    The Hadoop FS implementation includes a FS handle cache which
    caches based on the URI of the namenode along with the user
    connecting. So, all calls to `hdfsConnect` will return the same
    handle but calls to `hdfsConnectAsUser` with different users will
    return different handles. But, since HDFS client handles are
    completely thread safe, this has no bearing on concurrency.

*   Concurrency and libhdfs/JNI

    The libhdfs calls to JNI should always be creating thread local
    storage, so (in theory), libhdfs should be as thread safe as the
    underlying calls to the Hadoop FS.


