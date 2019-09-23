/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LIBHDFS_JCLASSES_H
#define LIBHDFS_JCLASSES_H

#include <jni.h>

/**
 * Encapsulates logic to cache jclass objects so they can re-used across
 * calls to FindClass. Creating jclass objects every time libhdfs has to
 * invoke a method can hurt performance. By cacheing jclass objects we avoid
 * this overhead.
 *
 * We use the term "cached" here loosely; jclasses are not truly cached,
 * instead they are created once during JVM load and are kept alive until the
 * process shutdowns. There is no eviction of jclass objects.
 *
 * @see https://www.ibm.com/developerworks/library/j-jni/index.html#notc
 */

/**
 * Each enum value represents one jclass that is cached. Enum values should
 * be passed to getJclass or getName to get the jclass object or class name
 * represented by the enum value.
 */
typedef enum {
    JC_CONFIGURATION,
    JC_PATH,
    JC_FILE_SYSTEM,
    JC_FS_STATUS,
    JC_FILE_UTIL,
    JC_BLOCK_LOCATION,
    JC_DFS_HEDGED_READ_METRICS,
    JC_DISTRIBUTED_FILE_SYSTEM,
    JC_FS_DATA_INPUT_STREAM,
    JC_FS_DATA_OUTPUT_STREAM,
    JC_FILE_STATUS,
    JC_FS_PERMISSION,
    JC_READ_STATISTICS,
    JC_HDFS_DATA_INPUT_STREAM,
    JC_DOMAIN_SOCKET,
    JC_URI,
    JC_BYTE_BUFFER,
    JC_ENUM_SET,
    JC_EXCEPTION_UTILS,
    // A special marker enum that counts the number of cached jclasses
    NUM_CACHED_CLASSES
} CachedJavaClass;

/**
 * Internally initializes all jclass objects listed in the CachedJavaClass
 * enum. This method is idempotent and thread-safe.
 */
jthrowable initCachedClasses(JNIEnv* env);

/**
 * Return the jclass object represented by the given CachedJavaClass
 */
jclass getJclass(CachedJavaClass cachedJavaClass);

/**
 * Return the class name represented by the given CachedJavaClass
 */
const char *getClassName(CachedJavaClass cachedJavaClass);

/* Some frequently used HDFS class names */
#define HADOOP_CONF     "org/apache/hadoop/conf/Configuration"
#define HADOOP_PATH     "org/apache/hadoop/fs/Path"
#define HADOOP_LOCALFS  "org/apache/hadoop/fs/LocalFileSystem"
#define HADOOP_FS       "org/apache/hadoop/fs/FileSystem"
#define HADOOP_FSSTATUS "org/apache/hadoop/fs/FsStatus"
#define HADOOP_FILEUTIL "org/apache/hadoop/fs/FileUtil"
#define HADOOP_BLK_LOC  "org/apache/hadoop/fs/BlockLocation"
#define HADOOP_DFS_HRM  "org/apache/hadoop/hdfs/DFSHedgedReadMetrics"
#define HADOOP_DFS      "org/apache/hadoop/hdfs/DistributedFileSystem"
#define HADOOP_FSDISTRM "org/apache/hadoop/fs/FSDataInputStream"
#define HADOOP_FSDOSTRM "org/apache/hadoop/fs/FSDataOutputStream"
#define HADOOP_FILESTAT "org/apache/hadoop/fs/FileStatus"
#define HADOOP_FSPERM   "org/apache/hadoop/fs/permission/FsPermission"
#define HADOOP_RSTAT    "org/apache/hadoop/hdfs/ReadStatistics"
#define HADOOP_HDISTRM  "org/apache/hadoop/hdfs/client/HdfsDataInputStream"
#define HADOOP_RO       "org/apache/hadoop/fs/ReadOption"
#define HADOOP_DS       "org/apache/hadoop/net/unix/DomainSocket"

/* Some frequently used Java class names */
#define JAVA_NET_ISA    "java/net/InetSocketAddress"
#define JAVA_NET_URI    "java/net/URI"
#define JAVA_BYTEBUFFER "java/nio/ByteBuffer"
#define JAVA_STRING     "java/lang/String"
#define JAVA_ENUMSET    "java/util/EnumSet"

/* Some frequently used third-party class names */

#define EXCEPTION_UTILS "org/apache/commons/lang3/exception/ExceptionUtils"

#endif /*LIBHDFS_JCLASSES_H*/
