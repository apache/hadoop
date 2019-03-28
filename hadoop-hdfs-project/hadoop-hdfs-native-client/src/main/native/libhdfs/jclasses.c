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

#include "exception.h"
#include "jclasses.h"
#include "jni_helper.h"
#include "os/mutexes.h"

#include <assert.h>

/**
 * Whether initCachedClasses has been called or not. Protected by the mutex
 * jclassInitMutex.
 */
static int jclassesInitialized = 0;

typedef struct {
    jclass javaClass;
    const char *className;
} javaClassAndName;

/**
 * A collection of commonly used jclass objects that are used throughout
 * libhdfs. The jclasses are loaded immediately after the JVM is created (see
 * initCachedClasses). The array is indexed using CachedJavaClass.
 */
javaClassAndName cachedJavaClasses[NUM_CACHED_CLASSES];

/**
 * Helper method that creates and sets a jclass object given a class name.
 * Returns a jthrowable on error, NULL otherwise.
 */
static jthrowable initCachedClass(JNIEnv *env, const char *className,
        jclass *cachedJclass) {
    assert(className != NULL && "Found a CachedJavaClass without a class "
                                "name");
    jthrowable jthr = NULL;
    jclass tempLocalClassRef;
    tempLocalClassRef = (*env)->FindClass(env, className);
    if (!tempLocalClassRef) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    *cachedJclass = (jclass) (*env)->NewGlobalRef(env, tempLocalClassRef);
    if (!*cachedJclass) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
done:
    destroyLocalReference(env, tempLocalClassRef);
    return jthr;
}

jthrowable initCachedClasses(JNIEnv* env) {
    mutexLock(&jclassInitMutex);
    if (!jclassesInitialized) {
        // Set all the class names
        cachedJavaClasses[JC_CONFIGURATION].className =
                "org/apache/hadoop/conf/Configuration";
        cachedJavaClasses[JC_PATH].className =
                "org/apache/hadoop/fs/Path";
        cachedJavaClasses[JC_FILE_SYSTEM].className =
                "org/apache/hadoop/fs/FileSystem";
        cachedJavaClasses[JC_FS_STATUS].className =
                "org/apache/hadoop/fs/FsStatus";
        cachedJavaClasses[JC_FILE_UTIL].className =
                "org/apache/hadoop/fs/FileUtil";
        cachedJavaClasses[JC_BLOCK_LOCATION].className =
                "org/apache/hadoop/fs/BlockLocation";
        cachedJavaClasses[JC_DFS_HEDGED_READ_METRICS].className =
                "org/apache/hadoop/hdfs/DFSHedgedReadMetrics";
        cachedJavaClasses[JC_DISTRIBUTED_FILE_SYSTEM].className =
                "org/apache/hadoop/hdfs/DistributedFileSystem";
        cachedJavaClasses[JC_FS_DATA_INPUT_STREAM].className =
                "org/apache/hadoop/fs/FSDataInputStream";
        cachedJavaClasses[JC_FS_DATA_OUTPUT_STREAM].className =
                "org/apache/hadoop/fs/FSDataOutputStream";
        cachedJavaClasses[JC_FILE_STATUS].className =
                "org/apache/hadoop/fs/FileStatus";
        cachedJavaClasses[JC_FS_PERMISSION].className =
                "org/apache/hadoop/fs/permission/FsPermission";
        cachedJavaClasses[JC_READ_STATISTICS].className =
                "org/apache/hadoop/hdfs/ReadStatistics";
        cachedJavaClasses[JC_HDFS_DATA_INPUT_STREAM].className =
                "org/apache/hadoop/hdfs/client/HdfsDataInputStream";
        cachedJavaClasses[JC_DOMAIN_SOCKET].className =
                "org/apache/hadoop/net/unix/DomainSocket";
        cachedJavaClasses[JC_URI].className =
                "java/net/URI";
        cachedJavaClasses[JC_BYTE_BUFFER].className =
                "java/nio/ByteBuffer";
        cachedJavaClasses[JC_ENUM_SET].className =
                "java/util/EnumSet";
        cachedJavaClasses[JC_EXCEPTION_UTILS].className =
                "org/apache/commons/lang3/exception/ExceptionUtils";

        // Create and set the jclass objects based on the class names set above
        jthrowable jthr;
        int numCachedClasses =
                sizeof(cachedJavaClasses) / sizeof(javaClassAndName);
        for (int i = 0; i < numCachedClasses; i++) {
            jthr = initCachedClass(env, cachedJavaClasses[i].className,
                                   &cachedJavaClasses[i].javaClass);
            if (jthr) {
                mutexUnlock(&jclassInitMutex);
                return jthr;
            }
        }
        jclassesInitialized = 1;
    }
    mutexUnlock(&jclassInitMutex);
    return NULL;
}

jclass getJclass(CachedJavaClass cachedJavaClass) {
    return cachedJavaClasses[cachedJavaClass].javaClass;
}

const char *getClassName(CachedJavaClass cachedJavaClass) {
    return cachedJavaClasses[cachedJavaClass].className;
}
