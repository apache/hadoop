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

#include "common/hadoop_err.h"
#include "fs/common.h"
#include "fs/fs.h"
#include "jni/exception.h"
#include "jni/jni_helper.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

/* Some frequently used Java paths */
#define HADOOP_CONF     "org/apache/hadoop/conf/Configuration"
#define HADOOP_PATH     "org/apache/hadoop/fs/Path"
#define HADOOP_LOCALFS  "org/apache/hadoop/fs/LocalFileSystem"
#define HADOOP_FS       "org/apache/hadoop/fs/FileSystem"
#define HADOOP_FSSTATUS "org/apache/hadoop/fs/FsStatus"
#define HADOOP_BLK_LOC  "org/apache/hadoop/fs/BlockLocation"
#define HADOOP_DFS      "org/apache/hadoop/hdfs/DistributedFileSystem"
#define HADOOP_ISTRM    "org/apache/hadoop/fs/FSDataInputStream"
#define HADOOP_OSTRM    "org/apache/hadoop/fs/FSDataOutputStream"
#define HADOOP_STAT     "org/apache/hadoop/fs/FileStatus"
#define HADOOP_FSPERM   "org/apache/hadoop/fs/permission/FsPermission"
#define JAVA_NET_ISA    "java/net/InetSocketAddress"
#define JAVA_NET_URI    "java/net/URI"
#define JAVA_STRING     "java/lang/String"
#define READ_OPTION     "org/apache/hadoop/fs/ReadOption"

#define JAVA_VOID       "V"

/* Macros for constructing method signatures */
#define JPARAM(X)           "L" X ";"
#define JARRPARAM(X)        "[L" X ";"
#define JMETHOD1(X, R)      "(" X ")" R
#define JMETHOD2(X, Y, R)   "(" X Y ")" R
#define JMETHOD3(X, Y, Z, R)   "(" X Y Z")" R

#define KERBEROS_TICKET_CACHE_PATH "hadoop.security.kerberos.ticket.cache.path"

// Bit fields for hdfsFile_internal flags
#define HDFS_FILE_SUPPORTS_DIRECT_READ (1<<0)

/**
 * The C equivalent of org.apache.org.hadoop.FSData(Input|Output)Stream .
 */
enum hdfsStreamType {
    UNINITIALIZED = 0,
    INPUT = 1,
    OUTPUT = 2,
};

struct jni_fs {
    struct hadoop_fs_base base;
    jobject obj;
};

/**
 * The 'file-handle' to a file in hdfs.
 */
struct jni_file {
    struct hadoop_file_base base;
    jobject stream;
    enum hdfsStreamType type;
    int flags;
};

/**
 * Zero-copy options cache.
 *
 * We cache the EnumSet of ReadOptions which has to be passed into every
 * readZero call, to avoid reconstructing it each time.  This cache is cleared
 * whenever an element changes.
 */
struct jni_rz_options_cache {
    char *pool_name;
    int skip_checksums;
    jobject pool_obj;
    jobject enum_set;
};

struct jni_rz_buffer {
    struct hadoop_rz_buffer_base base;
    jobject byte_buffer;
    int direct;
};

static tSize jni_read_direct(JNIEnv *env, struct jni_file *file,
            void* buffer, tSize length);
static jthrowable getFileInfoFromStat(JNIEnv *env, jobject jStat,
            hdfsFileInfo *fileInfo);

static int jni_file_is_open_for_read(hdfsFile bfile)
{
    struct jni_file *file = (struct jni_file*)bfile;
    return (file->type == INPUT);
}

static int jni_file_is_open_for_write(hdfsFile bfile)
{
    struct jni_file *file = (struct jni_file*)bfile;
    return (file->type == OUTPUT);
}

static int jni_file_get_read_statistics(hdfsFile bfile,
            struct hdfsReadStatistics **stats)
{
    jthrowable jthr;
    jobject readStats = NULL;
    jvalue jVal;
    struct hdfsReadStatistics *s = NULL;
    int ret;
    JNIEnv* env = getJNIEnv();
    struct jni_file *file = (struct jni_file*)bfile;

    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }
    if (file->type != INPUT) {
        ret = EINVAL;
        goto done;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, file->stream, 
                  "org/apache/hadoop/hdfs/client/HdfsDataInputStream",
                  "getReadStatistics",
                  "()Lorg/apache/hadoop/hdfs/DFSInputStream$ReadStatistics;");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsFileGetReadStatistics: getReadStatistics failed");
        goto done;
    }
    readStats = jVal.l;
    s = malloc(sizeof(struct hdfsReadStatistics));
    if (!s) {
        ret = ENOMEM;
        goto done;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, readStats,
                  "org/apache/hadoop/hdfs/DFSInputStream$ReadStatistics",
                  "getTotalBytesRead", "()J");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsFileGetReadStatistics: getTotalBytesRead failed");
        goto done;
    }
    s->totalBytesRead = jVal.j;

    jthr = invokeMethod(env, &jVal, INSTANCE, readStats,
                  "org/apache/hadoop/hdfs/DFSInputStream$ReadStatistics",
                  "getTotalLocalBytesRead", "()J");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsFileGetReadStatistics: getTotalLocalBytesRead failed");
        goto done;
    }
    s->totalLocalBytesRead = jVal.j;

    jthr = invokeMethod(env, &jVal, INSTANCE, readStats,
                  "org/apache/hadoop/hdfs/DFSInputStream$ReadStatistics",
                  "getTotalShortCircuitBytesRead", "()J");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsFileGetReadStatistics: getTotalShortCircuitBytesRead failed");
        goto done;
    }
    s->totalShortCircuitBytesRead = jVal.j;
    jthr = invokeMethod(env, &jVal, INSTANCE, readStats,
                  "org/apache/hadoop/hdfs/DFSInputStream$ReadStatistics",
                  "getTotalZeroCopyBytesRead", "()J");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsFileGetReadStatistics: getTotalZeroCopyBytesRead failed");
        goto done;
    }
    s->totalZeroCopyBytesRead = jVal.j;
    *stats = s;
    s = NULL;
    ret = 0;

done:
    destroyLocalReference(env, readStats);
    free(s);
    if (ret) {
      errno = ret;
      return -1;
    }
    return 0;
}

/**
 * hdfsJniEnv: A wrapper struct to be used as 'value'
 * while saving thread -> JNIEnv* mappings
 */
typedef struct
{
    JNIEnv* env;
} hdfsJniEnv;

/**
 * Helper function to create a org.apache.hadoop.fs.Path object.
 * @param env: The JNIEnv pointer. 
 * @param path: The file-path for which to construct org.apache.hadoop.fs.Path
 * object.
 * @return Returns a jobject on success and NULL on error.
 */
static jthrowable constructNewObjectOfPath(JNIEnv *env, const char *path,
                                           jobject *out)
{
    jthrowable jthr;
    jstring jPathString;
    jobject jPath;

    //Construct a java.lang.String object
    jthr = newJavaStr(env, path, &jPathString);
    if (jthr)
        return jthr;
    //Construct the org.apache.hadoop.fs.Path object
    jthr = constructNewObjectOfClass(env, &jPath, "org/apache/hadoop/fs/Path",
                                     "(Ljava/lang/String;)V", jPathString);
    destroyLocalReference(env, jPathString);
    if (jthr)
        return jthr;
    *out = jPath;
    return NULL;
}

static jthrowable get_str_from_conf(JNIEnv *env, jobject jConfiguration,
        const char *key, char **val)
{
    jthrowable jthr;
    jvalue jVal;
    jstring jkey = NULL, jRet = NULL;

    jthr = newJavaStr(env, key, &jkey);
    if (jthr)
        goto done;
    jthr = invokeMethod(env, &jVal, INSTANCE, jConfiguration,
            HADOOP_CONF, "get", JMETHOD1(JPARAM(JAVA_STRING),
                                         JPARAM(JAVA_STRING)), jkey);
    if (jthr)
        goto done;
    jRet = jVal.l;
    jthr = newCStr(env, jRet, val);
done:
    destroyLocalReference(env, jkey);
    destroyLocalReference(env, jRet);
    return jthr;
}

int jni_conf_get_str(const char *key, char **val)
{
    JNIEnv *env;
    int ret;
    jthrowable jthr;
    jobject jConfiguration = NULL;

    env = getJNIEnv();
    if (env == NULL) {
        ret = EINTERNAL;
        goto done;
    }
    jthr = constructNewObjectOfClass(env, &jConfiguration, HADOOP_CONF, "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_conf_get_str(%s): new Configuration", key);
        goto done;
    }
    jthr = get_str_from_conf(env, jConfiguration, key, val);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_conf_get_str(%s): hadoopConfGetStr", key);
        goto done;
    }
    ret = 0;
done:
    destroyLocalReference(env, jConfiguration);
    if (ret)
        errno = ret;
    return ret;
}

/**
 * Calculate the effective URI to use, given a builder configuration.
 *
 * If there is not already a URI scheme, we prepend 'hdfs://'.
 *
 * If there is not already a port specified, and a port was given to the
 * builder, we suffix that port.  If there is a port specified but also one in
 * the URI, that is an error.
 *
 * @param bld       The hdfs builder object
 * @param uri       (out param) dynamically allocated string representing the
 *                  effective URI
 *
 * @return          0 on success; error code otherwise
 */
static int calcEffectiveURI(struct hdfsBuilder *bld, char ** uri)
{
    const char *scheme;
    char suffix[64];
    const char *lastColon;
    char *u;
    size_t uriLen;

    if (!bld->nn)
        return EINVAL;
    scheme = (strstr(bld->nn, "://")) ? "" : "hdfs://";
    if (bld->port == 0) {
        suffix[0] = '\0';
    } else {
        lastColon = rindex(bld->nn, ':');
        if (lastColon && (strspn(lastColon + 1, "0123456789") ==
                          strlen(lastColon + 1))) {
            fprintf(stderr, "port %d was given, but URI '%s' already "
                "contains a port!\n", bld->port, bld->nn);
            return EINVAL;
        }
        snprintf(suffix, sizeof(suffix), ":%d", bld->port);
    }

    uriLen = strlen(scheme) + strlen(bld->nn) + strlen(suffix);
    u = malloc((uriLen + 1) * (sizeof(char)));
    if (!u) {
        fprintf(stderr, "calcEffectiveURI: out of memory");
        return ENOMEM;
    }
    snprintf(u, uriLen + 1, "%s%s%s", scheme, bld->nn, suffix);
    *uri = u;
    return 0;
}

static const char *maybeNull(const char *str)
{
    return str ? str : "(NULL)";
}

static const char *hdfsBuilderToStr(const struct hdfsBuilder *bld,
                                    char *buf, size_t bufLen)
{
    snprintf(buf, bufLen, "nn=%s, port=%d, "
             "kerbTicketCachePath=%s, userName=%s",
             maybeNull(bld->nn), bld->port,
             maybeNull(bld->kerbTicketCachePath), maybeNull(bld->userName));
    return buf;
}

struct hadoop_err *jni_connect(struct hdfsBuilder *bld,
                                  struct hdfs_internal **out)
{
    struct jni_fs *fs = NULL;
    JNIEnv *env = 0;
    jobject jConfiguration = NULL, fsObj = NULL, jURI = NULL, jCachePath = NULL;
    jstring jURIString = NULL, jUserString = NULL;
    jvalue  jVal;
    jthrowable jthr = NULL;
    char *cURI = 0, buf[512];
    int ret;
    struct hdfsBuilderConfOpt *opt;
    int forceNewInstance = 1;

    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();
    if (env == NULL) {
        ret = EINTERNAL;
        goto done;
    }
    fs = calloc(1, sizeof(*fs));
    if (!fs) {
        ret = ENOMEM;
        goto done;
    }
    fs->base.ty = HADOOP_FS_TY_JNI;

    //  jConfiguration = new Configuration();
    jthr = constructNewObjectOfClass(env, &jConfiguration, HADOOP_CONF, "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsBuilderConnect(%s)", hdfsBuilderToStr(bld, buf, sizeof(buf)));
        goto done;
    }
    // set configuration values
    for (opt = bld->opts; opt; opt = opt->next) {
        jthr = hadoopConfSetStr(env, jConfiguration, opt->key, opt->val);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hdfsBuilderConnect(%s): error setting conf '%s' to '%s'",
                hdfsBuilderToStr(bld, buf, sizeof(buf)), opt->key, opt->val);
            goto done;
        }
    }
 
    //Check what type of FileSystem the caller wants...
    if (bld->nn == NULL) {
        // Get a local filesystem.
        if (forceNewInstance) {
            // fs = FileSytem#newInstanceLocal(conf);
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS,
                    "newInstanceLocal", JMETHOD1(JPARAM(HADOOP_CONF),
                    JPARAM(HADOOP_LOCALFS)), jConfiguration);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "hdfsBuilderConnect(%s)",
                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            fsObj = jVal.l;
        } else {
            // fs = FileSytem#getLocal(conf);
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS, "getLocal",
                             JMETHOD1(JPARAM(HADOOP_CONF),
                                      JPARAM(HADOOP_LOCALFS)),
                             jConfiguration);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "hdfsBuilderConnect(%s)",
                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            fsObj = jVal.l;
        }
    } else {
        if (!strcmp(bld->nn, "default")) {
            // jURI = FileSystem.getDefaultUri(conf)
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS,
                          "getDefaultUri",
                          "(Lorg/apache/hadoop/conf/Configuration;)Ljava/net/URI;",
                          jConfiguration);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "hdfsBuilderConnect(%s)",
                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jURI = jVal.l;
        } else {
            // fs = FileSystem#get(URI, conf, ugi);
            ret = calcEffectiveURI(bld, &cURI);
            if (ret)
                goto done;
            jthr = newJavaStr(env, cURI, &jURIString);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "hdfsBuilderConnect(%s)",
                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jthr = invokeMethod(env, &jVal, STATIC, NULL, JAVA_NET_URI,
                             "create", "(Ljava/lang/String;)Ljava/net/URI;",
                             jURIString);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "hdfsBuilderConnect(%s)",
                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jURI = jVal.l;
        }

        if (bld->kerbTicketCachePath) {
            jthr = hadoopConfSetStr(env, jConfiguration,
                KERBEROS_TICKET_CACHE_PATH, bld->kerbTicketCachePath);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "hdfsBuilderConnect(%s)",
                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
        }
        jthr = newJavaStr(env, bld->userName, &jUserString);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hdfsBuilderConnect(%s)",
                hdfsBuilderToStr(bld, buf, sizeof(buf)));
            goto done;
        }
        if (forceNewInstance) {
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS,
                    "newInstance", JMETHOD3(JPARAM(JAVA_NET_URI), 
                        JPARAM(HADOOP_CONF), JPARAM(JAVA_STRING),
                        JPARAM(HADOOP_FS)),
                    jURI, jConfiguration, jUserString);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "hdfsBuilderConnect(%s)",
                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            fsObj = jVal.l;
        } else {
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS, "get",
                    JMETHOD3(JPARAM(JAVA_NET_URI), JPARAM(HADOOP_CONF),
                        JPARAM(JAVA_STRING), JPARAM(HADOOP_FS)),
                        jURI, jConfiguration, jUserString);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "hdfsBuilderConnect(%s)",
                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            fsObj = jVal.l;
        }
    }
    fs->obj = (*env)->NewGlobalRef(env, fsObj);
    if (!fs->obj) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                    "hdfsBuilderConnect(%s)",
                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
        goto done;
    }
    ret = 0;

done:
    // Release unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, fsObj);
    destroyLocalReference(env, jURI);
    destroyLocalReference(env, jCachePath);
    destroyLocalReference(env, jURIString);
    destroyLocalReference(env, jUserString);
    free(cURI);

    if (ret) {
        free(fs);
        return hadoop_lerr_alloc(ret, "jni_connect: failed to connect: "
                                 "error %d", ret);
    }
    *out = (struct hdfs_internal *)fs;
    return NULL;
}

static int jni_disconnect(hdfsFS bfs)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    JNIEnv* env = getJNIEnv();
    jthrowable jthr;
    int ret;

    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    if (fs == NULL) {
        errno = EBADF;
        return -1;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, fs->obj, HADOOP_FS,
                     "close", "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_disconnect: FileSystem#close");
    } else {
        ret = 0;
    }
    (*env)->DeleteGlobalRef(env, fs->obj);
    free(fs);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

/**
 * Get the default block size of a FileSystem object.
 *
 * @param env       The Java env
 * @param jFS       The FileSystem object
 * @param jPath     The path to find the default blocksize at
 * @param out       (out param) the default block size
 *
 * @return          NULL on success; or the exception
 */
static jthrowable getDefaultBlockSize(JNIEnv *env, jobject jFS,
                                      jobject jPath, jlong *out)
{
    jthrowable jthr;
    jvalue jVal;

    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                 "getDefaultBlockSize", JMETHOD1(JPARAM(HADOOP_PATH), "J"), jPath);
    if (jthr)
        return jthr;
    *out = jVal.j;
    return NULL;
}

static hdfsFile jni_open_file(hdfsFS bfs, const char* path, int flags, 
                      int bufferSize, short replication, tSize blockSize)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    jstring jStrBufferSize = NULL, jStrReplication = NULL;
    jobject jConfiguration = NULL, jPath = NULL, jFile = NULL;
    jthrowable jthr;
    jvalue jVal;
    struct jni_file *file = NULL;
    int ret;

    /*
      JAVA EQUIVALENT:
       File f = new File(path);
       FSData{Input|Output}Stream f{is|os} = fs.create(f);
       return f{is|os};
    */
    /* Get the JNIEnv* corresponding to current thread */
    JNIEnv* env = getJNIEnv();
    int accmode = flags & O_ACCMODE;

    if (!env) {
        errno = EINTERNAL;
        return NULL;
    }

    if (accmode == O_RDONLY || accmode == O_WRONLY) {
	/* yay */
    } else if (accmode == O_RDWR) {
      fprintf(stderr, "ERROR: cannot open a hadoop file in O_RDWR mode\n");
      errno = ENOTSUP;
      return NULL;
    } else {
      fprintf(stderr, "ERROR: cannot open a hadoop file in mode 0x%x\n", accmode);
      errno = EINVAL;
      return NULL;
    }

    if ((flags & O_CREAT) && (flags & O_EXCL)) {
      fprintf(stderr, "WARN: hadoop does not truly support O_CREATE && O_EXCL\n");
    }

    /* The hadoop java api/signature */
    const char* method = NULL;
    const char* signature = NULL;

    if (accmode == O_RDONLY) {
	method = "open";
        signature = JMETHOD2(JPARAM(HADOOP_PATH), "I", JPARAM(HADOOP_ISTRM));
    } else if (flags & O_APPEND) {
	method = "append";
	signature = JMETHOD1(JPARAM(HADOOP_PATH), JPARAM(HADOOP_OSTRM));
    } else {
	method = "create";
	signature = JMETHOD2(JPARAM(HADOOP_PATH), "ZISJ", JPARAM(HADOOP_OSTRM));
    }

    /* Create an object of org.apache.hadoop.fs.Path */
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_open_file(%s): constructNewObjectOfPath", path);
        goto done;
    }

    /* Get the Configuration object from the FileSystem object */
    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS,
                     "getConf", JMETHOD1("", JPARAM(HADOOP_CONF)));
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_open_file(%s): FileSystem#getConf", path);
        goto done;
    }
    jConfiguration = jVal.l;

    jint jBufferSize = bufferSize;
    jshort jReplication = replication;
    jStrBufferSize = (*env)->NewStringUTF(env, "io.file.buffer.size"); 
    if (!jStrBufferSize) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL, "OOM");
        goto done;
    }
    jStrReplication = (*env)->NewStringUTF(env, "dfs.replication");
    if (!jStrReplication) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL, "OOM");
        goto done;
    }

    if (!bufferSize) {
        jthr = invokeMethod(env, &jVal, INSTANCE, jConfiguration, 
                         HADOOP_CONF, "getInt", "(Ljava/lang/String;I)I",
                         jStrBufferSize, 4096);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, NOPRINT_EXC_FILE_NOT_FOUND |
                NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_UNRESOLVED_LINK,
                "jni_open_file(%s): Configuration#getInt(io.file.buffer.size)",
                path);
            goto done;
        }
        jBufferSize = jVal.i;
    }

    if ((accmode == O_WRONLY) && (flags & O_APPEND) == 0) {
        if (!replication) {
            jthr = invokeMethod(env, &jVal, INSTANCE, jConfiguration, 
                             HADOOP_CONF, "getInt", "(Ljava/lang/String;I)I",
                             jStrReplication, 1);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "jni_open_file(%s): Configuration#getInt(dfs.replication)",
                    path);
                goto done;
            }
            jReplication = jVal.i;
        }
    }
 
    /* Create and return either the FSDataInputStream or
       FSDataOutputStream references jobject jStream */

    // READ?
    if (accmode == O_RDONLY) {
        jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS,
                       method, signature, jPath, jBufferSize);
    }  else if ((accmode == O_WRONLY) && (flags & O_APPEND)) {
        // WRITE/APPEND?
       jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS,
                       method, signature, jPath);
    } else {
        // WRITE/CREATE
        jboolean jOverWrite = 1;
        jlong jBlockSize = blockSize;

        if (jBlockSize == 0) {
            jthr = getDefaultBlockSize(env, fs->obj, jPath, &jBlockSize);
            if (jthr) {
                ret = EIO;
                goto done;
            }
        }
        jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS,
                         method, signature, jPath, jOverWrite,
                         jBufferSize, jReplication, jBlockSize);
    }
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_open_file(%s): FileSystem#%s(%s)", path, method, signature);
        goto done;
    }
    jFile = jVal.l;

    file = calloc(1, sizeof(*file));
    if (!file) {
        fprintf(stderr, "jni_open_file(%s): OOM\n", path);
        ret = ENOMEM;
        goto done;
    }
    file->stream = (*env)->NewGlobalRef(env, jFile);
    if (!file->stream) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "jni_open_file(%s): NewGlobalRef", path); 
        goto done;
    }
    file->type = (((flags & O_WRONLY) == 0) ? INPUT : OUTPUT);
    file->flags = 0;

    if ((flags & O_WRONLY) == 0) {
        // Try a test read to see if we can do direct reads
        char buf;
        if (jni_read_direct(env, file, &buf, 0) == 0) {
            // Success - 0-byte read should return 0
            file->flags |= HDFS_FILE_SUPPORTS_DIRECT_READ;
        } else if (errno != ENOTSUP) {
            // Unexpected error. Clear it, don't set the direct flag.
            fprintf(stderr,
                  "jni_open_file(%s): WARN: Unexpected error %d when testing "
                  "for direct read compatibility\n", path, errno);
        }
    }
    ret = 0;

done:
    destroyLocalReference(env, jStrBufferSize);
    destroyLocalReference(env, jStrReplication);
    destroyLocalReference(env, jConfiguration); 
    destroyLocalReference(env, jPath); 
    destroyLocalReference(env, jFile); 
    if (ret) {
        if (file) {
            if (file->stream) {
                (*env)->DeleteGlobalRef(env, file->stream);
            }
            free(file);
        }
        errno = ret;
        return NULL;
    }
    return (hdfsFile)file;
}

static int jni_close_file(hdfsFS fs __attribute__((unused)), hdfsFile bfile)
{
    struct jni_file *file = (struct jni_file*)bfile;
    jthrowable jthr;
    const char* interface;
    int ret;
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }
    if (file->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }
    interface = (file->type == INPUT) ?  HADOOP_ISTRM : HADOOP_OSTRM;
    jthr = invokeMethod(env, NULL, INSTANCE, file->stream, interface,
                     "close", "()V");
    if (jthr) {
        const char *interfaceShortName = (file->type == INPUT) ? 
            "FSDataInputStream" : "FSDataOutputStream";
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "%s#close", interfaceShortName);
    } else {
        ret = 0;
    }

    //De-allocate memory
    (*env)->DeleteGlobalRef(env, file->stream);
    free(file);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

static int jni_file_exists(hdfsFS bfs, const char *path)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    jobject jPath;
    jvalue  jVal;
    jthrowable jthr;
    JNIEnv *env = getJNIEnv();

    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }
    if (path == NULL) {
        errno = EINVAL;
        return -1;
    }
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_file_exists: constructNewObjectOfPath");
        return -1;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS,
            "exists", JMETHOD1(JPARAM(HADOOP_PATH), "Z"), jPath);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_file_exists: invokeMethod(%s)",
            JMETHOD1(JPARAM(HADOOP_PATH), "Z"));
        return -1;
    }
    if (jVal.z) {
        return 0;
    } else {
        errno = ENOENT;
        return -1;
    }
}

// Reads using the read(ByteBuffer) API, which does fewer copies
static tSize jni_read_direct(JNIEnv *env, struct jni_file *file,
                             void* buffer, tSize length)
{
    // JAVA EQUIVALENT:
    //  ByteBuffer bbuffer = ByteBuffer.allocateDirect(length) // wraps C buffer
    //  fis.read(bbuffer);
    jobject bb;
    jvalue jVal;
    jthrowable jthr;

    bb = (*env)->NewDirectByteBuffer(env, buffer, length);
    if (bb == NULL) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "jni_read_direct: NewDirectByteBuffer");
        return -1;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, file->stream,
        HADOOP_ISTRM, "read", "(Ljava/nio/ByteBuffer;)I", bb);
    destroyLocalReference(env, bb);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_read_direct: FSDataInputStream#read");
        return -1;
    }
    return (jVal.i < 0) ? 0 : jVal.i;
}

static tSize jni_read(hdfsFS bfs __attribute__((unused)), hdfsFile bfile,
                      void* buffer, tSize length)
{
    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(bR);
    struct jni_file *file = (struct jni_file*)bfile;
    JNIEnv* env = getJNIEnv();
    jbyteArray jbRarray;
    jint noReadBytes = length;
    jvalue jVal;
    jthrowable jthr;

    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    if (length == 0) {
        return 0;
    } else if (length < 0) {
        errno = EINVAL;
        return -1;
    } else if (file->type != INPUT) {
        errno = EINVAL;
        return -1;
    }
    if (file->flags & HDFS_FILE_SUPPORTS_DIRECT_READ) {
        return jni_read_direct(env, file, buffer, length);
    }
    jbRarray = (*env)->NewByteArray(env, length);
    if (!jbRarray) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "jni_read: NewByteArray");
        return -1;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, file->stream, HADOOP_ISTRM,
                               "read", "([B)I", jbRarray);
    if (jthr) {
        destroyLocalReference(env, jbRarray);
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_read: FSDataInputStream#read");
        return -1;
    }
    if (jVal.i < 0) {
        // EOF
        destroyLocalReference(env, jbRarray);
        return 0;
    } else if (jVal.i == 0) {
        destroyLocalReference(env, jbRarray);
        errno = EINTR;
        return -1;
    }
    (*env)->GetByteArrayRegion(env, jbRarray, 0, noReadBytes, buffer);
    destroyLocalReference(env, jbRarray);
    if ((*env)->ExceptionCheck(env)) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "hdfsRead: GetByteArrayRegion");
        return -1;
    }
    return jVal.i;
}

static tSize jni_pread(hdfsFS bfs __attribute__((unused)), hdfsFile bfile,
            tOffset position, void* buffer, tSize length)
{
    JNIEnv* env;
    jbyteArray jbRarray;
    jvalue jVal;
    jthrowable jthr;
    struct jni_file *file = (struct jni_file*)bfile;

    if (length == 0) {
        return 0;
    } else if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    if (file->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Error checking... make sure that this file is 'readable'
    if (file->type != INPUT) {
        fprintf(stderr, "Cannot read from a non-InputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(pos, bR, 0, length);
    jbRarray = (*env)->NewByteArray(env, length);
    if (!jbRarray) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "jni_pread: NewByteArray");
        return -1;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, file->stream, HADOOP_ISTRM,
                     "read", "(J[BII)I", position, jbRarray, 0, length);
    if (jthr) {
        destroyLocalReference(env, jbRarray);
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_pread: FSDataInputStream#read");
        return -1;
    }
    if (jVal.i < 0) {
        // EOF
        destroyLocalReference(env, jbRarray);
        return 0;
    } else if (jVal.i == 0) {
        destroyLocalReference(env, jbRarray);
        errno = EINTR;
        return -1;
    }
    (*env)->GetByteArrayRegion(env, jbRarray, 0, jVal.i, buffer);
    destroyLocalReference(env, jbRarray);
    if ((*env)->ExceptionCheck(env)) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "jni_pread: GetByteArrayRegion");
        return -1;
    }
    return jVal.i;
}

static tSize jni_write(hdfsFS bfs __attribute__((unused)), hdfsFile bfile,
            const void* buffer, tSize length)
{
    // JAVA EQUIVALENT
    // byte b[] = str.getBytes();
    // fso.write(b);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    struct jni_file *file = (struct jni_file*)bfile;
    jbyteArray jbWarray;
    jthrowable jthr;
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (file->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }
    if (length < 0) {
    	errno = EINVAL;
    	return -1;
    }
    //Error checking... make sure that this file is 'writable'
    if (file->type != OUTPUT) {
        fprintf(stderr, "Cannot write into a non-OutputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    if (length == 0) {
        return 0;
    }
    //Write the requisite bytes into the file
    jbWarray = (*env)->NewByteArray(env, length);
    if (!jbWarray) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "jni_write: NewByteArray");
        return -1;
    }
    (*env)->SetByteArrayRegion(env, jbWarray, 0, length, buffer);
    if ((*env)->ExceptionCheck(env)) {
        destroyLocalReference(env, jbWarray);
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "jni_write(length = %d): SetByteArrayRegion", length);
        return -1;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, file->stream,
            HADOOP_OSTRM, "write", "([B)V", jbWarray);
    destroyLocalReference(env, jbWarray);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_write: FSDataOutputStream#write");
        return -1;
    }
    // Unlike most Java streams, FSDataOutputStream never does partial writes.
    // If we succeeded, all the data was written.
    return length;
}

static int jni_seek(hdfsFS bfs __attribute__((unused)), hdfsFile bfile,
                    tOffset desiredPos) 
{
    // JAVA EQUIVALENT
    //  fis.seek(pos);
    struct jni_file *file = (struct jni_file*)bfile;
    jthrowable jthr;
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    if (file->type != INPUT) {
        errno = EBADF;
        return -1;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, file->stream,
            HADOOP_ISTRM, "seek", "(J)V", desiredPos);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_seek(desiredPos=%" PRId64 ")"
            ": FSDataInputStream#seek", desiredPos);
        return -1;
    }
    return 0;
}

static tOffset jni_tell(hdfsFS bfs __attribute__((unused)), hdfsFile bfile)
{
    // JAVA EQUIVALENT
    //  pos = f.getPos();

    //Get the JNIEnv* corresponding to current thread
    struct jni_file *file = (struct jni_file*)bfile;
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (file->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //Parameters
    const char* interface = (file->type == INPUT) ?
        HADOOP_ISTRM : HADOOP_OSTRM;
    jvalue jVal;
    jthrowable jthr = invokeMethod(env, &jVal, INSTANCE, file->stream,
                     interface, "getPos", "()J");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_tell: %s#getPos",
            ((file->type == INPUT) ? "FSDataInputStream" :
                                 "FSDataOutputStream"));
        return -1;
    }
    return jVal.j;
}

static int jni_flush(hdfsFS bfs __attribute__((unused)), hdfsFile bfile)
{
    //Get the JNIEnv* corresponding to current thread
    jthrowable jthr;
    struct jni_file *file = (struct jni_file*)bfile;
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    if (file->type != OUTPUT) {
        errno = EBADF;
        return -1;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, file->stream,
                     HADOOP_OSTRM, "flush", "()V");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_flush: FSDataOutputStream#flush");
        return -1;
    }
    return 0;
}

static int jni_hflush(hdfsFS bfs __attribute__((unused)), hdfsFile bfile)
{
    //Get the JNIEnv* corresponding to current thread
    jthrowable jthr;
    struct jni_file *file = (struct jni_file*)bfile;
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    if (file->type != OUTPUT) {
        errno = EBADF;
        return -1;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, file->stream,
                     HADOOP_OSTRM, "hflush", "()V");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_hflush: FSDataOutputStream#hflush");
        return -1;
    }
    return 0;
}

static int jni_hsync(hdfsFS bfs __attribute__((unused)), hdfsFile bfile)
{
    //Get the JNIEnv* corresponding to current thread
    jthrowable jthr;
    struct jni_file *file = (struct jni_file*)bfile;
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    if (file->type != OUTPUT) {
        errno = EBADF;
        return -1;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, file->stream,
                     HADOOP_OSTRM, "hsync", "()V");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_hsync: FSDataOutputStream#hsync");
        return -1;
    }
    return 0;
}

static int jni_available(hdfsFS bfs __attribute__((unused)), hdfsFile bfile)
{
    // JAVA EQUIVALENT
    //  fis.available();
    jvalue jVal;
    jthrowable jthr = NULL;
    struct jni_file *file = (struct jni_file*)bfile;
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    if (file->type != INPUT) {
        errno = EBADF;
        return -1;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, file->stream,
                     HADOOP_ISTRM, "available", "()I");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_available: FSDataInputStream#available");
        return -1;
    }
    return jVal.i;
}

static int jni_copy_impl(struct jni_fs *srcFS, const char* src,
        struct jni_fs *dstFs, const char* dst, jboolean deleteSource)
{
    //JAVA EQUIVALENT
    //  FileUtil#copy(srcFS, srcPath, dstFS, dstPath,
    //                 deleteSource = false, conf)
    jobject jConfiguration = NULL, jSrcPath = NULL, jDstPath = NULL;
    jthrowable jthr;
    jvalue jVal;
    int ret;
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    jthr = constructNewObjectOfPath(env, src, &jSrcPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_copy_impl(src=%s): constructNewObjectOfPath", src);
        goto done;
    }
    jthr = constructNewObjectOfPath(env, dst, &jDstPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_copy_impl(dst=%s): constructNewObjectOfPath", dst);
        goto done;
    }
    //Create the org.apache.hadoop.conf.Configuration object
    jthr = constructNewObjectOfClass(env, &jConfiguration,
                                     HADOOP_CONF, "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_copy_impl: Configuration constructor");
        goto done;
    }

    //FileUtil#copy
    jthr = invokeMethod(env, &jVal, STATIC,
            NULL, "org/apache/hadoop/fs/FileUtil", "copy",
            "(Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;"
            "Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;"
            "ZLorg/apache/hadoop/conf/Configuration;)Z",
            srcFS->obj, jSrcPath, dstFs->obj, jDstPath, deleteSource, 
            jConfiguration);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_copy_impl(src=%s, dst=%s, deleteSource=%d): "
            "FileUtil#copy", src, dst, deleteSource);
        goto done;
    }
    if (!jVal.z) {
        ret = EIO;
        goto done;
    }
    ret = 0;

done:
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jSrcPath);
    destroyLocalReference(env, jDstPath);
  
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

static int jni_copy(hdfsFS srcFS, const char* src, hdfsFS dstFS,
                    const char* dst)
{
    return jni_copy_impl((struct jni_fs*)srcFS, src,
                         (struct jni_fs*)dstFS, dst, 0);
}

static int jni_move(hdfsFS srcFS, const char* src, hdfsFS dstFS,
                    const char* dst)
{
    return jni_copy_impl((struct jni_fs*)srcFS, src,
                         (struct jni_fs*)dstFS, dst, 1);
}

static int jni_unlink(hdfsFS bfs, const char *path, int recursive)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    jthrowable jthr;
    jobject jPath;
    jvalue jVal;

    // JAVA EQUIVALENT:
    //  Path p = new Path(path);
    //  bool retval = fs.delete(p, recursive);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_unlink(path=%s): constructNewObjectOfPath", path);
        return -1;
    }
    jboolean jRecursive = recursive ? JNI_TRUE : JNI_FALSE;
    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS,
                     "delete", "(Lorg/apache/hadoop/fs/Path;Z)Z",
                     jPath, jRecursive);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_unlink(path=%s, recursive=%d): "
            "FileSystem#delete", path, recursive);
        return -1;
    }
    if (!jVal.z) {
        errno = EIO;
        return -1;
    }
    return 0;
}

static int jni_rename(hdfsFS bfs, const char* oldPath, const char* newPath)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;

    // JAVA EQUIVALENT:
    //  Path old = new Path(oldPath);
    //  Path new = new Path(newPath);
    //  fs.rename(old, new);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jthrowable jthr;
    jobject jOldPath = NULL, jNewPath = NULL;
    int ret = -1;
    jvalue jVal;

    jthr = constructNewObjectOfPath(env, oldPath, &jOldPath );
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_rename: constructNewObjectOfPath(%s)", oldPath);
        goto done;
    }
    jthr = constructNewObjectOfPath(env, newPath, &jNewPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_rename: constructNewObjectOfPath(%s)", newPath);
        goto done;
    }

    // Rename the file
    // TODO: use rename2 here?  (See HDFS-3592)
    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS, "rename",
                     JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_PATH), "Z"),
                     jOldPath, jNewPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_rename(oldPath=%s, newPath=%s): FileSystem#rename",
            oldPath, newPath);
        goto done;
    }
    if (!jVal.z) {
        errno = EIO;
        goto done;
    }
    ret = 0;

done:
    destroyLocalReference(env, jOldPath);
    destroyLocalReference(env, jNewPath);
    return ret;
}

static char* jni_get_working_directory(hdfsFS bfs, char *buffer,
                                       size_t bufferSize)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    // JAVA EQUIVALENT:
    //  Path p = fs.getWorkingDirectory(); 
    //  return p.toString()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    jobject jPath = NULL;
    jstring jPathString = NULL;
    jvalue jVal;
    jthrowable jthr;
    int ret;
    const char *jPathChars = NULL;

    //FileSystem#getWorkingDirectory()
    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj,
                     HADOOP_FS, "getWorkingDirectory",
                     "()Lorg/apache/hadoop/fs/Path;");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_get_working_directory: FileSystem#getWorkingDirectory");
        goto done;
    }
    jPath = jVal.l;
    if (!jPath) {
        fprintf(stderr, "jni_get_working_directory: "
            "FileSystem#getWorkingDirectory returned NULL");
        ret = -EIO;
        goto done;
    }

    //Path#toString()
    jthr = invokeMethod(env, &jVal, INSTANCE, jPath, 
                     "org/apache/hadoop/fs/Path", "toString",
                     "()Ljava/lang/String;");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_get_working_directory: Path#toString");
        goto done;
    }
    jPathString = jVal.l;
    jPathChars = (*env)->GetStringUTFChars(env, jPathString, NULL);
    if (!jPathChars) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "jni_get_working_directory: GetStringUTFChars");
        goto done;
    }

    //Copy to user-provided buffer
    ret = snprintf(buffer, bufferSize, "%s", jPathChars);
    if ((size_t)ret >= bufferSize) {
        ret = ENAMETOOLONG;
        goto done;
    }
    ret = 0;

done:
    if (jPathChars) {
        (*env)->ReleaseStringUTFChars(env, jPathString, jPathChars);
    }
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathString);

    if (ret) {
        errno = ret;
        return NULL;
    }
    return buffer;
}

static int jni_set_working_directory(hdfsFS bfs, const char* path)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;

    // JAVA EQUIVALENT:
    //  fs.setWorkingDirectory(Path(path)); 

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jthrowable jthr;
    jobject jPath;

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_set_working_directory(%s): constructNewObjectOfPath",
            path);
        return -1;
    }

    //FileSystem#setWorkingDirectory()
    jthr = invokeMethod(env, NULL, INSTANCE, fs->obj, HADOOP_FS,
                     "setWorkingDirectory", 
                     "(Lorg/apache/hadoop/fs/Path;)V", jPath);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, NOPRINT_EXC_ILLEGAL_ARGUMENT,
            "jni_set_working_directory(%s): FileSystem#setWorkingDirectory",
            path);
        return -1;
    }
    return 0;
}

static int jni_mkdir(hdfsFS bfs, const char* path)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    // JAVA EQUIVALENT:
    //  fs.mkdirs(new Path(path));

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jobject jPath;
    jthrowable jthr;

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_mkdir(%s): constructNewObjectOfPath", path);
        return -1;
    }

    //Create the directory
    jvalue jVal;
    jVal.z = 0;
    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS,
                     "mkdirs", "(Lorg/apache/hadoop/fs/Path;)Z",
                     jPath);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK | NOPRINT_EXC_PARENT_NOT_DIRECTORY,
            "jni_mkdir(%s): FileSystem#mkdirs", path);
        return -1;
    }
    if (!jVal.z) {
        // It's unclear under exactly which conditions FileSystem#mkdirs
        // is supposed to return false (as opposed to throwing an exception.)
        // It seems like the current code never actually returns false.
        // So we're going to translate this to EIO, since there seems to be
        // nothing more specific we can do with it.
        errno = EIO;
        return -1;
    }
    return 0;
}

static int jni_set_replication(hdfsFS bfs, const char* path,
                               int16_t replication)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    // JAVA EQUIVALENT:
    //  fs.setReplication(new Path(path), replication);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jthrowable jthr;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath;
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_set_replication(path=%s): constructNewObjectOfPath", path);
        return -1;
    }

    //Create the directory
    jvalue jVal;
    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS,
                     "setReplication", "(Lorg/apache/hadoop/fs/Path;S)Z",
                     jPath, replication);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_set_replication(path=%s, replication=%d): "
            "FileSystem#setReplication", path, replication);
        return -1;
    }
    if (!jVal.z) {
        // setReplication returns false "if file does not exist or is a
        // directory."  So the nearest translation to that is ENOENT.
        errno = ENOENT;
        return -1;
    }

    return 0;
}

static hdfsFileInfo* jni_list_directory(hdfsFS bfs, const char* path,
                                        int *numEntries)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    // JAVA EQUIVALENT:
    //  Path p(path);
    //  Path []pathList = fs.listPaths(p)
    //  foreach path in pathList 
    //    getFileInfo(path)
    jthrowable jthr;
    jobject jPath = NULL;
    hdfsFileInfo *pathList = NULL; 
    jobjectArray jPathList = NULL;
    jvalue jVal;
    jsize jPathListSize = 0;
    int ret;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_list_directory(%s): constructNewObjectOfPath", path);
        goto done;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_DFS, "listStatus",
                     JMETHOD1(JPARAM(HADOOP_PATH), JARRPARAM(HADOOP_STAT)),
                     jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "jni_list_directory(%s): FileSystem#listStatus", path);
        goto done;
    }
    jPathList = jVal.l;

    //Figure out the number of entries in that directory
    jPathListSize = (*env)->GetArrayLength(env, jPathList);
    if (jPathListSize == 0) {
        ret = 0;
        goto done;
    }

    //Allocate memory
    pathList = calloc(jPathListSize, sizeof(hdfsFileInfo));
    if (pathList == NULL) {
        ret = ENOMEM;
        goto done;
    }

    //Save path information in pathList
    jsize i;
    jobject tmpStat;
    for (i=0; i < jPathListSize; ++i) {
        tmpStat = (*env)->GetObjectArrayElement(env, jPathList, i);
        if (!tmpStat) {
            ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                "jni_list_directory(%s): GetObjectArrayElement(%d out of %d)",
                path, i, jPathListSize);
            goto done;
        }
        jthr = getFileInfoFromStat(env, tmpStat, &pathList[i]);
        destroyLocalReference(env, tmpStat);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "jni_list_directory(%s): getFileInfoFromStat(%d out of %d)",
                path, i, jPathListSize);
            goto done;
        }
    }
    ret = 0;

done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathList);

    if (ret) {
        hdfsFreeFileInfo(pathList, jPathListSize);
        errno = ret;
        return NULL;
    }
    *numEntries = jPathListSize;
    return pathList;
}

static void jni_rz_options_cache_free(void *vcache)
{
    struct jni_rz_options_cache *cache = vcache;
    JNIEnv *env;

    env = getJNIEnv();
    if (!env) {
        fprintf(stderr, "jni_rz_options_cache_free: failed to get JNI env\n");
        return;
    }
    free(cache->pool_name);
    if (cache->pool_obj) {
        (*env)->DeleteGlobalRef(env, cache->pool_obj);
    }
    if (cache->enum_set) {
        (*env)->DeleteGlobalRef(env, cache->enum_set);
    }
    free(cache);
}

static jthrowable jni_rz_setup_cache(JNIEnv *env, struct hadoopRzOptions *opts)
{
    if (opts->cache) {
        if (opts->cache_teardown_cb != jni_rz_options_cache_free) {
            return newRuntimeError(env, "jni_rz_options_get_enumset: this "
                    "hadoopRzOptions structure is already associated with "
                    "another filesystem.  Please create a new one.");
        }
        return NULL;
    }
    opts->cache = calloc(1, sizeof(struct jni_rz_options_cache));
    if (!opts->cache) {
        return newRuntimeError(env, "rz_get_or_create_cache: OOM "
                "allocating jni_rz_options_cache");
    }
    opts->cache_teardown_cb = jni_rz_options_cache_free;
    return NULL;
}

static jthrowable jni_rz_setup_enumset(JNIEnv *env,
        struct hadoopRzOptions *opts)
{
    jthrowable jthr = NULL;
    jobject enumInst = NULL, enumSetObj = NULL;
    jvalue jVal;
    struct jni_rz_options_cache *cache = opts->cache;

    fprintf(stderr, "WATERMELON 4.1\n");
    if (cache->enum_set) {
    fprintf(stderr, "WATERMELON 4.2\n");
        if (cache->skip_checksums == opts->skip_checksums) {
    fprintf(stderr, "WATERMELON 4.3\n");
            // If we cached the value, return it now.
            goto done;
        }
    fprintf(stderr, "WATERMELON 4.4\n");
        (*env)->DeleteGlobalRef(env, cache->enum_set);
        cache->enum_set = NULL;
    }
    fprintf(stderr, "WATERMELON 4.5\n");
    if (opts->skip_checksums) {
    fprintf(stderr, "WATERMELON 4.6\n");
        jthr = fetchEnumInstance(env, READ_OPTION,
                  "SKIP_CHECKSUMS", &enumInst);
        if (jthr) {
            goto done;
        }
    fprintf(stderr, "WATERMELON 4.7\n");
        jthr = invokeMethod(env, &jVal, STATIC, NULL,
                "java/util/EnumSet", "of",
                "(Ljava/lang/Enum;)Ljava/util/EnumSet;", enumInst);
        if (jthr) {
            goto done;
        }
        enumSetObj = jVal.l;
    } else {
    fprintf(stderr, "WATERMELON 4.8\n");
        jclass clazz = (*env)->FindClass(env, READ_OPTION);
        if (!clazz) {
            jthr = newRuntimeError(env, "failed "
                    "to find class for %s", READ_OPTION);
            goto done;
        }
    fprintf(stderr, "WATERMELON 4.9\n");
        jthr = invokeMethod(env, &jVal, STATIC, NULL,
                "java/util/EnumSet", "noneOf",
                "(Ljava/lang/Class;)Ljava/util/EnumSet;", clazz);
        enumSetObj = jVal.l;
    }
    fprintf(stderr, "WATERMELON 4.95\n");
    // create global ref
    cache->enum_set = (*env)->NewGlobalRef(env, enumSetObj);
    if (!cache->enum_set) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    cache->skip_checksums = opts->skip_checksums;
    jthr = NULL;
done:
    (*env)->DeleteLocalRef(env, enumInst);
    (*env)->DeleteLocalRef(env, enumSetObj);
    return jthr;
}

static jthrowable jni_rz_setup_bb_pool(JNIEnv *env,
        struct hadoopRzOptions *opts)
{
    jthrowable jthr = NULL;
    jobject pool_obj = NULL;
    struct jni_rz_options_cache *cache = opts->cache;

    if (!opts->pool_name) {
        if (cache->pool_obj) {
            (*env)->DeleteGlobalRef(env, cache->pool_obj);
            cache->pool_obj = NULL;
        }
        free(cache->pool_name);
        cache->pool_name = NULL;
        goto done;
    }
    if (cache->pool_obj) {
        if (!strcmp(cache->pool_name, opts->pool_name)) {
            // If we cached the value, we can just use the existing value.
            goto done;
        }
        (*env)->DeleteGlobalRef(env, cache->pool_obj);
        cache->pool_obj = NULL;
        free(cache->pool_name);
    }
    cache->pool_name = strdup(opts->pool_name);
    if (!cache->pool_name) {
        jthr = newRuntimeError(env, "jni_rz_setup_bb_pool: memory "
                               "allocation failed.");
        goto done;
    }
    jthr = constructNewObjectOfClass(env, &pool_obj, opts->pool_name, "()V");
    if (jthr) {
        fprintf(stderr, "jni_rz_setup_bb_pool(pool_name=%s): failed "
            "to construct ByteBufferPool class.", opts->pool_name);
        goto done;
    }
    cache->pool_obj = (*env)->NewGlobalRef(env, pool_obj);
    if (!cache->pool_obj) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    jthr = NULL;
done:
    (*env)->DeleteLocalRef(env, pool_obj);
    if (jthr) {
        free(cache->pool_name);
        cache->pool_name = NULL;
    }
    return jthr;
}

static int jni_rz_extract_buffer(JNIEnv *env,
        const struct hadoopRzOptions *opts, struct jni_rz_buffer *buf)
{
    int ret;
    jthrowable jthr;
    jvalue jVal;
    uint8_t *direct_start;
    void *malloc_buf = NULL;
    jint position;
    jarray array = NULL;

    jthr = invokeMethod(env, &jVal, INSTANCE, buf->byte_buffer,
                     "java/nio/ByteBuffer", "remaining", "()I");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "jni_rz_extract_buffer: ByteBuffer#remaining failed: ");
        goto done;
    }
    buf->base.length = jVal.i;
    jthr = invokeMethod(env, &jVal, INSTANCE, buf->byte_buffer,
                     "java/nio/ByteBuffer", "position", "()I");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "jni_rz_extract_buffer: ByteBuffer#position failed: ");
        goto done;
    }
    position = jVal.i;
    direct_start = (*env)->GetDirectBufferAddress(env, buf->byte_buffer);
    if (direct_start) {
        // Handle direct buffers.
        buf->base.ptr = direct_start + position;
        buf->direct = 1;
        ret = 0;
        goto done;
    }
    // Handle indirect buffers.
    // The JNI docs don't say that GetDirectBufferAddress throws any exceptions
    // when it fails.  However, they also don't clearly say that it doesn't.  It
    // seems safest to clear any pending exceptions here, to prevent problems on
    // various JVMs.
    (*env)->ExceptionClear(env);
    if (!opts->pool_name) {
        fputs("jni_rz_extract_buffer: we read through the "
                "zero-copy path, but failed to get the address of the buffer via "
                "GetDirectBufferAddress.  Please make sure your JVM supports "
                "GetDirectBufferAddress.\n", stderr);
        ret = ENOTSUP;
        goto done;
    }
    // Get the backing array object of this buffer.
    jthr = invokeMethod(env, &jVal, INSTANCE, buf->byte_buffer,
                     "java/nio/ByteBuffer", "array", "()[B");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "jni_rz_extract_buffer: ByteBuffer#array failed: ");
        goto done;
    }
    array = jVal.l;
    if (!array) {
        fputs("jni_rz_extract_buffer: ByteBuffer#array returned NULL.",
              stderr);
        ret = EIO;
        goto done;
    }
    malloc_buf = malloc(buf->base.length);
    if (!malloc_buf) {
        fprintf(stderr, "jni_rz_extract_buffer: failed to allocate %d "
                "bytes of memory\n", buf->base.length);
        ret = ENOMEM;
        goto done;
    }
    (*env)->GetByteArrayRegion(env, array, position,
            buf->base.length, malloc_buf);
    jthr = (*env)->ExceptionOccurred(env);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "jni_rz_extract_buffer: GetByteArrayRegion failed: ");
        goto done;
    }
    buf->base.ptr = malloc_buf;
    buf->direct = 0;
    ret = 0;

done:
    free(malloc_buf);
    (*env)->DeleteLocalRef(env, array);
    return ret;
}

static int translate_zcr_exception(JNIEnv *env, jthrowable exc)
{
    int ret;
    char *className = NULL;
    jthrowable jthr = classNameOfObject(exc, env, &className);

    if (jthr) {
        fputs("jni_read_zero: failed to get class name of "
                "exception from read().\n", stderr);
        destroyLocalReference(env, exc);
        destroyLocalReference(env, jthr);
        ret = EIO;
        goto done;
    }
    if (!strcmp(className, "java.lang.UnsupportedOperationException")) {
        ret = EPROTONOSUPPORT;
        goto done;
    }
    ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_read_zero failed");
done:
    free(className);
    return ret;
}

static struct hadoopRzBuffer* jni_read_zero(hdfsFile bfile,
            struct hadoopRzOptions *opts, int32_t maxLength)
{
    JNIEnv *env;
    struct jni_file *file = (struct jni_file*)bfile;
    jthrowable jthr = NULL;
    jvalue jVal;
    jobject byteBuffer = NULL;
    struct jni_rz_buffer *buf = NULL;
    int ret;
    struct jni_rz_options_cache *cache;

    env = getJNIEnv();
    if (!env) {
        errno = EINTERNAL;
        return NULL;
    }
    if (file->type != INPUT) {
        fputs("Cannot read from a non-InputStream object!\n", stderr);
        ret = EINVAL;
        goto done;
    }
    buf = calloc(1, sizeof(*buf));
    if (!buf) {
        ret = ENOMEM;
        goto done;
    }
    fprintf(stderr, "WATERMELON 2\n");
    jthr = jni_rz_setup_cache(env, opts);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "jni_read_zero: jni_rz_setup_cache failed: ");
        goto done;
    }
    fprintf(stderr, "WATERMELON 3\n");
    jthr = jni_rz_setup_enumset(env, opts);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "jni_read_zero: jni_rz_setup_enumset failed: ");
        goto done;
    }
    fprintf(stderr, "WATERMELON 5\n");
    jthr = jni_rz_setup_bb_pool(env, opts);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "jni_read_zero: jni_rz_setup_enumset failed: ");
        goto done;
    }
    fprintf(stderr, "WATERMELON 5.1\n");
    cache = opts->cache;
    jthr = invokeMethod(env, &jVal, INSTANCE, file->stream, HADOOP_ISTRM,
        "read", "(Lorg/apache/hadoop/io/ByteBufferPool;ILjava/util/EnumSet;)"
        "Ljava/nio/ByteBuffer;", cache->pool_obj, maxLength, cache->enum_set);
    if (jthr) {
        ret = translate_zcr_exception(env, jthr);
        goto done;
    }
    byteBuffer = jVal.l;
    if (!byteBuffer) {
        buf->base.ptr = NULL;
        buf->base.length = 0;
        buf->byte_buffer = NULL;
        buf->direct = 0;
    } else {
        buf->byte_buffer = (*env)->NewGlobalRef(env, byteBuffer);
        if (!buf->byte_buffer) {
            ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                "jni_read_zero: failed to create global ref to ByteBuffer");
            goto done;
        }
        ret = jni_rz_extract_buffer(env, opts, buf);
        if (ret) {
            goto done;
        }
    }
    ret = 0;
done:
    (*env)->DeleteLocalRef(env, byteBuffer);
    if (ret) {
        if (buf) {
            if (buf->byte_buffer) {
                (*env)->DeleteGlobalRef(env, buf->byte_buffer);
            }
            free(buf);
        }
        errno = ret;
        return NULL;
    } else {
        errno = 0;
    }
    return (struct hadoopRzBuffer*)buf;
}

static void jni_rz_buffer_free(hdfsFile bfile, struct hadoopRzBuffer *buffer)
{
    struct jni_file *file = (struct jni_file *)bfile;
    struct jni_rz_buffer *buf = (struct jni_rz_buffer *)buffer;
    jvalue jVal;
    jthrowable jthr;
    JNIEnv* env = getJNIEnv();
    if (!env) {
        fprintf(stderr, "jni_rz_buffer_free: failed to get a "
                "thread-local JNI env");
        return;
    }
    if (buf->byte_buffer) {
        jthr = invokeMethod(env, &jVal, INSTANCE, file->stream,
                    HADOOP_ISTRM, "releaseBuffer",
                    "(Ljava/nio/ByteBuffer;)V", buf->byte_buffer);
        if (jthr) {
            printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "hadoopRzBufferFree: releaseBuffer failed: ");
            // even on error, we have to delete the reference.
        }
        (*env)->DeleteGlobalRef(env, buf->byte_buffer);
    }
    if (!buf->direct) {
        free(buf->base.ptr);
    }
    memset(buf, 0, sizeof(*buf));
    free(buf);
}

char***
jni_get_hosts(hdfsFS bfs, const char* path, tOffset start, tOffset length)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;

    // JAVA EQUIVALENT:
    //  fs.getFileBlockLoctions(new Path(path), start, length);
    jthrowable jthr;
    jobject jPath = NULL;
    jobject jFileStatus = NULL;
    jvalue jFSVal, jVal;
    jobjectArray jBlockLocations = NULL, jFileBlockHosts = NULL;
    jstring jHost = NULL;
    char*** blockHosts = NULL;
    int i, j, ret;
    jsize jNumFileBlocks = 0;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_get_hosts(path=%s): constructNewObjectOfPath", path);
        goto done;
    }
    jthr = invokeMethod(env, &jFSVal, INSTANCE, fs->obj,
            HADOOP_FS, "getFileStatus", "(Lorg/apache/hadoop/fs/Path;)"
            "Lorg/apache/hadoop/fs/FileStatus;", jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, NOPRINT_EXC_FILE_NOT_FOUND,
                "jni_get_hosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "FileSystem#getFileStatus", path, start, length);
        destroyLocalReference(env, jPath);
        goto done;
    }
    jFileStatus = jFSVal.l;

    //org.apache.hadoop.fs.FileSystem#getFileBlockLocations
    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj,
                     HADOOP_FS, "getFileBlockLocations", 
                     "(Lorg/apache/hadoop/fs/FileStatus;JJ)"
                     "[Lorg/apache/hadoop/fs/BlockLocation;",
                     jFileStatus, start, length);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "jni_get_hosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "FileSystem#getFileBlockLocations", path, start, length);
        goto done;
    }
    jBlockLocations = jVal.l;

    //Figure out no of entries in jBlockLocations
    //Allocate memory and add NULL at the end
    jNumFileBlocks = (*env)->GetArrayLength(env, jBlockLocations);

    blockHosts = calloc(jNumFileBlocks + 1, sizeof(char**));
    if (blockHosts == NULL) {
        ret = ENOMEM;
        goto done;
    }
    if (jNumFileBlocks == 0) {
        ret = 0;
        goto done;
    }

    //Now parse each block to get hostnames
    for (i = 0; i < jNumFileBlocks; ++i) {
        jobject jFileBlock =
            (*env)->GetObjectArrayElement(env, jBlockLocations, i);
        if (!jFileBlock) {
            ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                "jni_get_hosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "GetObjectArrayElement(%d)", path, start, length, i);
            goto done;
        }
        
        jthr = invokeMethod(env, &jVal, INSTANCE, jFileBlock, HADOOP_BLK_LOC,
                         "getHosts", "()[Ljava/lang/String;");
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "jni_get_hosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "BlockLocation#getHosts", path, start, length);
            goto done;
        }
        jFileBlockHosts = jVal.l;
        if (!jFileBlockHosts) {
            fprintf(stderr,
                "jni_get_hosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "BlockLocation#getHosts returned NULL", path, start, length);
            ret = EINTERNAL;
            goto done;
        }
        //Figure out no of hosts in jFileBlockHosts, and allocate the memory
        jsize jNumBlockHosts = (*env)->GetArrayLength(env, jFileBlockHosts);
        blockHosts[i] = calloc(jNumBlockHosts + 1, sizeof(char*));
        if (!blockHosts[i]) {
            ret = ENOMEM;
            goto done;
        }

        //Now parse each hostname
        const char *hostName;
        for (j = 0; j < jNumBlockHosts; ++j) {
            jHost = (*env)->GetObjectArrayElement(env, jFileBlockHosts, j);
            if (!jHost) {
                ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                    "jni_get_hosts(path=%s, start=%"PRId64", length=%"PRId64"): "
                    "NewByteArray", path, start, length);
                goto done;
            }
            hostName =
                (const char*)((*env)->GetStringUTFChars(env, jHost, NULL));
            if (!hostName) {
                ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                    "jni_get_hosts(path=%s, start=%"PRId64", length=%"PRId64", "
                    "j=%d out of %d): GetStringUTFChars",
                    path, start, length, j, jNumBlockHosts);
                goto done;
            }
            blockHosts[i][j] = strdup(hostName);
            (*env)->ReleaseStringUTFChars(env, jHost, hostName);
            if (!blockHosts[i][j]) {
                ret = ENOMEM;
                goto done;
            }
            destroyLocalReference(env, jHost);
            jHost = NULL;
        }

        destroyLocalReference(env, jFileBlockHosts);
        jFileBlockHosts = NULL;
    }
    ret = 0;

done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jFileStatus);
    destroyLocalReference(env, jBlockLocations);
    destroyLocalReference(env, jFileBlockHosts);
    destroyLocalReference(env, jHost);
    if (ret) {
        if (blockHosts) {
            hdfsFreeHosts(blockHosts);
        }
        return NULL;
    }

    return blockHosts;
}

static tOffset jni_get_default_block_size(hdfsFS bfs)
{
    // JAVA EQUIVALENT:
    //  fs.getDefaultBlockSize();
    jvalue jVal;
    jthrowable jthr;
    struct jni_fs *fs = (struct jni_fs*)bfs;
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS,
                     "getDefaultBlockSize", "()J");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_get_default_block_size: FileSystem#getDefaultBlockSize");
        return -1;
    }
    return jVal.j;
}

static tOffset jni_get_default_block_size_at_path(hdfsFS bfs, const char *path)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    // JAVA EQUIVALENT:
    //  fs.getDefaultBlockSize(path);

    jthrowable jthr;
    jobject jPath;
    tOffset blockSize;
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_get_default_block_size_at_path(path=%s): "
            "constructNewObjectOfPath", path);
        return -1;
    }
    jthr = getDefaultBlockSize(env, fs->obj, jPath, &blockSize);
    (*env)->DeleteLocalRef(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_get_default_block_size_at_path(path=%s): "
            "FileSystem#getDefaultBlockSize", path);
        return -1;
    }
    return blockSize;
}

static tOffset jni_get_capacity(hdfsFS bfs)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    // JAVA EQUIVALENT:
    //  FsStatus fss = fs.getStatus();
    //  return Fss.getCapacity();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //FileSystem#getStatus
    jvalue  jVal;
    jthrowable jthr;
    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS,
                     "getStatus", "()Lorg/apache/hadoop/fs/FsStatus;");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_get_capacity: FileSystem#getStatus");
        return -1;
    }
    jobject fss = (jobject)jVal.l;
    jthr = invokeMethod(env, &jVal, INSTANCE, fss, HADOOP_FSSTATUS,
                     "getCapacity", "()J");
    destroyLocalReference(env, fss);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_get_capacity: FsStatus#getCapacity");
        return -1;
    }
    return jVal.j;
}

static jthrowable
getFileInfoFromStat(JNIEnv *env, jobject jStat, hdfsFileInfo *fileInfo)
{
    jvalue jVal;
    jthrowable jthr;
    jobject jPath = NULL;
    jstring jPathName = NULL;
    jstring jUserName = NULL;
    jstring jGroupName = NULL;
    jobject jPermission = NULL;

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                     HADOOP_STAT, "isDir", "()Z");
    if (jthr)
        goto done;
    fileInfo->mKind = jVal.z ? kObjectKindDirectory : kObjectKindFile;

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                     HADOOP_STAT, "getReplication", "()S");
    if (jthr)
        goto done;
    fileInfo->mReplication = jVal.s;

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                     HADOOP_STAT, "getBlockSize", "()J");
    if (jthr)
        goto done;
    fileInfo->mBlockSize = jVal.j;

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                     HADOOP_STAT, "getModificationTime", "()J");
    if (jthr)
        goto done;
    fileInfo->mLastMod = jVal.j / 1000;

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                     HADOOP_STAT, "getAccessTime", "()J");
    if (jthr)
        goto done;
    fileInfo->mLastAccess = (tTime) (jVal.j / 1000);

    if (fileInfo->mKind == kObjectKindFile) {
        jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                         HADOOP_STAT, "getLen", "()J");
        if (jthr)
            goto done;
        fileInfo->mSize = jVal.j;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat, HADOOP_STAT,
                     "getPath", "()Lorg/apache/hadoop/fs/Path;");
    if (jthr)
        goto done;
    jPath = jVal.l;
    if (jPath == NULL) {
        jthr = newRuntimeError(env, "org.apache.hadoop.fs.FileStatus#"
            "getPath returned NULL!");
        goto done;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jPath, HADOOP_PATH,
                     "toString", "()Ljava/lang/String;");
    if (jthr)
        goto done;
    jPathName = jVal.l;
    const char *cPathName = 
        (const char*) ((*env)->GetStringUTFChars(env, jPathName, NULL));
    if (!cPathName) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    fileInfo->mName = strdup(cPathName);
    (*env)->ReleaseStringUTFChars(env, jPathName, cPathName);
    jthr = invokeMethod(env, &jVal, INSTANCE, jStat, HADOOP_STAT,
                    "getOwner", "()Ljava/lang/String;");
    if (jthr)
        goto done;
    jUserName = jVal.l;
    const char* cUserName = 
        (const char*) ((*env)->GetStringUTFChars(env, jUserName, NULL));
    if (!cUserName) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    fileInfo->mOwner = strdup(cUserName);
    (*env)->ReleaseStringUTFChars(env, jUserName, cUserName);

    const char* cGroupName;
    jthr = invokeMethod(env, &jVal, INSTANCE, jStat, HADOOP_STAT,
                    "getGroup", "()Ljava/lang/String;");
    if (jthr)
        goto done;
    jGroupName = jVal.l;
    cGroupName = (const char*) ((*env)->GetStringUTFChars(env, jGroupName, NULL));
    if (!cGroupName) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    fileInfo->mGroup = strdup(cGroupName);
    (*env)->ReleaseStringUTFChars(env, jGroupName, cGroupName);

    jthr = invokeMethod(env, &jVal, INSTANCE, jStat, HADOOP_STAT,
            "getPermission",
            "()Lorg/apache/hadoop/fs/permission/FsPermission;");
    if (jthr)
        goto done;
    if (jVal.l == NULL) {
        jthr = newRuntimeError(env, "%s#getPermission returned NULL!",
            HADOOP_STAT);
        goto done;
    }
    jPermission = jVal.l;
    jthr = invokeMethod(env, &jVal, INSTANCE, jPermission, HADOOP_FSPERM,
                         "toShort", "()S");
    if (jthr)
        goto done;
    fileInfo->mPermissions = jVal.s;
    jthr = NULL;

done:
    if (jthr)
        release_file_info_entry(fileInfo);
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathName);
    destroyLocalReference(env, jUserName);
    destroyLocalReference(env, jGroupName);
    destroyLocalReference(env, jPermission);
    destroyLocalReference(env, jPath);
    return jthr;
}

static jthrowable
getFileInfo(JNIEnv *env, jobject jFS, jobject jPath, hdfsFileInfo **fileInfo)
{
    // JAVA EQUIVALENT:
    //  fs.isDirectory(f)
    //  fs.getModificationTime()
    //  fs.getAccessTime()
    //  fs.getLength(f)
    //  f.getPath()
    //  f.getOwner()
    //  f.getGroup()
    //  f.getPermission().toShort()
    jobject jStat;
    jvalue  jVal;
    jthrowable jthr;

    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "exists", JMETHOD1(JPARAM(HADOOP_PATH), "Z"),
                     jPath);
    if (jthr)
        return jthr;
    if (jVal.z == 0) {
        *fileInfo = NULL;
        return NULL;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS,
            HADOOP_FS, "getFileStatus",
            JMETHOD1(JPARAM(HADOOP_PATH), JPARAM(HADOOP_STAT)), jPath);
    if (jthr)
        return jthr;
    jStat = jVal.l;
    *fileInfo = calloc(1, sizeof(hdfsFileInfo));
    if (!*fileInfo) {
        destroyLocalReference(env, jStat);
        return newRuntimeError(env, "getFileInfo: OOM allocating hdfsFileInfo");
    }
    jthr = getFileInfoFromStat(env, jStat, *fileInfo); 
    destroyLocalReference(env, jStat);
    return jthr;
}

static hdfsFileInfo *jni_get_path_info(hdfsFS bfs, const char* path)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    // JAVA EQUIVALENT:
    //  File f(path);
    //  fs.isDirectory(f)
    //  fs.lastModified() ??
    //  fs.getLength(f)
    //  f.getPath()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath;
    jthrowable jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_get_path_info(%s): constructNewObjectOfPath", path);
        return NULL;
    }
    hdfsFileInfo *fileInfo;
    jthr = getFileInfo(env, fs->obj, jPath, &fileInfo);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "jni_get_path_info(%s): getFileInfo", path);
        return NULL;
    }
    if (!fileInfo) {
        errno = ENOENT;
        return NULL;
    }
    return fileInfo;
}

static tOffset jni_get_used(hdfsFS bfs)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    // JAVA EQUIVALENT:
    //  FsStatus fss = fs.getStatus();
    //  return Fss.getUsed();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //FileSystem#getStatus
    jvalue  jVal;
    jthrowable jthr;
    jthr = invokeMethod(env, &jVal, INSTANCE, fs->obj, HADOOP_FS,
                     "getStatus", "()Lorg/apache/hadoop/fs/FsStatus;");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_get_used: FileSystem#getStatus");
        return -1;
    }
    jobject fss = (jobject)jVal.l;
    jthr = invokeMethod(env, &jVal, INSTANCE, fss, HADOOP_FSSTATUS,
                     "getUsed", "()J");
    destroyLocalReference(env, fss);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_get_used: FsStatus#getUsed");
        return -1;
    }
    return jVal.j;
}
 
int jni_chown(hdfsFS bfs, const char* path, const char *owner, const char *group)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    // JAVA EQUIVALENT:
    //  fs.setOwner(path, owner, group)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    if (owner == NULL && group == NULL) {
      return 0;
    }

    jobject jPath = NULL;
    jstring jOwner = NULL, jGroup = NULL;
    jthrowable jthr;
    int ret;

    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_chown(path=%s): constructNewObjectOfPath", path);
        goto done;
    }

    jthr = newJavaStr(env, owner, &jOwner); 
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_chown(path=%s): newJavaStr(%s)", path, owner);
        goto done;
    }
    jthr = newJavaStr(env, group, &jGroup);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_chown(path=%s): newJavaStr(%s)", path, group);
        goto done;
    }

    //Create the directory
    jthr = invokeMethod(env, NULL, INSTANCE, fs->obj, HADOOP_FS,
            "setOwner", JMETHOD3(JPARAM(HADOOP_PATH), 
                    JPARAM(JAVA_STRING), JPARAM(JAVA_STRING), JAVA_VOID),
            jPath, jOwner, jGroup);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "jni_chown(path=%s, owner=%s, group=%s): "
            "FileSystem#setOwner", path, owner, group);
        goto done;
    }
    ret = 0;

done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jOwner);
    destroyLocalReference(env, jGroup);

    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

int jni_chmod(hdfsFS bfs, const char* path, short mode)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;
    int ret;
    // JAVA EQUIVALENT:
    //  fs.setPermission(path, FsPermission)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jthrowable jthr;
    jobject jPath = NULL, jPermObj = NULL;

    // construct jPerm = FsPermission.createImmutable(short mode);
    jshort jmode = mode;
    jthr = constructNewObjectOfClass(env, &jPermObj,
                HADOOP_FSPERM,"(S)V",jmode);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "constructNewObjectOfClass(%s)", HADOOP_FSPERM);
        return -1;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_chmod(%s): constructNewObjectOfPath", path);
        goto done;
    }

    //Create the directory
    jthr = invokeMethod(env, NULL, INSTANCE, fs->obj, HADOOP_FS,
            "setPermission",
            JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_FSPERM), JAVA_VOID),
            jPath, jPermObj);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "jni_chmod(%s): FileSystem#setPermission", path);
        goto done;
    }
    ret = 0;

done:
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPermObj);

    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

static int jni_utime(hdfsFS bfs, const char* path, tTime mtime, tTime atime)
{
    struct jni_fs *fs = (struct jni_fs*)bfs;

    // JAVA EQUIVALENT:
    //  fs.setTimes(src, mtime, atime)
    jthrowable jthr;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath;
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "jni_utime(path=%s): constructNewObjectOfPath", path);
        return -1;
    }

    const tTime NO_CHANGE = -1;
    jlong jmtime = (mtime == NO_CHANGE) ? -1 : (mtime * (jlong)1000);
    jlong jatime = (atime == NO_CHANGE) ? -1 : (atime * (jlong)1000);

    jthr = invokeMethod(env, NULL, INSTANCE, fs->obj, HADOOP_FS,
            "setTimes", JMETHOD3(JPARAM(HADOOP_PATH), "J", "J", JAVA_VOID),
            jPath, jmtime, jatime);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "jni_utime(path=%s): FileSystem#setTimes", path);
        return -1;
    }
    return 0;
}

int jni_file_uses_direct_read(hdfsFile bfile)
{
    struct jni_file *file = (struct jni_file*)bfile;
    return !!(file->flags & HDFS_FILE_SUPPORTS_DIRECT_READ);
}

void jni_file_disable_direct_read(hdfsFile bfile)
{
    struct jni_file *file = (struct jni_file*)bfile;
    file->flags &= ~HDFS_FILE_SUPPORTS_DIRECT_READ;
}

const struct hadoop_fs_ops g_jni_ops = {
    .name = "jnifs",
    .file_is_open_for_read = jni_file_is_open_for_read,
    .file_is_open_for_write = jni_file_is_open_for_write,
    .get_read_statistics = jni_file_get_read_statistics,
    .connect = jni_connect,
    .disconnect = jni_disconnect,
    .open = jni_open_file,
    .close = jni_close_file,
    .exists = jni_file_exists,
    .seek = jni_seek,
    .tell = jni_tell,
    .read = jni_read,
    .pread = jni_pread,
    .write = jni_write,
    .flush = jni_flush,
    .hflush = jni_hflush,
    .hsync = jni_hsync,
    .available = jni_available,
    .copy = jni_copy,
    .move = jni_move,
    .unlink = jni_unlink,
    .rename = jni_rename,
    .get_working_directory = jni_get_working_directory,
    .set_working_directory = jni_set_working_directory,
    .mkdir = jni_mkdir,
    .set_replication = jni_set_replication,
    .list_directory = jni_list_directory,
    .get_path_info = jni_get_path_info,
    .get_hosts = jni_get_hosts,
    .get_default_block_size = jni_get_default_block_size,
    .get_default_block_size_at_path = jni_get_default_block_size_at_path,
    .get_capacity = jni_get_capacity,
    .get_used = jni_get_used,
    .chown = jni_chown,
    .chmod = jni_chmod,
    .utime = jni_utime,
    .read_zero = jni_read_zero,
    .rz_buffer_free = jni_rz_buffer_free,

    // test
    .file_uses_direct_read = jni_file_uses_direct_read,
    .file_disable_direct_read = jni_file_disable_direct_read,
};

// vim: ts=4:sw=4:et
