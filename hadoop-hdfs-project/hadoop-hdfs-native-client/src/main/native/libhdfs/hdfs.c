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
#include "hdfs/hdfs.h"
#include "jni_helper.h"
#include "platform.h"

#include <fcntl.h>
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

tSize readDirect(hdfsFS fs, hdfsFile f, void* buffer, tSize length);
static void hdfsFreeFileInfoEntry(hdfsFileInfo *hdfsFileInfo);

/**
 * The C equivalent of org.apache.org.hadoop.FSData(Input|Output)Stream .
 */
enum hdfsStreamType
{
    HDFS_STREAM_UNINITIALIZED = 0,
    HDFS_STREAM_INPUT = 1,
    HDFS_STREAM_OUTPUT = 2,
};

/**
 * The 'file-handle' to a file in hdfs.
 */
struct hdfsFile_internal {
    void* file;
    enum hdfsStreamType type;
    int flags;
};

#define HDFS_EXTENDED_FILE_INFO_ENCRYPTED 0x1

/**
 * Extended file information.
 */
struct hdfsExtendedFileInfo {
    int flags;
};

int hdfsFileIsOpenForRead(hdfsFile file)
{
    return (file->type == HDFS_STREAM_INPUT);
}

int hdfsGetHedgedReadMetrics(hdfsFS fs, struct hdfsHedgedReadMetrics **metrics)
{
    jthrowable jthr;
    jobject hedgedReadMetrics = NULL;
    jvalue jVal;
    struct hdfsHedgedReadMetrics *m = NULL;
    int ret;
    jobject jFS = (jobject)fs;
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jFS,
                  HADOOP_DFS,
                  "getHedgedReadMetrics",
                  "()Lorg/apache/hadoop/hdfs/DFSHedgedReadMetrics;");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetHedgedReadMetrics: getHedgedReadMetrics failed");
        goto done;
    }
    hedgedReadMetrics = jVal.l;

    m = malloc(sizeof(struct hdfsHedgedReadMetrics));
    if (!m) {
      ret = ENOMEM;
      goto done;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, hedgedReadMetrics,
                  "org/apache/hadoop/hdfs/DFSHedgedReadMetrics",
                  "getHedgedReadOps", "()J");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetHedgedReadStatistics: getHedgedReadOps failed");
        goto done;
    }
    m->hedgedReadOps = jVal.j;

    jthr = invokeMethod(env, &jVal, INSTANCE, hedgedReadMetrics,
                  "org/apache/hadoop/hdfs/DFSHedgedReadMetrics",
                  "getHedgedReadWins", "()J");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetHedgedReadStatistics: getHedgedReadWins failed");
        goto done;
    }
    m->hedgedReadOpsWin = jVal.j;

    jthr = invokeMethod(env, &jVal, INSTANCE, hedgedReadMetrics,
                  "org/apache/hadoop/hdfs/DFSHedgedReadMetrics",
                  "getHedgedReadOpsInCurThread", "()J");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetHedgedReadStatistics: getHedgedReadOpsInCurThread failed");
        goto done;
    }
    m->hedgedReadOpsInCurThread = jVal.j;

    *metrics = m;
    m = NULL;
    ret = 0;

done:
    destroyLocalReference(env, hedgedReadMetrics);
    free(m);
    if (ret) {
      errno = ret;
      return -1;
    }
    return 0;
}

void hdfsFreeHedgedReadMetrics(struct hdfsHedgedReadMetrics *metrics)
{
  free(metrics);
}

int hdfsFileGetReadStatistics(hdfsFile file,
                              struct hdfsReadStatistics **stats)
{
    jthrowable jthr;
    jobject readStats = NULL;
    jvalue jVal;
    struct hdfsReadStatistics *s = NULL;
    int ret;
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }
    if (file->type != HDFS_STREAM_INPUT) {
        ret = EINVAL;
        goto done;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, file->file, 
                  "org/apache/hadoop/hdfs/client/HdfsDataInputStream",
                  "getReadStatistics",
                  "()Lorg/apache/hadoop/hdfs/ReadStatistics;");
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
                  "org/apache/hadoop/hdfs/ReadStatistics",
                  "getTotalBytesRead", "()J");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsFileGetReadStatistics: getTotalBytesRead failed");
        goto done;
    }
    s->totalBytesRead = jVal.j;

    jthr = invokeMethod(env, &jVal, INSTANCE, readStats,
                  "org/apache/hadoop/hdfs/ReadStatistics",
                  "getTotalLocalBytesRead", "()J");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsFileGetReadStatistics: getTotalLocalBytesRead failed");
        goto done;
    }
    s->totalLocalBytesRead = jVal.j;

    jthr = invokeMethod(env, &jVal, INSTANCE, readStats,
                  "org/apache/hadoop/hdfs/ReadStatistics",
                  "getTotalShortCircuitBytesRead", "()J");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsFileGetReadStatistics: getTotalShortCircuitBytesRead failed");
        goto done;
    }
    s->totalShortCircuitBytesRead = jVal.j;
    jthr = invokeMethod(env, &jVal, INSTANCE, readStats,
                  "org/apache/hadoop/hdfs/ReadStatistics",
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

int64_t hdfsReadStatisticsGetRemoteBytesRead(
                            const struct hdfsReadStatistics *stats)
{
    return stats->totalBytesRead - stats->totalLocalBytesRead;
}

int hdfsFileClearReadStatistics(hdfsFile file)
{
    jthrowable jthr;
    int ret;
    JNIEnv* env = getJNIEnv();

    if (env == NULL) {
        errno = EINTERNAL;
        return EINTERNAL;
    }
    if (file->type != HDFS_STREAM_INPUT) {
        ret = EINVAL;
        goto done;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, file->file,
                  "org/apache/hadoop/hdfs/client/HdfsDataInputStream",
                  "clearReadStatistics", "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsFileClearReadStatistics: clearReadStatistics failed");
        goto done;
    }
    ret = 0;
done:
    if (ret) {
        errno = ret;
        return ret;
    }
    return 0;
}

void hdfsFileFreeReadStatistics(struct hdfsReadStatistics *stats)
{
    free(stats);
}

int hdfsFileIsOpenForWrite(hdfsFile file)
{
    return (file->type == HDFS_STREAM_OUTPUT);
}

int hdfsFileUsesDirectRead(hdfsFile file)
{
    return !!(file->flags & HDFS_FILE_SUPPORTS_DIRECT_READ);
}

void hdfsFileDisableDirectRead(hdfsFile file)
{
    file->flags &= ~HDFS_FILE_SUPPORTS_DIRECT_READ;
}

int hdfsDisableDomainSocketSecurity(void)
{
    jthrowable jthr;
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }
    jthr = invokeMethod(env, NULL, STATIC, NULL,
            "org/apache/hadoop/net/unix/DomainSocket",
            "disableBindPathValidation", "()V");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "DomainSocket#disableBindPathValidation");
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

static jthrowable hadoopConfGetStr(JNIEnv *env, jobject jConfiguration,
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

int hdfsConfGetStr(const char *key, char **val)
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
            "hdfsConfGetStr(%s): new Configuration", key);
        goto done;
    }
    jthr = hadoopConfGetStr(env, jConfiguration, key, val);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsConfGetStr(%s): hadoopConfGetStr", key);
        goto done;
    }
    ret = 0;
done:
    destroyLocalReference(env, jConfiguration);
    if (ret)
        errno = ret;
    return ret;
}

void hdfsConfStrFree(char *val)
{
    free(val);
}

static jthrowable hadoopConfGetInt(JNIEnv *env, jobject jConfiguration,
        const char *key, int32_t *val)
{
    jthrowable jthr = NULL;
    jvalue jVal;
    jstring jkey = NULL;

    jthr = newJavaStr(env, key, &jkey);
    if (jthr)
        return jthr;
    jthr = invokeMethod(env, &jVal, INSTANCE, jConfiguration,
            HADOOP_CONF, "getInt", JMETHOD2(JPARAM(JAVA_STRING), "I", "I"),
            jkey, (jint)(*val));
    destroyLocalReference(env, jkey);
    if (jthr)
        return jthr;
    *val = jVal.i;
    return NULL;
}

int hdfsConfGetInt(const char *key, int32_t *val)
{
    JNIEnv *env;
    int ret;
    jobject jConfiguration = NULL;
    jthrowable jthr;

    env = getJNIEnv();
    if (env == NULL) {
      ret = EINTERNAL;
      goto done;
    }
    jthr = constructNewObjectOfClass(env, &jConfiguration, HADOOP_CONF, "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsConfGetInt(%s): new Configuration", key);
        goto done;
    }
    jthr = hadoopConfGetInt(env, jConfiguration, key, val);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsConfGetInt(%s): hadoopConfGetInt", key);
        goto done;
    }
    ret = 0;
done:
    destroyLocalReference(env, jConfiguration);
    if (ret)
        errno = ret;
    return ret;
}

struct hdfsBuilderConfOpt {
    struct hdfsBuilderConfOpt *next;
    const char *key;
    const char *val;
};

struct hdfsBuilder {
    int forceNewInstance;
    const char *nn;
    tPort port;
    const char *kerbTicketCachePath;
    const char *userName;
    struct hdfsBuilderConfOpt *opts;
};

struct hdfsBuilder *hdfsNewBuilder(void)
{
    struct hdfsBuilder *bld = calloc(1, sizeof(struct hdfsBuilder));
    if (!bld) {
        errno = ENOMEM;
        return NULL;
    }
    return bld;
}

int hdfsBuilderConfSetStr(struct hdfsBuilder *bld, const char *key,
                          const char *val)
{
    struct hdfsBuilderConfOpt *opt, *next;
    
    opt = calloc(1, sizeof(struct hdfsBuilderConfOpt));
    if (!opt)
        return -ENOMEM;
    next = bld->opts;
    bld->opts = opt;
    opt->next = next;
    opt->key = key;
    opt->val = val;
    return 0;
}

void hdfsFreeBuilder(struct hdfsBuilder *bld)
{
    struct hdfsBuilderConfOpt *cur, *next;

    cur = bld->opts;
    for (cur = bld->opts; cur; ) {
        next = cur->next;
        free(cur);
        cur = next;
    }
    free(bld);
}

void hdfsBuilderSetForceNewInstance(struct hdfsBuilder *bld)
{
    bld->forceNewInstance = 1;
}

void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn)
{
    bld->nn = nn;
}

void hdfsBuilderSetNameNodePort(struct hdfsBuilder *bld, tPort port)
{
    bld->port = port;
}

void hdfsBuilderSetUserName(struct hdfsBuilder *bld, const char *userName)
{
    bld->userName = userName;
}

void hdfsBuilderSetKerbTicketCachePath(struct hdfsBuilder *bld,
                                       const char *kerbTicketCachePath)
{
    bld->kerbTicketCachePath = kerbTicketCachePath;
}

hdfsFS hdfsConnect(const char *host, tPort port)
{
    struct hdfsBuilder *bld = hdfsNewBuilder();
    if (!bld)
        return NULL;
    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    return hdfsBuilderConnect(bld);
}

/** Always return a new FileSystem handle */
hdfsFS hdfsConnectNewInstance(const char *host, tPort port)
{
    struct hdfsBuilder *bld = hdfsNewBuilder();
    if (!bld)
        return NULL;
    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderSetForceNewInstance(bld);
    return hdfsBuilderConnect(bld);
}

hdfsFS hdfsConnectAsUser(const char *host, tPort port, const char *user)
{
    struct hdfsBuilder *bld = hdfsNewBuilder();
    if (!bld)
        return NULL;
    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderSetUserName(bld, user);
    return hdfsBuilderConnect(bld);
}

/** Always return a new FileSystem handle */
hdfsFS hdfsConnectAsUserNewInstance(const char *host, tPort port,
        const char *user)
{
    struct hdfsBuilder *bld = hdfsNewBuilder();
    if (!bld)
        return NULL;
    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderSetForceNewInstance(bld);
    hdfsBuilderSetUserName(bld, user);
    return hdfsBuilderConnect(bld);
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
        lastColon = strrchr(bld->nn, ':');
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
    snprintf(buf, bufLen, "forceNewInstance=%d, nn=%s, port=%d, "
             "kerbTicketCachePath=%s, userName=%s",
             bld->forceNewInstance, maybeNull(bld->nn), bld->port,
             maybeNull(bld->kerbTicketCachePath), maybeNull(bld->userName));
    return buf;
}

hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld)
{
    JNIEnv *env = 0;
    jobject jConfiguration = NULL, jFS = NULL, jURI = NULL, jCachePath = NULL;
    jstring jURIString = NULL, jUserString = NULL;
    jvalue  jVal;
    jthrowable jthr = NULL;
    char *cURI = 0, buf[512];
    int ret;
    jobject jRet = NULL;
    struct hdfsBuilderConfOpt *opt;

    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();
    if (env == NULL) {
        ret = EINTERNAL;
        goto done;
    }

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
        if (bld->forceNewInstance) {
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
            jFS = jVal.l;
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
            jFS = jVal.l;
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
        if (bld->forceNewInstance) {
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
            jFS = jVal.l;
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
            jFS = jVal.l;
        }
    }
    jRet = (*env)->NewGlobalRef(env, jFS);
    if (!jRet) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                    "hdfsBuilderConnect(%s)",
                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
        goto done;
    }
    ret = 0;

done:
    // Release unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jFS);
    destroyLocalReference(env, jURI);
    destroyLocalReference(env, jCachePath);
    destroyLocalReference(env, jURIString);
    destroyLocalReference(env, jUserString);
    free(cURI);
    hdfsFreeBuilder(bld);

    if (ret) {
        errno = ret;
        return NULL;
    }
    return (hdfsFS)jRet;
}

int hdfsDisconnect(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.close()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    int ret;
    jobject jFS;
    jthrowable jthr;

    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    jFS = (jobject)fs;

    //Sanity check
    if (fs == NULL) {
        errno = EBADF;
        return -1;
    }

    jthr = invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
                     "close", "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsDisconnect: FileSystem#close");
    } else {
        ret = 0;
    }
    (*env)->DeleteGlobalRef(env, jFS);
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

hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags,
                      int bufferSize, short replication, tSize blockSize)
{
    struct hdfsStreamBuilder *bld = hdfsStreamBuilderAlloc(fs, path, flags);
    if (bufferSize != 0) {
      hdfsStreamBuilderSetBufferSize(bld, bufferSize);
    }
    if (replication != 0) {
      hdfsStreamBuilderSetReplication(bld, replication);
    }
    if (blockSize != 0) {
      hdfsStreamBuilderSetDefaultBlockSize(bld, blockSize);
    }
    return hdfsStreamBuilderBuild(bld);
}

struct hdfsStreamBuilder {
    hdfsFS fs;
    int flags;
    int32_t bufferSize;
    int16_t replication;
    int64_t defaultBlockSize;
    char path[1];
};

struct hdfsStreamBuilder *hdfsStreamBuilderAlloc(hdfsFS fs,
                                            const char *path, int flags)
{
    int path_len = strlen(path);
    struct hdfsStreamBuilder *bld;

    // sizeof(hdfsStreamBuilder->path) includes one byte for the string
    // terminator
    bld = malloc(sizeof(struct hdfsStreamBuilder) + path_len);
    if (!bld) {
        errno = ENOMEM;
        return NULL;
    }
    bld->fs = fs;
    bld->flags = flags;
    bld->bufferSize = 0;
    bld->replication = 0;
    bld->defaultBlockSize = 0;
    memcpy(bld->path, path, path_len);
    bld->path[path_len] = '\0';
    return bld;
}

void hdfsStreamBuilderFree(struct hdfsStreamBuilder *bld)
{
    free(bld);
}

int hdfsStreamBuilderSetBufferSize(struct hdfsStreamBuilder *bld,
                                   int32_t bufferSize)
{
    if ((bld->flags & O_ACCMODE) != O_WRONLY) {
        errno = EINVAL;
        return -1;
    }
    bld->bufferSize = bufferSize;
    return 0;
}

int hdfsStreamBuilderSetReplication(struct hdfsStreamBuilder *bld,
                                    int16_t replication)
{
    if ((bld->flags & O_ACCMODE) != O_WRONLY) {
        errno = EINVAL;
        return -1;
    }
    bld->replication = replication;
    return 0;
}

int hdfsStreamBuilderSetDefaultBlockSize(struct hdfsStreamBuilder *bld,
                                         int64_t defaultBlockSize)
{
    if ((bld->flags & O_ACCMODE) != O_WRONLY) {
        errno = EINVAL;
        return -1;
    }
    bld->defaultBlockSize = defaultBlockSize;
    return 0;
}

static hdfsFile hdfsOpenFileImpl(hdfsFS fs, const char *path, int flags,
                  int32_t bufferSize, int16_t replication, int64_t blockSize)
{
    /*
      JAVA EQUIVALENT:
       File f = new File(path);
       FSData{Input|Output}Stream f{is|os} = fs.create(f);
       return f{is|os};
    */
    int accmode = flags & O_ACCMODE;
    jstring jStrBufferSize = NULL, jStrReplication = NULL, jCapabilityString = NULL;
    jobject jConfiguration = NULL, jPath = NULL, jFile = NULL;
    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jvalue jVal;
    hdfsFile file = NULL;
    int ret;
    jint jBufferSize = bufferSize;
    jshort jReplication = replication;

    /* The hadoop java api/signature */
    const char *method = NULL;
    const char *signature = NULL;

    /* Get the JNIEnv* corresponding to current thread */
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }


    if (accmode == O_RDONLY || accmode == O_WRONLY) {
	/* yay */
    } else if (accmode == O_RDWR) {
      fprintf(stderr, "ERROR: cannot open an hdfs file in O_RDWR mode\n");
      errno = ENOTSUP;
      return NULL;
    } else {
      fprintf(stderr, "ERROR: cannot open an hdfs file in mode 0x%x\n", accmode);
      errno = EINVAL;
      return NULL;
    }

    if ((flags & O_CREAT) && (flags & O_EXCL)) {
      fprintf(stderr, "WARN: hdfs does not truly support O_CREATE && O_EXCL\n");
    }

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
            "hdfsOpenFile(%s): constructNewObjectOfPath", path);
        goto done;
    }

    /* Get the Configuration object from the FileSystem object */
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "getConf", JMETHOD1("", JPARAM(HADOOP_CONF)));
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsOpenFile(%s): FileSystem#getConf", path);
        goto done;
    }
    jConfiguration = jVal.l;

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
                "hdfsOpenFile(%s): Configuration#getInt(io.file.buffer.size)",
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
                    "hdfsOpenFile(%s): Configuration#getInt(dfs.replication)",
                    path);
                goto done;
            }
            jReplication = (jshort)jVal.i;
        }
    }
 
    /* Create and return either the FSDataInputStream or
       FSDataOutputStream references jobject jStream */

    // READ?
    if (accmode == O_RDONLY) {
        jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                       method, signature, jPath, jBufferSize);
    }  else if ((accmode == O_WRONLY) && (flags & O_APPEND)) {
        // WRITE/APPEND?
       jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                       method, signature, jPath);
    } else {
        // WRITE/CREATE
        jboolean jOverWrite = 1;
        jlong jBlockSize = blockSize;

        if (jBlockSize == 0) {
            jthr = getDefaultBlockSize(env, jFS, jPath, &jBlockSize);
            if (jthr) {
                ret = EIO;
                goto done;
            }
        }
        jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                         method, signature, jPath, jOverWrite,
                         jBufferSize, jReplication, jBlockSize);
    }
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsOpenFile(%s): FileSystem#%s(%s)", path, method, signature);
        goto done;
    }
    jFile = jVal.l;

    file = calloc(1, sizeof(struct hdfsFile_internal));
    if (!file) {
        fprintf(stderr, "hdfsOpenFile(%s): OOM create hdfsFile\n", path);
        ret = ENOMEM;
        goto done;
    }
    file->file = (*env)->NewGlobalRef(env, jFile);
    if (!file->file) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "hdfsOpenFile(%s): NewGlobalRef", path); 
        goto done;
    }
    file->type = (((flags & O_WRONLY) == 0) ? HDFS_STREAM_INPUT :
        HDFS_STREAM_OUTPUT);
    file->flags = 0;

    if ((flags & O_WRONLY) == 0) {
        // Check the StreamCapabilities of jFile to see if we can do direct reads
        jthr = newJavaStr(env, "in:readbytebuffer", &jCapabilityString);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                        "hdfsOpenFile(%s): newJavaStr", path);
            goto done;
        }
        jthr = invokeMethod(env, &jVal, INSTANCE, jFile, HADOOP_ISTRM,
                            "hasCapability", "(Ljava/lang/String;)Z", jCapabilityString);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                        "hdfsOpenFile(%s): FSDataInputStream#hasCapability", path);
            goto done;
        }
        if (jVal.z) {
            file->flags |= HDFS_FILE_SUPPORTS_DIRECT_READ;
        }
    }
    ret = 0;

done:
    destroyLocalReference(env, jStrBufferSize);
    destroyLocalReference(env, jStrReplication);
    destroyLocalReference(env, jConfiguration); 
    destroyLocalReference(env, jPath); 
    destroyLocalReference(env, jFile);
    destroyLocalReference(env, jCapabilityString);
    if (ret) {
        if (file) {
            if (file->file) {
                (*env)->DeleteGlobalRef(env, file->file);
            }
            free(file);
        }
        errno = ret;
        return NULL;
    }
    return file;
}

hdfsFile hdfsStreamBuilderBuild(struct hdfsStreamBuilder *bld)
{
    hdfsFile file = hdfsOpenFileImpl(bld->fs, bld->path, bld->flags,
                  bld->bufferSize, bld->replication, bld->defaultBlockSize);
    int prevErrno = errno;
    hdfsStreamBuilderFree(bld);
    errno = prevErrno;
    return file;
}

int hdfsTruncateFile(hdfsFS fs, const char* path, tOffset newlength)
{
    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jvalue jVal;
    jobject jPath = NULL;

    JNIEnv *env = getJNIEnv();

    if (!env) {
        errno = EINTERNAL;
        return -1;
    }

    /* Create an object of org.apache.hadoop.fs.Path */
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsTruncateFile(%s): constructNewObjectOfPath", path);
        return -1;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                        "truncate", JMETHOD2(JPARAM(HADOOP_PATH), "J", "Z"),
                        jPath, newlength);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsTruncateFile(%s): FileSystem#truncate", path);
        return -1;
    }
    if (jVal.z == JNI_TRUE) {
        return 1;
    }
    return 0;
}

int hdfsUnbufferFile(hdfsFile file)
{
    int ret;
    jthrowable jthr;
    JNIEnv *env = getJNIEnv();

    if (!env) {
        ret = EINTERNAL;
        goto done;
    }
    if (file->type != HDFS_STREAM_INPUT) {
        ret = ENOTSUP;
        goto done;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, file->file, HADOOP_ISTRM,
                     "unbuffer", "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                HADOOP_ISTRM "#unbuffer failed:");
        goto done;
    }
    ret = 0;

done:
    errno = ret;
    return ret;
}

int hdfsCloseFile(hdfsFS fs, hdfsFile file)
{
    int ret;
    // JAVA EQUIVALENT:
    //  file.close 

    //The interface whose 'close' method to be called
    const char *interface;
    const char *interfaceShortName;

    //Caught exception
    jthrowable jthr;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }

    //Sanity check
    if (!file || file->type == HDFS_STREAM_UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    interface = (file->type == HDFS_STREAM_INPUT) ?
        HADOOP_ISTRM : HADOOP_OSTRM;
  
    jthr = invokeMethod(env, NULL, INSTANCE, file->file, interface,
                     "close", "()V");
    if (jthr) {
        interfaceShortName = (file->type == HDFS_STREAM_INPUT) ? 
            "FSDataInputStream" : "FSDataOutputStream";
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "%s#close", interfaceShortName);
    } else {
        ret = 0;
    }

    //De-allocate memory
    (*env)->DeleteGlobalRef(env, file->file);
    free(file);

    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

int hdfsExists(hdfsFS fs, const char *path)
{
    JNIEnv *env = getJNIEnv();
    jobject jPath;
    jvalue  jVal;
    jobject jFS = (jobject)fs;
    jthrowable jthr;

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
            "hdfsExists: constructNewObjectOfPath");
        return -1;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
            "exists", JMETHOD1(JPARAM(HADOOP_PATH), "Z"), jPath);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsExists: invokeMethod(%s)",
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

// Checks input file for readiness for reading.
static int readPrepare(JNIEnv* env, hdfsFS fs, hdfsFile f,
                       jobject* jInputStream)
{
    *jInputStream = (jobject)(f ? f->file : NULL);

    //Sanity check
    if (!f || f->type == HDFS_STREAM_UNINITIALIZED) {
      errno = EBADF;
      return -1;
    }

    //Error checking... make sure that this file is 'readable'
    if (f->type != HDFS_STREAM_INPUT) {
      fprintf(stderr, "Cannot read from a non-InputStream object!\n");
      errno = EINVAL;
      return -1;
    }

    return 0;
}

tSize hdfsRead(hdfsFS fs, hdfsFile f, void* buffer, tSize length)
{
    jobject jInputStream;
    jbyteArray jbRarray;
    jvalue jVal;
    jthrowable jthr;
    JNIEnv* env;

    if (length == 0) {
        return 0;
    } else if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    if (f->flags & HDFS_FILE_SUPPORTS_DIRECT_READ) {
      return readDirect(fs, f, buffer, length);
    }

    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(bR);

    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Parameters
    if (readPrepare(env, fs, f, &jInputStream) == -1) {
      return -1;
    }

    //Read the requisite bytes
    jbRarray = (*env)->NewByteArray(env, length);
    if (!jbRarray) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "hdfsRead: NewByteArray");
        return -1;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jInputStream, HADOOP_ISTRM,
                               "read", "([B)I", jbRarray);
    if (jthr) {
        destroyLocalReference(env, jbRarray);
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsRead: FSDataInputStream#read");
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
    // We only copy the portion of the jbRarray that was actually filled by
    // the call to FsDataInputStream#read; #read is not guaranteed to fill the
    // entire buffer, instead it returns the number of bytes read into the
    // buffer; we use the return value as the input in GetByteArrayRegion to
    // ensure don't copy more bytes than necessary
    (*env)->GetByteArrayRegion(env, jbRarray, 0, jVal.i, buffer);
    destroyLocalReference(env, jbRarray);
    if ((*env)->ExceptionCheck(env)) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "hdfsRead: GetByteArrayRegion");
        return -1;
    }
    return jVal.i;
}

// Reads using the read(ByteBuffer) API, which does fewer copies
tSize readDirect(hdfsFS fs, hdfsFile f, void* buffer, tSize length)
{
    // JAVA EQUIVALENT:
    //  ByteBuffer bbuffer = ByteBuffer.allocateDirect(length) // wraps C buffer
    //  fis.read(bbuffer);

    jobject jInputStream;
    jvalue jVal;
    jthrowable jthr;
    jobject bb;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    if (readPrepare(env, fs, f, &jInputStream) == -1) {
      return -1;
    }

    //Read the requisite bytes
    bb = (*env)->NewDirectByteBuffer(env, buffer, length);
    if (bb == NULL) {
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "readDirect: NewDirectByteBuffer");
        return -1;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jInputStream,
        HADOOP_ISTRM, "read", "(Ljava/nio/ByteBuffer;)I", bb);
    destroyLocalReference(env, bb);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "readDirect: FSDataInputStream#read");
        return -1;
    }
    return (jVal.i < 0) ? 0 : jVal.i;
}

tSize hdfsPread(hdfsFS fs, hdfsFile f, tOffset position,
                void* buffer, tSize length)
{
    JNIEnv* env;
    jbyteArray jbRarray;
    jvalue jVal;
    jthrowable jthr;

    if (length == 0) {
        return 0;
    } else if (length < 0) {
        errno = EINVAL;
        return -1;
    }
    if (!f || f->type == HDFS_STREAM_UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Error checking... make sure that this file is 'readable'
    if (f->type != HDFS_STREAM_INPUT) {
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
            "hdfsPread: NewByteArray");
        return -1;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, f->file, HADOOP_ISTRM,
                     "read", "(J[BII)I", position, jbRarray, 0, length);
    if (jthr) {
        destroyLocalReference(env, jbRarray);
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsPread: FSDataInputStream#read");
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
            "hdfsPread: GetByteArrayRegion");
        return -1;
    }
    return jVal.i;
}

tSize hdfsWrite(hdfsFS fs, hdfsFile f, const void* buffer, tSize length)
{
    // JAVA EQUIVALENT
    // byte b[] = str.getBytes();
    // fso.write(b);

    jobject jOutputStream;
    jbyteArray jbWarray;
    jthrowable jthr;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type == HDFS_STREAM_UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    jOutputStream = f->file;
    
    if (length < 0) {
    	errno = EINVAL;
    	return -1;
    }

    //Error checking... make sure that this file is 'writable'
    if (f->type != HDFS_STREAM_OUTPUT) {
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
            "hdfsWrite: NewByteArray");
        return -1;
    }
    (*env)->SetByteArrayRegion(env, jbWarray, 0, length, buffer);
    if ((*env)->ExceptionCheck(env)) {
        destroyLocalReference(env, jbWarray);
        errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "hdfsWrite(length = %d): SetByteArrayRegion", length);
        return -1;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, jOutputStream,
            HADOOP_OSTRM, "write", "([B)V", jbWarray);
    destroyLocalReference(env, jbWarray);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsWrite: FSDataOutputStream#write");
        return -1;
    }
    // Unlike most Java streams, FSDataOutputStream never does partial writes.
    // If we succeeded, all the data was written.
    return length;
}

int hdfsSeek(hdfsFS fs, hdfsFile f, tOffset desiredPos) 
{
    // JAVA EQUIVALENT
    //  fis.seek(pos);

    jobject jInputStream;
    jthrowable jthr;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type != HDFS_STREAM_INPUT) {
        errno = EBADF;
        return -1;
    }

    jInputStream = f->file;
    jthr = invokeMethod(env, NULL, INSTANCE, jInputStream,
            HADOOP_ISTRM, "seek", "(J)V", desiredPos);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsSeek(desiredPos=%" PRId64 ")"
            ": FSDataInputStream#seek", desiredPos);
        return -1;
    }
    return 0;
}



tOffset hdfsTell(hdfsFS fs, hdfsFile f)
{
    // JAVA EQUIVALENT
    //  pos = f.getPos();

    jobject jStream;
    const char *interface;
    jvalue jVal;
    jthrowable jthr;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type == HDFS_STREAM_UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //Parameters
    jStream = f->file;
    interface = (f->type == HDFS_STREAM_INPUT) ?
        HADOOP_ISTRM : HADOOP_OSTRM;
    jthr = invokeMethod(env, &jVal, INSTANCE, jStream,
                     interface, "getPos", "()J");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsTell: %s#getPos",
            ((f->type == HDFS_STREAM_INPUT) ? "FSDataInputStream" :
                                 "FSDataOutputStream"));
        return -1;
    }
    return jVal.j;
}

int hdfsFlush(hdfsFS fs, hdfsFile f) 
{
    // JAVA EQUIVALENT
    //  fos.flush();

    jthrowable jthr;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type != HDFS_STREAM_OUTPUT) {
        errno = EBADF;
        return -1;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, f->file,
                     HADOOP_OSTRM, "flush", "()V");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsFlush: FSDataInputStream#flush");
        return -1;
    }
    return 0;
}

int hdfsHFlush(hdfsFS fs, hdfsFile f)
{
    jobject jOutputStream;
    jthrowable jthr;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type != HDFS_STREAM_OUTPUT) {
        errno = EBADF;
        return -1;
    }

    jOutputStream = f->file;
    jthr = invokeMethod(env, NULL, INSTANCE, jOutputStream,
                     HADOOP_OSTRM, "hflush", "()V");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsHFlush: FSDataOutputStream#hflush");
        return -1;
    }
    return 0;
}

int hdfsHSync(hdfsFS fs, hdfsFile f)
{
    jobject jOutputStream;
    jthrowable jthr;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type != HDFS_STREAM_OUTPUT) {
        errno = EBADF;
        return -1;
    }

    jOutputStream = f->file;
    jthr = invokeMethod(env, NULL, INSTANCE, jOutputStream,
                     HADOOP_OSTRM, "hsync", "()V");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsHSync: FSDataOutputStream#hsync");
        return -1;
    }
    return 0;
}

int hdfsAvailable(hdfsFS fs, hdfsFile f)
{
    // JAVA EQUIVALENT
    //  fis.available();

    jobject jInputStream;
    jvalue jVal;
    jthrowable jthr;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Sanity check
    if (!f || f->type != HDFS_STREAM_INPUT) {
        errno = EBADF;
        return -1;
    }

    //Parameters
    jInputStream = f->file;
    jthr = invokeMethod(env, &jVal, INSTANCE, jInputStream,
                     HADOOP_ISTRM, "available", "()I");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsAvailable: FSDataInputStream#available");
        return -1;
    }
    return jVal.i;
}

static int hdfsCopyImpl(hdfsFS srcFS, const char *src, hdfsFS dstFS,
        const char *dst, jboolean deleteSource)
{
    //JAVA EQUIVALENT
    //  FileUtil#copy(srcFS, srcPath, dstFS, dstPath,
    //                 deleteSource = false, conf)

    //Parameters
    jobject jSrcFS = (jobject)srcFS;
    jobject jDstFS = (jobject)dstFS;
    jobject jConfiguration = NULL, jSrcPath = NULL, jDstPath = NULL;
    jthrowable jthr;
    jvalue jVal;
    int ret;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jthr = constructNewObjectOfPath(env, src, &jSrcPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsCopyImpl(src=%s): constructNewObjectOfPath", src);
        goto done;
    }
    jthr = constructNewObjectOfPath(env, dst, &jDstPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsCopyImpl(dst=%s): constructNewObjectOfPath", dst);
        goto done;
    }

    //Create the org.apache.hadoop.conf.Configuration object
    jthr = constructNewObjectOfClass(env, &jConfiguration,
                                     HADOOP_CONF, "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsCopyImpl: Configuration constructor");
        goto done;
    }

    //FileUtil#copy
    jthr = invokeMethod(env, &jVal, STATIC,
            NULL, "org/apache/hadoop/fs/FileUtil", "copy",
            "(Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;"
            "Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;"
            "ZLorg/apache/hadoop/conf/Configuration;)Z",
            jSrcFS, jSrcPath, jDstFS, jDstPath, deleteSource, 
            jConfiguration);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsCopyImpl(src=%s, dst=%s, deleteSource=%d): "
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

int hdfsCopy(hdfsFS srcFS, const char *src, hdfsFS dstFS, const char *dst)
{
    return hdfsCopyImpl(srcFS, src, dstFS, dst, 0);
}

int hdfsMove(hdfsFS srcFS, const char *src, hdfsFS dstFS, const char *dst)
{
    return hdfsCopyImpl(srcFS, src, dstFS, dst, 1);
}

int hdfsDelete(hdfsFS fs, const char *path, int recursive)
{
    // JAVA EQUIVALENT:
    //  Path p = new Path(path);
    //  bool retval = fs.delete(p, recursive);

    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jobject jPath;
    jvalue jVal;
    jboolean jRecursive;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsDelete(path=%s): constructNewObjectOfPath", path);
        return -1;
    }
    jRecursive = recursive ? JNI_TRUE : JNI_FALSE;
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "delete", "(Lorg/apache/hadoop/fs/Path;Z)Z",
                     jPath, jRecursive);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsDelete(path=%s, recursive=%d): "
            "FileSystem#delete", path, recursive);
        return -1;
    }
    if (!jVal.z) {
        errno = EIO;
        return -1;
    }
    return 0;
}



int hdfsRename(hdfsFS fs, const char *oldPath, const char *newPath)
{
    // JAVA EQUIVALENT:
    //  Path old = new Path(oldPath);
    //  Path new = new Path(newPath);
    //  fs.rename(old, new);

    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jobject jOldPath = NULL, jNewPath = NULL;
    int ret = -1;
    jvalue jVal;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    jthr = constructNewObjectOfPath(env, oldPath, &jOldPath );
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsRename: constructNewObjectOfPath(%s)", oldPath);
        goto done;
    }
    jthr = constructNewObjectOfPath(env, newPath, &jNewPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsRename: constructNewObjectOfPath(%s)", newPath);
        goto done;
    }

    // Rename the file
    // TODO: use rename2 here?  (See HDFS-3592)
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS, "rename",
                     JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_PATH), "Z"),
                     jOldPath, jNewPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsRename(oldPath=%s, newPath=%s): FileSystem#rename",
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



char* hdfsGetWorkingDirectory(hdfsFS fs, char* buffer, size_t bufferSize)
{
    // JAVA EQUIVALENT:
    //  Path p = fs.getWorkingDirectory(); 
    //  return p.toString()

    jobject jPath = NULL;
    jstring jPathString = NULL;
    jobject jFS = (jobject)fs;
    jvalue jVal;
    jthrowable jthr;
    int ret;
    const char *jPathChars = NULL;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    //FileSystem#getWorkingDirectory()
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS,
                     HADOOP_FS, "getWorkingDirectory",
                     "()Lorg/apache/hadoop/fs/Path;");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetWorkingDirectory: FileSystem#getWorkingDirectory");
        goto done;
    }
    jPath = jVal.l;
    if (!jPath) {
        fprintf(stderr, "hdfsGetWorkingDirectory: "
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
            "hdfsGetWorkingDirectory: Path#toString");
        goto done;
    }
    jPathString = jVal.l;
    jPathChars = (*env)->GetStringUTFChars(env, jPathString, NULL);
    if (!jPathChars) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "hdfsGetWorkingDirectory: GetStringUTFChars");
        goto done;
    }

    //Copy to user-provided buffer
    ret = snprintf(buffer, bufferSize, "%s", jPathChars);
    if (ret >= bufferSize) {
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



int hdfsSetWorkingDirectory(hdfsFS fs, const char *path)
{
    // JAVA EQUIVALENT:
    //  fs.setWorkingDirectory(Path(path)); 

    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jobject jPath;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsSetWorkingDirectory(%s): constructNewObjectOfPath",
            path);
        return -1;
    }

    //FileSystem#setWorkingDirectory()
    jthr = invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
                     "setWorkingDirectory", 
                     "(Lorg/apache/hadoop/fs/Path;)V", jPath);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, NOPRINT_EXC_ILLEGAL_ARGUMENT,
            "hdfsSetWorkingDirectory(%s): FileSystem#setWorkingDirectory",
            path);
        return -1;
    }
    return 0;
}



int hdfsCreateDirectory(hdfsFS fs, const char *path)
{
    // JAVA EQUIVALENT:
    //  fs.mkdirs(new Path(path));

    jobject jFS = (jobject)fs;
    jobject jPath;
    jthrowable jthr;
    jvalue jVal;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsCreateDirectory(%s): constructNewObjectOfPath", path);
        return -1;
    }

    //Create the directory
    jVal.z = 0;
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "mkdirs", "(Lorg/apache/hadoop/fs/Path;)Z",
                     jPath);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK | NOPRINT_EXC_PARENT_NOT_DIRECTORY,
            "hdfsCreateDirectory(%s): FileSystem#mkdirs", path);
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


int hdfsSetReplication(hdfsFS fs, const char *path, int16_t replication)
{
    // JAVA EQUIVALENT:
    //  fs.setReplication(new Path(path), replication);

    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jobject jPath;
    jvalue jVal;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsSetReplication(path=%s): constructNewObjectOfPath", path);
        return -1;
    }

    //Create the directory
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "setReplication", "(Lorg/apache/hadoop/fs/Path;S)Z",
                     jPath, replication);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsSetReplication(path=%s, replication=%d): "
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

int hdfsChown(hdfsFS fs, const char *path, const char *owner, const char *group)
{
    // JAVA EQUIVALENT:
    //  fs.setOwner(path, owner, group)

    jobject jFS = (jobject)fs;
    jobject jPath = NULL;
    jstring jOwner = NULL, jGroup = NULL;
    jthrowable jthr;
    int ret;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    if (owner == NULL && group == NULL) {
      return 0;
    }

    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsChown(path=%s): constructNewObjectOfPath", path);
        goto done;
    }

    jthr = newJavaStr(env, owner, &jOwner); 
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsChown(path=%s): newJavaStr(%s)", path, owner);
        goto done;
    }
    jthr = newJavaStr(env, group, &jGroup);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsChown(path=%s): newJavaStr(%s)", path, group);
        goto done;
    }

    //Create the directory
    jthr = invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
            "setOwner", JMETHOD3(JPARAM(HADOOP_PATH), 
                    JPARAM(JAVA_STRING), JPARAM(JAVA_STRING), JAVA_VOID),
            jPath, jOwner, jGroup);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "hdfsChown(path=%s, owner=%s, group=%s): "
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

int hdfsChmod(hdfsFS fs, const char *path, short mode)
{
    int ret;
    // JAVA EQUIVALENT:
    //  fs.setPermission(path, FsPermission)

    jthrowable jthr;
    jobject jPath = NULL, jPermObj = NULL;
    jobject jFS = (jobject)fs;
    jshort jmode = mode;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    // construct jPerm = FsPermission.createImmutable(short mode);
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
            "hdfsChmod(%s): constructNewObjectOfPath", path);
        goto done;
    }

    //Create the directory
    jthr = invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
            "setPermission",
            JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_FSPERM), JAVA_VOID),
            jPath, jPermObj);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "hdfsChmod(%s): FileSystem#setPermission", path);
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

int hdfsUtime(hdfsFS fs, const char *path, tTime mtime, tTime atime)
{
    // JAVA EQUIVALENT:
    //  fs.setTimes(src, mtime, atime)

    jthrowable jthr;
    jobject jFS = (jobject)fs;
    jobject jPath;
    static const tTime NO_CHANGE = -1;
    jlong jmtime, jatime;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsUtime(path=%s): constructNewObjectOfPath", path);
        return -1;
    }

    jmtime = (mtime == NO_CHANGE) ? -1 : (mtime * (jlong)1000);
    jatime = (atime == NO_CHANGE) ? -1 : (atime * (jlong)1000);

    jthr = invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
            "setTimes", JMETHOD3(JPARAM(HADOOP_PATH), "J", "J", JAVA_VOID),
            jPath, jmtime, jatime);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "hdfsUtime(path=%s): FileSystem#setTimes", path);
        return -1;
    }
    return 0;
}

/**
 * Zero-copy options.
 *
 * We cache the EnumSet of ReadOptions which has to be passed into every
 * readZero call, to avoid reconstructing it each time.  This cache is cleared
 * whenever an element changes.
 */
struct hadoopRzOptions
{
    JNIEnv *env;
    int skipChecksums;
    jobject byteBufferPool;
    jobject cachedEnumSet;
};

struct hadoopRzOptions *hadoopRzOptionsAlloc(void)
{
    struct hadoopRzOptions *opts;
    JNIEnv *env;

    env = getJNIEnv();
    if (!env) {
        // Check to make sure the JNI environment is set up properly.
        errno = EINTERNAL;
        return NULL;
    }
    opts = calloc(1, sizeof(struct hadoopRzOptions));
    if (!opts) {
        errno = ENOMEM;
        return NULL;
    }
    return opts;
}

static void hadoopRzOptionsClearCached(JNIEnv *env,
        struct hadoopRzOptions *opts)
{
    if (!opts->cachedEnumSet) {
        return;
    }
    (*env)->DeleteGlobalRef(env, opts->cachedEnumSet);
    opts->cachedEnumSet = NULL;
}

int hadoopRzOptionsSetSkipChecksum(
        struct hadoopRzOptions *opts, int skip)
{
    JNIEnv *env;
    env = getJNIEnv();
    if (!env) {
        errno = EINTERNAL;
        return -1;
    }
    hadoopRzOptionsClearCached(env, opts);
    opts->skipChecksums = !!skip;
    return 0;
}

int hadoopRzOptionsSetByteBufferPool(
        struct hadoopRzOptions *opts, const char *className)
{
    JNIEnv *env;
    jthrowable jthr;
    jobject byteBufferPool = NULL;
    jobject globalByteBufferPool = NULL;
    int ret;

    env = getJNIEnv();
    if (!env) {
        errno = EINTERNAL;
        return -1;
    }

    if (className) {
      // Note: we don't have to call hadoopRzOptionsClearCached in this
      // function, since the ByteBufferPool is passed separately from the
      // EnumSet of ReadOptions.

      jthr = constructNewObjectOfClass(env, &byteBufferPool, className, "()V");
      if (jthr) {
          printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
              "hadoopRzOptionsSetByteBufferPool(className=%s): ", className);
          ret = EINVAL;
          goto done;
      }
      // Only set opts->byteBufferPool if creating a global reference is
      // successful
      globalByteBufferPool = (*env)->NewGlobalRef(env, byteBufferPool);
      if (!globalByteBufferPool) {
          printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                  "hadoopRzOptionsSetByteBufferPool(className=%s): ",
                  className);
          ret = EINVAL;
          goto done;
      }
      // Delete any previous ByteBufferPool we had before setting a new one.
      if (opts->byteBufferPool) {
          (*env)->DeleteGlobalRef(env, opts->byteBufferPool);
      }
      opts->byteBufferPool = globalByteBufferPool;
    } else if (opts->byteBufferPool) {
        // If the specified className is NULL, delete any previous
        // ByteBufferPool we had.
        (*env)->DeleteGlobalRef(env, opts->byteBufferPool);
        opts->byteBufferPool = NULL;
    }
    ret = 0;
done:
    destroyLocalReference(env, byteBufferPool);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

void hadoopRzOptionsFree(struct hadoopRzOptions *opts)
{
    JNIEnv *env;
    env = getJNIEnv();
    if (!env) {
        return;
    }
    hadoopRzOptionsClearCached(env, opts);
    if (opts->byteBufferPool) {
        (*env)->DeleteGlobalRef(env, opts->byteBufferPool);
        opts->byteBufferPool = NULL;
    }
    free(opts);
}

struct hadoopRzBuffer
{
    jobject byteBuffer;
    uint8_t *ptr;
    int32_t length;
    int direct;
};

static jthrowable hadoopRzOptionsGetEnumSet(JNIEnv *env,
        struct hadoopRzOptions *opts, jobject *enumSet)
{
    jthrowable jthr = NULL;
    jobject enumInst = NULL, enumSetObj = NULL;
    jvalue jVal;

    if (opts->cachedEnumSet) {
        // If we cached the value, return it now.
        *enumSet = opts->cachedEnumSet;
        goto done;
    }
    if (opts->skipChecksums) {
        jthr = fetchEnumInstance(env, READ_OPTION,
                  "SKIP_CHECKSUMS", &enumInst);
        if (jthr) {
            goto done;
        }
        jthr = invokeMethod(env, &jVal, STATIC, NULL,
                "java/util/EnumSet", "of",
                "(Ljava/lang/Enum;)Ljava/util/EnumSet;", enumInst);
        if (jthr) {
            goto done;
        }
        enumSetObj = jVal.l;
    } else {
        jclass clazz = (*env)->FindClass(env, READ_OPTION);
        if (!clazz) {
            jthr = getPendingExceptionAndClear(env);
            goto done;
        }
        jthr = invokeMethod(env, &jVal, STATIC, NULL,
                "java/util/EnumSet", "noneOf",
                "(Ljava/lang/Class;)Ljava/util/EnumSet;", clazz);
        enumSetObj = jVal.l;
    }
    // create global ref
    opts->cachedEnumSet = (*env)->NewGlobalRef(env, enumSetObj);
    if (!opts->cachedEnumSet) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    *enumSet = opts->cachedEnumSet;
    jthr = NULL;
done:
    (*env)->DeleteLocalRef(env, enumInst);
    (*env)->DeleteLocalRef(env, enumSetObj);
    return jthr;
}

static int hadoopReadZeroExtractBuffer(JNIEnv *env,
        const struct hadoopRzOptions *opts, struct hadoopRzBuffer *buffer)
{
    int ret;
    jthrowable jthr;
    jvalue jVal;
    uint8_t *directStart;
    void *mallocBuf = NULL;
    jint position;
    jarray array = NULL;

    jthr = invokeMethod(env, &jVal, INSTANCE, buffer->byteBuffer,
                     "java/nio/ByteBuffer", "remaining", "()I");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hadoopReadZeroExtractBuffer: ByteBuffer#remaining failed: ");
        goto done;
    }
    buffer->length = jVal.i;
    jthr = invokeMethod(env, &jVal, INSTANCE, buffer->byteBuffer,
                     "java/nio/ByteBuffer", "position", "()I");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hadoopReadZeroExtractBuffer: ByteBuffer#position failed: ");
        goto done;
    }
    position = jVal.i;
    directStart = (*env)->GetDirectBufferAddress(env, buffer->byteBuffer);
    if (directStart) {
        // Handle direct buffers.
        buffer->ptr = directStart + position;
        buffer->direct = 1;
        ret = 0;
        goto done;
    }
    // Handle indirect buffers.
    // The JNI docs don't say that GetDirectBufferAddress throws any exceptions
    // when it fails.  However, they also don't clearly say that it doesn't.  It
    // seems safest to clear any pending exceptions here, to prevent problems on
    // various JVMs.
    (*env)->ExceptionClear(env);
    if (!opts->byteBufferPool) {
        fputs("hadoopReadZeroExtractBuffer: we read through the "
                "zero-copy path, but failed to get the address of the buffer via "
                "GetDirectBufferAddress.  Please make sure your JVM supports "
                "GetDirectBufferAddress.\n", stderr);
        ret = ENOTSUP;
        goto done;
    }
    // Get the backing array object of this buffer.
    jthr = invokeMethod(env, &jVal, INSTANCE, buffer->byteBuffer,
                     "java/nio/ByteBuffer", "array", "()[B");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hadoopReadZeroExtractBuffer: ByteBuffer#array failed: ");
        goto done;
    }
    array = jVal.l;
    if (!array) {
        fputs("hadoopReadZeroExtractBuffer: ByteBuffer#array returned NULL.",
              stderr);
        ret = EIO;
        goto done;
    }
    mallocBuf = malloc(buffer->length);
    if (!mallocBuf) {
        fprintf(stderr, "hadoopReadZeroExtractBuffer: failed to allocate %d bytes of memory\n",
                buffer->length);
        ret = ENOMEM;
        goto done;
    }
    (*env)->GetByteArrayRegion(env, array, position, buffer->length, mallocBuf);
    jthr = (*env)->ExceptionOccurred(env);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hadoopReadZeroExtractBuffer: GetByteArrayRegion failed: ");
        goto done;
    }
    buffer->ptr = mallocBuf;
    buffer->direct = 0;
    ret = 0;

done:
    free(mallocBuf);
    (*env)->DeleteLocalRef(env, array);
    return ret;
}

static int translateZCRException(JNIEnv *env, jthrowable exc)
{
    int ret;
    char *className = NULL;
    jthrowable jthr = classNameOfObject(exc, env, &className);

    if (jthr) {
        fputs("hadoopReadZero: failed to get class name of "
                "exception from read().\n", stderr);
        destroyLocalReference(env, exc);
        destroyLocalReference(env, jthr);
        ret = EIO;
        goto done;
    }
    if (!strcmp(className, "java.lang.UnsupportedOperationException")) {
        ret = EPROTONOSUPPORT;
        destroyLocalReference(env, exc);
        goto done;
    }
    ret = printExceptionAndFree(env, exc, PRINT_EXC_ALL,
            "hadoopZeroCopyRead: ZeroCopyCursor#read failed");
done:
    free(className);
    return ret;
}

struct hadoopRzBuffer* hadoopReadZero(hdfsFile file,
            struct hadoopRzOptions *opts, int32_t maxLength)
{
    JNIEnv *env;
    jthrowable jthr = NULL;
    jvalue jVal;
    jobject enumSet = NULL, byteBuffer = NULL;
    struct hadoopRzBuffer* buffer = NULL;
    int ret;

    env = getJNIEnv();
    if (!env) {
        errno = EINTERNAL;
        return NULL;
    }
    if (file->type != HDFS_STREAM_INPUT) {
        fputs("Cannot read from a non-InputStream object!\n", stderr);
        ret = EINVAL;
        goto done;
    }
    buffer = calloc(1, sizeof(struct hadoopRzBuffer));
    if (!buffer) {
        ret = ENOMEM;
        goto done;
    }
    jthr = hadoopRzOptionsGetEnumSet(env, opts, &enumSet);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hadoopReadZero: hadoopRzOptionsGetEnumSet failed: ");
        goto done;
    }
    jthr = invokeMethod(env, &jVal, INSTANCE, file->file, HADOOP_ISTRM, "read",
        "(Lorg/apache/hadoop/io/ByteBufferPool;ILjava/util/EnumSet;)"
        "Ljava/nio/ByteBuffer;", opts->byteBufferPool, maxLength, enumSet);
    if (jthr) {
        ret = translateZCRException(env, jthr);
        goto done;
    }
    byteBuffer = jVal.l;
    if (!byteBuffer) {
        buffer->byteBuffer = NULL;
        buffer->length = 0;
        buffer->ptr = NULL;
    } else {
        buffer->byteBuffer = (*env)->NewGlobalRef(env, byteBuffer);
        if (!buffer->byteBuffer) {
            ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                "hadoopReadZero: failed to create global ref to ByteBuffer");
            goto done;
        }
        ret = hadoopReadZeroExtractBuffer(env, opts, buffer);
        if (ret) {
            goto done;
        }
    }
    ret = 0;
done:
    (*env)->DeleteLocalRef(env, byteBuffer);
    if (ret) {
        if (buffer) {
            if (buffer->byteBuffer) {
                (*env)->DeleteGlobalRef(env, buffer->byteBuffer);
            }
            free(buffer);
        }
        errno = ret;
        return NULL;
    } else {
        errno = 0;
    }
    return buffer;
}

int32_t hadoopRzBufferLength(const struct hadoopRzBuffer *buffer)
{
    return buffer->length;
}

const void *hadoopRzBufferGet(const struct hadoopRzBuffer *buffer)
{
    return buffer->ptr;
}

void hadoopRzBufferFree(hdfsFile file, struct hadoopRzBuffer *buffer)
{
    jvalue jVal;
    jthrowable jthr;
    JNIEnv* env;
    
    env = getJNIEnv();
    if (env == NULL) {
        errno = EINTERNAL;
        return;
    }
    if (buffer->byteBuffer) {
        jthr = invokeMethod(env, &jVal, INSTANCE, file->file,
                    HADOOP_ISTRM, "releaseBuffer",
                    "(Ljava/nio/ByteBuffer;)V", buffer->byteBuffer);
        if (jthr) {
            printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "hadoopRzBufferFree: releaseBuffer failed: ");
            // even on error, we have to delete the reference.
        }
        (*env)->DeleteGlobalRef(env, buffer->byteBuffer);
    }
    if (!buffer->direct) {
        free(buffer->ptr);
    }
    memset(buffer, 0, sizeof(*buffer));
    free(buffer);
}

char***
hdfsGetHosts(hdfsFS fs, const char *path, tOffset start, tOffset length)
{
    // JAVA EQUIVALENT:
    //  fs.getFileBlockLoctions(new Path(path), start, length);

    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jobject jPath = NULL;
    jobject jFileStatus = NULL;
    jvalue jFSVal, jVal;
    jobjectArray jBlockLocations = NULL, jFileBlockHosts = NULL;
    jstring jHost = NULL;
    char*** blockHosts = NULL;
    int i, j, ret;
    jsize jNumFileBlocks = 0;
    jobject jFileBlock;
    jsize jNumBlockHosts;
    const char *hostName;

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
            "hdfsGetHosts(path=%s): constructNewObjectOfPath", path);
        goto done;
    }
    jthr = invokeMethod(env, &jFSVal, INSTANCE, jFS,
            HADOOP_FS, "getFileStatus", "(Lorg/apache/hadoop/fs/Path;)"
            "Lorg/apache/hadoop/fs/FileStatus;", jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, NOPRINT_EXC_FILE_NOT_FOUND,
                "hdfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "FileSystem#getFileStatus", path, start, length);
        destroyLocalReference(env, jPath);
        goto done;
    }
    jFileStatus = jFSVal.l;

    //org.apache.hadoop.fs.FileSystem#getFileBlockLocations
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS,
                     HADOOP_FS, "getFileBlockLocations", 
                     "(Lorg/apache/hadoop/fs/FileStatus;JJ)"
                     "[Lorg/apache/hadoop/fs/BlockLocation;",
                     jFileStatus, start, length);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hdfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"):"
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
        jFileBlock =
            (*env)->GetObjectArrayElement(env, jBlockLocations, i);
        jthr = (*env)->ExceptionOccurred(env);
        if (jthr || !jFileBlock) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hdfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "GetObjectArrayElement(%d)", path, start, length, i);
            goto done;
        }
        
        jthr = invokeMethod(env, &jVal, INSTANCE, jFileBlock, HADOOP_BLK_LOC,
                         "getHosts", "()[Ljava/lang/String;");
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hdfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "BlockLocation#getHosts", path, start, length);
            goto done;
        }
        jFileBlockHosts = jVal.l;
        if (!jFileBlockHosts) {
            fprintf(stderr,
                "hdfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"):"
                "BlockLocation#getHosts returned NULL", path, start, length);
            ret = EINTERNAL;
            goto done;
        }
        //Figure out no of hosts in jFileBlockHosts, and allocate the memory
        jNumBlockHosts = (*env)->GetArrayLength(env, jFileBlockHosts);
        blockHosts[i] = calloc(jNumBlockHosts + 1, sizeof(char*));
        if (!blockHosts[i]) {
            ret = ENOMEM;
            goto done;
        }

        //Now parse each hostname
        for (j = 0; j < jNumBlockHosts; ++j) {
            jHost = (*env)->GetObjectArrayElement(env, jFileBlockHosts, j);
            jthr = (*env)->ExceptionOccurred(env);
            if (jthr || !jHost) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                    "hdfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64"): "
                    "NewByteArray", path, start, length);
                goto done;
            }
            hostName =
                (const char*)((*env)->GetStringUTFChars(env, jHost, NULL));
            if (!hostName) {
                ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                    "hdfsGetHosts(path=%s, start=%"PRId64", length=%"PRId64", "
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
        errno = ret;
        if (blockHosts) {
            hdfsFreeHosts(blockHosts);
        }
        return NULL;
    }

    return blockHosts;
}


void hdfsFreeHosts(char ***blockHosts)
{
    int i, j;
    for (i=0; blockHosts[i]; i++) {
        for (j=0; blockHosts[i][j]; j++) {
            free(blockHosts[i][j]);
        }
        free(blockHosts[i]);
    }
    free(blockHosts);
}


tOffset hdfsGetDefaultBlockSize(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getDefaultBlockSize();

    jobject jFS = (jobject)fs;
    jvalue jVal;
    jthrowable jthr;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //FileSystem#getDefaultBlockSize()
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "getDefaultBlockSize", "()J");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetDefaultBlockSize: FileSystem#getDefaultBlockSize");
        return -1;
    }
    return jVal.j;
}


tOffset hdfsGetDefaultBlockSizeAtPath(hdfsFS fs, const char *path)
{
    // JAVA EQUIVALENT:
    //  fs.getDefaultBlockSize(path);

    jthrowable jthr;
    jobject jFS = (jobject)fs;
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
            "hdfsGetDefaultBlockSize(path=%s): constructNewObjectOfPath",
            path);
        return -1;
    }
    jthr = getDefaultBlockSize(env, jFS, jPath, &blockSize);
    (*env)->DeleteLocalRef(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetDefaultBlockSize(path=%s): "
            "FileSystem#getDefaultBlockSize", path);
        return -1;
    }
    return blockSize;
}


tOffset hdfsGetCapacity(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  FsStatus fss = fs.getStatus();
    //  return Fss.getCapacity();

    jobject jFS = (jobject)fs;
    jvalue  jVal;
    jthrowable jthr;
    jobject fss;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //FileSystem#getStatus
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "getStatus", "()Lorg/apache/hadoop/fs/FsStatus;");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetCapacity: FileSystem#getStatus");
        return -1;
    }
    fss = (jobject)jVal.l;
    jthr = invokeMethod(env, &jVal, INSTANCE, fss, HADOOP_FSSTATUS,
                     "getCapacity", "()J");
    destroyLocalReference(env, fss);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetCapacity: FsStatus#getCapacity");
        return -1;
    }
    return jVal.j;
}


  
tOffset hdfsGetUsed(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  FsStatus fss = fs.getStatus();
    //  return Fss.getUsed();

    jobject jFS = (jobject)fs;
    jvalue  jVal;
    jthrowable jthr;
    jobject fss;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return -1;
    }

    //FileSystem#getStatus
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "getStatus", "()Lorg/apache/hadoop/fs/FsStatus;");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetUsed: FileSystem#getStatus");
        return -1;
    }
    fss = (jobject)jVal.l;
    jthr = invokeMethod(env, &jVal, INSTANCE, fss, HADOOP_FSSTATUS,
                     "getUsed", "()J");
    destroyLocalReference(env, fss);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetUsed: FsStatus#getUsed");
        return -1;
    }
    return jVal.j;
}
 
/**
 * We cannot add new fields to the hdfsFileInfo structure because it would break
 * binary compatibility.  The reason is because we return an array
 * of hdfsFileInfo structures from hdfsListDirectory.  So changing the size of
 * those structures would break all programs that relied on finding the second
 * element in the array at <base_offset> + sizeof(struct hdfsFileInfo).
 *
 * So instead, we add the new fields to the hdfsExtendedFileInfo structure.
 * This structure is contained in the mOwner string found inside the
 * hdfsFileInfo.  Specifically, the format of mOwner is:
 *
 * [owner-string] [null byte] [padding] [hdfsExtendedFileInfo structure]
 *
 * The padding is added so that the hdfsExtendedFileInfo structure starts on an
 * 8-byte boundary.
 *
 * @param str           The string to locate the extended info in.
 * @return              The offset of the hdfsExtendedFileInfo structure.
 */
static size_t getExtendedFileInfoOffset(const char *str)
{
    int num_64_bit_words = ((strlen(str) + 1) + 7) / 8;
    return num_64_bit_words * 8;
}

static struct hdfsExtendedFileInfo *getExtendedFileInfo(hdfsFileInfo *fileInfo)
{
    char *owner = fileInfo->mOwner;
    return (struct hdfsExtendedFileInfo *)(owner +
                getExtendedFileInfoOffset(owner));
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
    const char *cPathName;
    const char *cUserName;
    const char *cGroupName;
    struct hdfsExtendedFileInfo *extInfo;
    size_t extOffset;

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
    cPathName =
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
    cUserName =
        (const char*) ((*env)->GetStringUTFChars(env, jUserName, NULL));
    if (!cUserName) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    extOffset = getExtendedFileInfoOffset(cUserName);
    fileInfo->mOwner = malloc(extOffset + sizeof(struct hdfsExtendedFileInfo));
    if (!fileInfo->mOwner) {
        jthr = newRuntimeError(env, "getFileInfo: OOM allocating mOwner");
        goto done;
    }
    strcpy(fileInfo->mOwner, cUserName);
    (*env)->ReleaseStringUTFChars(env, jUserName, cUserName);
    extInfo = getExtendedFileInfo(fileInfo);
    memset(extInfo, 0, sizeof(*extInfo));
    jthr = invokeMethod(env, &jVal, INSTANCE, jStat,
                    HADOOP_STAT, "isEncrypted", "()Z");
    if (jthr) {
        goto done;
    }
    if (jVal.z == JNI_TRUE) {
        extInfo->flags |= HDFS_EXTENDED_FILE_INFO_ENCRYPTED;
    }
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
        hdfsFreeFileInfoEntry(fileInfo);
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



hdfsFileInfo* hdfsListDirectory(hdfsFS fs, const char *path, int *numEntries)
{
    // JAVA EQUIVALENT:
    //  Path p(path);
    //  Path []pathList = fs.listPaths(p)
    //  foreach path in pathList 
    //    getFileInfo(path)

    jobject jFS = (jobject)fs;
    jthrowable jthr;
    jobject jPath = NULL;
    hdfsFileInfo *pathList = NULL; 
    jobjectArray jPathList = NULL;
    jvalue jVal;
    jsize jPathListSize = 0;
    int ret;
    jsize i;
    jobject tmpStat;

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
            "hdfsListDirectory(%s): constructNewObjectOfPath", path);
        goto done;
    }

    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_DFS, "listStatus",
                     JMETHOD1(JPARAM(HADOOP_PATH), JARRPARAM(HADOOP_STAT)),
                     jPath);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "hdfsListDirectory(%s): FileSystem#listStatus", path);
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
    for (i=0; i < jPathListSize; ++i) {
        tmpStat = (*env)->GetObjectArrayElement(env, jPathList, i);
        jthr = (*env)->ExceptionOccurred(env);
        if (jthr || !tmpStat) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hdfsListDirectory(%s): GetObjectArrayElement(%d out of %d)",
                path, i, jPathListSize);
            goto done;
        }
        jthr = getFileInfoFromStat(env, tmpStat, &pathList[i]);
        destroyLocalReference(env, tmpStat);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "hdfsListDirectory(%s): getFileInfoFromStat(%d out of %d)",
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
    errno = 0;
    return pathList;
}



hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char *path)
{
    // JAVA EQUIVALENT:
    //  File f(path);
    //  fs.isDirectory(f)
    //  fs.lastModified() ??
    //  fs.getLength(f)
    //  f.getPath()

    jobject jFS = (jobject)fs;
    jobject jPath;
    jthrowable jthr;
    hdfsFileInfo *fileInfo;

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
      errno = EINTERNAL;
      return NULL;
    }

    //Create an object of org.apache.hadoop.fs.Path
    jthr = constructNewObjectOfPath(env, path, &jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "hdfsGetPathInfo(%s): constructNewObjectOfPath", path);
        return NULL;
    }
    jthr = getFileInfo(env, jFS, jPath, &fileInfo);
    destroyLocalReference(env, jPath);
    if (jthr) {
        errno = printExceptionAndFree(env, jthr,
            NOPRINT_EXC_ACCESS_CONTROL | NOPRINT_EXC_FILE_NOT_FOUND |
            NOPRINT_EXC_UNRESOLVED_LINK,
            "hdfsGetPathInfo(%s): getFileInfo", path);
        return NULL;
    }
    if (!fileInfo) {
        errno = ENOENT;
        return NULL;
    }
    return fileInfo;
}

static void hdfsFreeFileInfoEntry(hdfsFileInfo *hdfsFileInfo)
{
    free(hdfsFileInfo->mName);
    free(hdfsFileInfo->mOwner);
    free(hdfsFileInfo->mGroup);
    memset(hdfsFileInfo, 0, sizeof(*hdfsFileInfo));
}

void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries)
{
    //Free the mName, mOwner, and mGroup
    int i;
    for (i=0; i < numEntries; ++i) {
        hdfsFreeFileInfoEntry(hdfsFileInfo + i);
    }

    //Free entire block
    free(hdfsFileInfo);
}

int hdfsFileIsEncrypted(hdfsFileInfo *fileInfo)
{
    struct hdfsExtendedFileInfo *extInfo;

    extInfo = getExtendedFileInfo(fileInfo);
    return !!(extInfo->flags & HDFS_EXTENDED_FILE_INFO_ENCRYPTED);
}

char* hdfsGetLastExceptionRootCause()
{
  return getLastTLSExceptionRootCause();
}

char* hdfsGetLastExceptionStackTrace()
{
  return getLastTLSExceptionStackTrace();
}

/**
 * vim: ts=4: sw=4: et:
 */
