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

#include <stdio.h>
#include <string.h>
#include "webhdfs.h"
#include "jni_helper.h"
#include "exception.h"

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

#define JAVA_VOID       "V"

/* Macros for constructing method signatures */
#define JPARAM(X)           "L" X ";"
#define JARRPARAM(X)        "[L" X ";"
#define JMETHOD1(X, R)      "(" X ")" R
#define JMETHOD2(X, Y, R)   "(" X Y ")" R
#define JMETHOD3(X, Y, Z, R)   "(" X Y Z")" R

#define KERBEROS_TICKET_CACHE_PATH "hadoop.security.kerberos.ticket.cache.path"

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

/**
 * Set a configuration value.
 *
 * @param env               The JNI environment
 * @param jConfiguration    The configuration object to modify
 * @param key               The key to modify
 * @param value             The value to set the key to
 *
 * @return                  NULL on success; exception otherwise
 */
static jthrowable hadoopConfSetStr(JNIEnv *env, jobject jConfiguration,
                                   const char *key, const char *value)
{
    jthrowable jthr;
    jstring jkey = NULL, jvalue = NULL;
    
    jthr = newJavaStr(env, key, &jkey);
    if (jthr)
        goto done;
    jthr = newJavaStr(env, value, &jvalue);
    if (jthr)
        goto done;
    jthr = invokeMethod(env, NULL, INSTANCE, jConfiguration,
                        HADOOP_CONF, "set", JMETHOD2(JPARAM(JAVA_STRING),
                                                     JPARAM(JAVA_STRING), JAVA_VOID),
                        jkey, jvalue);
    if (jthr)
        goto done;
done:
    destroyLocalReference(env, jkey);
    destroyLocalReference(env, jvalue);
    return jthr;
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
    
    if (!bld->nn_jni)
        return EINVAL;
    scheme = (strstr(bld->nn_jni, "://")) ? "" : "hdfs://";
    if (bld->port == 0) {
        suffix[0] = '\0';
    } else {
        lastColon = rindex(bld->nn_jni, ':');
        if (lastColon && (strspn(lastColon + 1, "0123456789") ==
                          strlen(lastColon + 1))) {
            fprintf(stderr, "port %d was given, but URI '%s' already "
                    "contains a port!\n", bld->port, bld->nn_jni);
            return EINVAL;
        }
        snprintf(suffix, sizeof(suffix), ":%d", bld->port);
    }
    
    uriLen = strlen(scheme) + strlen(bld->nn_jni) + strlen(suffix);
    u = malloc((uriLen + 1) * (sizeof(char)));
    if (!u) {
        fprintf(stderr, "calcEffectiveURI: out of memory");
        return ENOMEM;
    }
    snprintf(u, uriLen + 1, "%s%s%s", scheme, bld->nn_jni, suffix);
    *uri = u;
    return 0;
}

static const char *maybeNull(const char *str)
{
    return str ? str : "(NULL)";
}

const char *hdfsBuilderToStr(const struct hdfsBuilder *bld,
                                    char *buf, size_t bufLen)
{
    snprintf(buf, bufLen, "forceNewInstance=%d, nn=%s, port=%d, "
             "kerbTicketCachePath=%s, userName=%s, workingDir=%s\n",
             bld->forceNewInstance, maybeNull(bld->nn), bld->port,
             maybeNull(bld->kerbTicketCachePath),
             maybeNull(bld->userName), maybeNull(bld->workingDir));
    return buf;
}

/*
 * The JNI version of builderConnect, return the reflection of FileSystem
 */
jobject hdfsBuilderConnect_JNI(JNIEnv *env, struct hdfsBuilder *bld)
{
    jobject jConfiguration = NULL, jFS = NULL, jURI = NULL, jCachePath = NULL;
    jstring jURIString = NULL, jUserString = NULL;
    jvalue  jVal;
    jthrowable jthr = NULL;
    char *cURI = 0, buf[512];
    int ret;
    jobject jRet = NULL;
    
    //  jConfiguration = new Configuration();
    jthr = constructNewObjectOfClass(env, &jConfiguration, HADOOP_CONF, "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                    "hdfsBuilderConnect_JNI(%s)", hdfsBuilderToStr(bld, buf, sizeof(buf)));
        goto done;
    }
    
    //Check what type of FileSystem the caller wants...
    if (bld->nn_jni == NULL) {
        // Get a local filesystem.
        // Also handle the scenario where nn of hdfsBuilder is set to localhost.
        if (bld->forceNewInstance) {
            // fs = FileSytem#newInstanceLocal(conf);
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS,
                                "newInstanceLocal", JMETHOD1(JPARAM(HADOOP_CONF),
                                                             JPARAM(HADOOP_LOCALFS)), jConfiguration);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                            "hdfsBuilderConnect_JNI(%s)",
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
                                            "hdfsBuilderConnect_JNI(%s)",
                                            hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jFS = jVal.l;
        }
    } else {
        if (!strcmp(bld->nn_jni, "default")) {
            // jURI = FileSystem.getDefaultUri(conf)
            jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_FS,
                                "getDefaultUri",
                                "(Lorg/apache/hadoop/conf/Configuration;)Ljava/net/URI;",
                                jConfiguration);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                            "hdfsBuilderConnect_JNI(%s)",
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
                                            "hdfsBuilderConnect_JNI(%s)",
                                            hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jthr = invokeMethod(env, &jVal, STATIC, NULL, JAVA_NET_URI,
                                "create", "(Ljava/lang/String;)Ljava/net/URI;",
                                jURIString);
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                            "hdfsBuilderConnect_JNI(%s)",
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
                                            "hdfsBuilderConnect_JNI(%s)",
                                            hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
        }
        jthr = newJavaStr(env, bld->userName, &jUserString);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                        "hdfsBuilderConnect_JNI(%s)",
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
                                            "hdfsBuilderConnect_JNI(%s)",
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
                                            "hdfsBuilderConnect_JNI(%s)",
                                            hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            jFS = jVal.l;
        }
    }
    jRet = (*env)->NewGlobalRef(env, jFS);
    if (!jRet) {
        ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
                                           "hdfsBuilderConnect_JNI(%s)",
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
    
    if (ret) {
        errno = ret;
        return NULL;
    }
    return jRet;
}

int hdfsDisconnect_JNI(jobject jFS)
{
    // JAVA EQUIVALENT:
    //  fs.close()
    
    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    int ret;
    
    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }
    
    //Sanity check
    if (jFS == NULL) {
        errno = EBADF;
        return -1;
    }
    
    jthrowable jthr = invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
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

static int hdfsCopyImpl(hdfsFS srcFS, const char* src, hdfsFS dstFS,
                        const char* dst, jboolean deleteSource)
{
    //JAVA EQUIVALENT
    //  FileUtil#copy(srcFS, srcPath, dstFS, dstPath,
    //                 deleteSource = false, conf)
    
    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }
    
    //In libwebhdfs, the hdfsFS derived from hdfsBuilderConnect series functions
    //is actually a hdfsBuilder instance containing address information of NameNode.
    //Thus here we need to use JNI to get the real java FileSystem objects.
    jobject jSrcFS = hdfsBuilderConnect_JNI(env, (struct hdfsBuilder *) srcFS);
    jobject jDstFS = hdfsBuilderConnect_JNI(env, (struct hdfsBuilder *) dstFS);
    
    //Parameters
    jobject jConfiguration = NULL, jSrcPath = NULL, jDstPath = NULL;
    jthrowable jthr;
    jvalue jVal;
    int ret;
    
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
    //Disconnect src/dst FileSystem
    hdfsDisconnect_JNI(jSrcFS);
    hdfsDisconnect_JNI(jDstFS);
    
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    return hdfsCopyImpl(srcFS, src, dstFS, dst, 0);
}

int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    return hdfsCopyImpl(srcFS, src, dstFS, dst, 1);
}

tOffset hdfsGetDefaultBlockSize(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getDefaultBlockSize();
    
    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();
    if (env == NULL) {
        errno = EINTERNAL;
        return -1;
    }
    
    //In libwebhdfs, the hdfsFS derived from hdfsConnect functions
    //is actually a hdfsBuilder instance containing address information of NameNode.
    //Thus here we need to use JNI to get the real java FileSystem objects.
    jobject jFS = hdfsBuilderConnect_JNI(env, (struct hdfsBuilder *) fs);
    
    //FileSystem#getDefaultBlockSize()
    jvalue jVal;
    jthrowable jthr;
    jthr = invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                        "getDefaultBlockSize", "()J");
    if (jthr) {
        errno = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                      "hdfsGetDefaultBlockSize: FileSystem#getDefaultBlockSize");
        //Disconnect
        hdfsDisconnect_JNI(jFS);
        return -1;
    }
    
    //Disconnect
    hdfsDisconnect_JNI(jFS);
    return jVal.j;
}



