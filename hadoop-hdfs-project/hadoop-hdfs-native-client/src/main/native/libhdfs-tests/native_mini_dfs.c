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
#include "jni_helper.h"
#include "native_mini_dfs.h"
#include "platform.h"

#include <errno.h>
#include <jni.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef EINTERNAL
#define EINTERNAL 255
#endif

#define MINIDFS_CLUSTER_BUILDER "org/apache/hadoop/hdfs/MiniDFSCluster$Builder"
#define MINIDFS_CLUSTER "org/apache/hadoop/hdfs/MiniDFSCluster"
#define HADOOP_CONF     "org/apache/hadoop/conf/Configuration"
#define HADOOP_NAMENODE "org/apache/hadoop/hdfs/server/namenode/NameNode"
#define JAVA_INETSOCKETADDRESS "java/net/InetSocketAddress"

struct NativeMiniDfsCluster {
    /**
     * The NativeMiniDfsCluster object
     */
    jobject obj;

    /**
     * Path to the domain socket, or the empty string if there is none.
     */
    char domainSocketPath[PATH_MAX];
};

static int hdfsDisableDomainSocketSecurity(void)
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

static jthrowable nmdConfigureShortCircuit(JNIEnv *env,
              struct NativeMiniDfsCluster *cl, jobject cobj)
{
    jthrowable jthr;
    char *tmpDir;

    int ret = hdfsDisableDomainSocketSecurity();
    if (ret) {
        return newRuntimeError(env, "failed to disable hdfs domain "
                               "socket security: error %d", ret);
    }
    jthr = hadoopConfSetStr(env, cobj, "dfs.client.read.shortcircuit", "true");
    if (jthr) {
        return jthr;
    }
    tmpDir = getenv("TMPDIR");
    if (!tmpDir) {
        tmpDir = "/tmp";
    }
    snprintf(cl->domainSocketPath, PATH_MAX, "%s/native_mini_dfs.sock.%d.%d",
             tmpDir, getpid(), rand());
    snprintf(cl->domainSocketPath, PATH_MAX, "%s/native_mini_dfs.sock.%d.%d",
             tmpDir, getpid(), rand());
    jthr = hadoopConfSetStr(env, cobj, "dfs.domain.socket.path",
                            cl->domainSocketPath);
    if (jthr) {
        return jthr;
    }
    return NULL;
}

struct NativeMiniDfsCluster* nmdCreate(struct NativeMiniDfsConf *conf)
{
    struct NativeMiniDfsCluster* cl = NULL;
    jobject bld = NULL, cobj = NULL, cluster = NULL;
    jvalue  val;
    JNIEnv *env = getJNIEnv();
    jthrowable jthr;
    jstring jconfStr = NULL;

    if (!env) {
        fprintf(stderr, "nmdCreate: unable to construct JNIEnv.\n");
        return NULL;
    }
    cl = calloc(1, sizeof(struct NativeMiniDfsCluster));
    if (!cl) {
        fprintf(stderr, "nmdCreate: OOM");
        goto error;
    }
    jthr = constructNewObjectOfClass(env, &cobj, HADOOP_CONF, "()V");
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "nmdCreate: new Configuration");
        goto error;
    }
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                              "nmdCreate: Configuration::setBoolean");
        goto error;
    }
    // Disable 'minimum block size' -- it's annoying in tests.
    (*env)->DeleteLocalRef(env, jconfStr);
    jconfStr = NULL;
    jthr = newJavaStr(env, "dfs.namenode.fs-limits.min-block-size", &jconfStr);
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                              "nmdCreate: new String");
        goto error;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, cobj, HADOOP_CONF,
                        "setLong", "(Ljava/lang/String;J)V", jconfStr, 0LL);
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                              "nmdCreate: Configuration::setLong");
        goto error;
    }
    // Creae MiniDFSCluster object
    jthr = constructNewObjectOfClass(env, &bld, MINIDFS_CLUSTER_BUILDER,
                    "(L"HADOOP_CONF";)V", cobj);
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "nmdCreate: NativeMiniDfsCluster#Builder#Builder");
        goto error;
    }
    if (conf->configureShortCircuit) {
        jthr = nmdConfigureShortCircuit(env, cl, cobj);
        if (jthr) {
            printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                "nmdCreate: nmdConfigureShortCircuit error");
            goto error;
        }
    }
    jthr = invokeMethod(env, &val, INSTANCE, bld, MINIDFS_CLUSTER_BUILDER,
            "format", "(Z)L" MINIDFS_CLUSTER_BUILDER ";", conf->doFormat);
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL, "nmdCreate: "
                              "Builder::format");
        goto error;
    }
    (*env)->DeleteLocalRef(env, val.l);
    if (conf->webhdfsEnabled) {
        jthr = invokeMethod(env, &val, INSTANCE, bld, MINIDFS_CLUSTER_BUILDER,
                        "nameNodeHttpPort", "(I)L" MINIDFS_CLUSTER_BUILDER ";",
                        conf->namenodeHttpPort);
        if (jthr) {
            printExceptionAndFree(env, jthr, PRINT_EXC_ALL, "nmdCreate: "
                                  "Builder::nameNodeHttpPort");
            goto error;
        }
        (*env)->DeleteLocalRef(env, val.l);
    }
    if (conf->numDataNodes) {
        jthr = invokeMethod(env, &val, INSTANCE, bld, MINIDFS_CLUSTER_BUILDER,
                "numDataNodes", "(I)L" MINIDFS_CLUSTER_BUILDER ";", conf->numDataNodes);
        if (jthr) {
            printExceptionAndFree(env, jthr, PRINT_EXC_ALL, "nmdCreate: "
                                  "Builder::numDataNodes");
            goto error;
        }
    }
    (*env)->DeleteLocalRef(env, val.l);
    jthr = invokeMethod(env, &val, INSTANCE, bld, MINIDFS_CLUSTER_BUILDER,
            "build", "()L" MINIDFS_CLUSTER ";");
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                              "nmdCreate: Builder#build");
        goto error;
    }
    cluster = val.l;
	  cl->obj = (*env)->NewGlobalRef(env, val.l);
    if (!cl->obj) {
        printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "nmdCreate: NewGlobalRef");
        goto error;
    }
    (*env)->DeleteLocalRef(env, cluster);
    (*env)->DeleteLocalRef(env, bld);
    (*env)->DeleteLocalRef(env, cobj);
    (*env)->DeleteLocalRef(env, jconfStr);
    return cl;

error:
    (*env)->DeleteLocalRef(env, cluster);
    (*env)->DeleteLocalRef(env, bld);
    (*env)->DeleteLocalRef(env, cobj);
    (*env)->DeleteLocalRef(env, jconfStr);
    free(cl);
    return NULL;
}

void nmdFree(struct NativeMiniDfsCluster* cl)
{
    JNIEnv *env = getJNIEnv();
    if (!env) {
        fprintf(stderr, "nmdFree: getJNIEnv failed\n");
        free(cl);
        return;
    }
    (*env)->DeleteGlobalRef(env, cl->obj);
    free(cl);
}

int nmdShutdown(struct NativeMiniDfsCluster* cl)
{
    JNIEnv *env = getJNIEnv();
    jthrowable jthr;

    if (!env) {
        fprintf(stderr, "nmdShutdown: getJNIEnv failed\n");
        return -EIO;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, cl->obj,
            MINIDFS_CLUSTER, "shutdown", "()V");
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "nmdShutdown: MiniDFSCluster#shutdown");
        return -EIO;
    }
    return 0;
}

int nmdWaitClusterUp(struct NativeMiniDfsCluster *cl)
{
    jthrowable jthr;
    JNIEnv *env = getJNIEnv();
    if (!env) {
        fprintf(stderr, "nmdWaitClusterUp: getJNIEnv failed\n");
        return -EIO;
    }
    jthr = invokeMethod(env, NULL, INSTANCE, cl->obj,
            MINIDFS_CLUSTER, "waitClusterUp", "()V");
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "nmdWaitClusterUp: MiniDFSCluster#waitClusterUp ");
        return -EIO;
    }
    return 0;
}

int nmdGetNameNodePort(const struct NativeMiniDfsCluster *cl)
{
    JNIEnv *env = getJNIEnv();
    jvalue jVal;
    jthrowable jthr;

    if (!env) {
        fprintf(stderr, "nmdHdfsConnect: getJNIEnv failed\n");
        return -EIO;
    }
    // Note: this will have to be updated when HA nativeMiniDfs clusters are
    // supported
    jthr = invokeMethod(env, &jVal, INSTANCE, cl->obj,
            MINIDFS_CLUSTER, "getNameNodePort", "()I");
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "nmdHdfsConnect: MiniDFSCluster#getNameNodePort");
        return -EIO;
    }
    return jVal.i;
}

int nmdGetNameNodeHttpAddress(const struct NativeMiniDfsCluster *cl,
                               int *port, const char **hostName)
{
    JNIEnv *env = getJNIEnv();
    jvalue jVal;
    jobject jNameNode, jAddress;
    jthrowable jthr;
    int ret = 0;
    const char *host;

    if (!env) {
        fprintf(stderr, "nmdHdfsConnect: getJNIEnv failed\n");
        return -EIO;
    }
    // First get the (first) NameNode of the cluster
    jthr = invokeMethod(env, &jVal, INSTANCE, cl->obj, MINIDFS_CLUSTER,
                        "getNameNode", "()L" HADOOP_NAMENODE ";");
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                              "nmdGetNameNodeHttpAddress: "
                              "MiniDFSCluster#getNameNode");
        return -EIO;
    }
    jNameNode = jVal.l;

    // Then get the http address (InetSocketAddress) of the NameNode
    jthr = invokeMethod(env, &jVal, INSTANCE, jNameNode, HADOOP_NAMENODE,
                        "getHttpAddress", "()L" JAVA_INETSOCKETADDRESS ";");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                    "nmdGetNameNodeHttpAddress: "
                                    "NameNode#getHttpAddress");
        goto error_dlr_nn;
    }
    jAddress = jVal.l;

    jthr = invokeMethod(env, &jVal, INSTANCE, jAddress,
                        JAVA_INETSOCKETADDRESS, "getPort", "()I");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                    "nmdGetNameNodeHttpAddress: "
                                    "InetSocketAddress#getPort");
        goto error_dlr_addr;
    }
    *port = jVal.i;

    jthr = invokeMethod(env, &jVal, INSTANCE, jAddress, JAVA_INETSOCKETADDRESS,
                        "getHostName", "()Ljava/lang/String;");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                    "nmdGetNameNodeHttpAddress: "
                                    "InetSocketAddress#getHostName");
        goto error_dlr_addr;
    }
    host = (*env)->GetStringUTFChars(env, jVal.l, NULL);
    *hostName = strdup(host);
    (*env)->ReleaseStringUTFChars(env, jVal.l, host);

error_dlr_addr:
    (*env)->DeleteLocalRef(env, jAddress);
error_dlr_nn:
    (*env)->DeleteLocalRef(env, jNameNode);

    return ret;
}

const char *hdfsGetDomainSocketPath(const struct NativeMiniDfsCluster *cl) {
    if (cl->domainSocketPath[0]) {
        return cl->domainSocketPath;
    }

    return NULL;
}
