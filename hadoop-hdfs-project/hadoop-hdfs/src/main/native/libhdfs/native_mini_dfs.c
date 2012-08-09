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

#include <errno.h>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#define MINIDFS_CLUSTER_BUILDER "org/apache/hadoop/hdfs/MiniDFSCluster$Builder"
#define MINIDFS_CLUSTER "org/apache/hadoop/hdfs/MiniDFSCluster"
#define HADOOP_CONF     "org/apache/hadoop/conf/Configuration"

struct NativeMiniDfsCluster {
    /**
     * The NativeMiniDfsCluster object
     */
    jobject obj;
};

struct NativeMiniDfsCluster* nmdCreate(struct NativeMiniDfsConf *conf)
{
    struct NativeMiniDfsCluster* cl = NULL;
    jobject bld = NULL, bld2 = NULL, cobj = NULL;
    jvalue  val;
    JNIEnv *env = getJNIEnv();
    jthrowable jthr;

    if (!env) {
        fprintf(stderr, "nmdCreate: unable to construct JNIEnv.\n");
        goto error;
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
        goto error_free_cl;
    }
    jthr = constructNewObjectOfClass(env, &bld, MINIDFS_CLUSTER_BUILDER,
                    "(L"HADOOP_CONF";)V", cobj);
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
            "nmdCreate: NativeMiniDfsCluster#Builder#Builder");
        goto error_dlr_cobj;
    }
    jthr = invokeMethod(env, &val, INSTANCE, bld, MINIDFS_CLUSTER_BUILDER,
            "format", "(Z)L" MINIDFS_CLUSTER_BUILDER ";", conf->doFormat);
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL, "nmdCreate: "
                              "Builder::format");
        goto error_dlr_bld;
    }
    bld2 = val.l;
    jthr = invokeMethod(env, &val, INSTANCE, bld, MINIDFS_CLUSTER_BUILDER,
            "build", "()L" MINIDFS_CLUSTER ";");
    if (jthr) {
        printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                              "nmdCreate: Builder#build");
        goto error_dlr_bld2;
    }
	cl->obj = (*env)->NewGlobalRef(env, val.l);
    if (!cl->obj) {
        printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "nmdCreate: NewGlobalRef");
        goto error_dlr_val;
    }
    (*env)->DeleteLocalRef(env, val.l);
    (*env)->DeleteLocalRef(env, bld2);
    (*env)->DeleteLocalRef(env, bld);
    (*env)->DeleteLocalRef(env, cobj);
    return cl;

error_dlr_val:
    (*env)->DeleteLocalRef(env, val.l);
error_dlr_bld2:
    (*env)->DeleteLocalRef(env, bld2);
error_dlr_bld:
    (*env)->DeleteLocalRef(env, bld);
error_dlr_cobj:
    (*env)->DeleteLocalRef(env, cobj);
error_free_cl:
    free(cl);
error:
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
