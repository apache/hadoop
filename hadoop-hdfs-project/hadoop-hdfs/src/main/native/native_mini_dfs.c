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

#include "hdfsJniHelper.h"
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
    if (!env) {
        fprintf(stderr, "nmdCreate: unable to construct JNIEnv.\n");
        goto error;
    }
    cl = calloc(1, sizeof(struct NativeMiniDfsCluster));
    if (!cl) {
        fprintf(stderr, "nmdCreate: OOM");
        goto error;
    }
    cobj = constructNewObjectOfClass(env, NULL, HADOOP_CONF, "()V");
    if (!cobj) {
        fprintf(stderr, "nmdCreate: unable to construct Configuration\n");
        goto error_free_cl;
    }
    bld = constructNewObjectOfClass(env, NULL, MINIDFS_CLUSTER_BUILDER,
                    "(L"HADOOP_CONF";)V", cobj);
    if (!bld) {
        fprintf(stderr, "nmdCreate: unable to construct "
                "NativeMiniDfsCluster#Builder\n");
        goto error_dlr_cobj;
    }
    if (invokeMethod(env, &val, NULL, INSTANCE, bld,
            MINIDFS_CLUSTER_BUILDER, "format",
            "(Z)L" MINIDFS_CLUSTER_BUILDER ";", conf->doFormat)) {
        fprintf(stderr, "nmdCreate: failed to call Builder#doFormat\n");
        goto error_dlr_bld;
    }
    bld2 = val.l;
    if (invokeMethod(env, &val, NULL, INSTANCE, bld,
            MINIDFS_CLUSTER_BUILDER, "build",
            "()L" MINIDFS_CLUSTER ";")) {
        fprintf(stderr, "nmdCreate: failed to call Builder#build\n");
        goto error_dlr_bld2;
    }
	cl->obj = (*env)->NewGlobalRef(env, val.l);
    if (!cl->obj) {
        fprintf(stderr, "nmdCreate: failed to create global reference to "
            "MiniDFSCluster\n");
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
    if (!env) {
        fprintf(stderr, "nmdShutdown: getJNIEnv failed\n");
        return -EIO;
    }
    if (invokeMethod(env, NULL, NULL, INSTANCE, cl->obj,
            MINIDFS_CLUSTER, "shutdown", "()V")) {
        fprintf(stderr, "nmdShutdown: MiniDFSCluster#shutdown failure\n");
        return -EIO;
    }
    return 0;
}

int nmdWaitClusterUp(struct NativeMiniDfsCluster *cl)
{
    JNIEnv *env = getJNIEnv();
    if (!env) {
        fprintf(stderr, "nmdWaitClusterUp: getJNIEnv failed\n");
        return -EIO;
    }
    if (invokeMethod(env, NULL, NULL, INSTANCE, cl->obj,
            MINIDFS_CLUSTER, "waitClusterUp", "()V")) {
        fprintf(stderr, "nmdWaitClusterUp: MiniDFSCluster#waitClusterUp "
                "failure\n");
        return -EIO;
    }
    return 0;
}

int nmdGetNameNodePort(struct NativeMiniDfsCluster *cl)
{
    JNIEnv *env = getJNIEnv();
    jvalue jVal;

    if (!env) {
        fprintf(stderr, "nmdHdfsConnect: getJNIEnv failed\n");
        return -EIO;
    }
    // Note: this will have to be updated when HA nativeMiniDfs clusters are
    // supported
    if (invokeMethod(env, &jVal, NULL, INSTANCE, cl->obj,
            MINIDFS_CLUSTER, "getNameNodePort", "()I")) {
        fprintf(stderr, "nmdHdfsConnect: MiniDFSCluster#getNameNodePort "
                "failure\n");
        return -EIO;
    }
    return jVal.i;
}
