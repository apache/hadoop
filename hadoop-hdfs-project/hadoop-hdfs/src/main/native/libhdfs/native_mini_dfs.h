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

#ifndef LIBHDFS_NATIVE_MINI_DFS_H
#define LIBHDFS_NATIVE_MINI_DFS_H

#include <jni.h> /* for jboolean */

#ifdef __cplusplus
extern  "C" {
#endif

struct hdfsBuilder;
struct NativeMiniDfsCluster; 

/**
 * Represents a configuration to use for creating a Native MiniDFSCluster
 */
struct NativeMiniDfsConf {
    /**
     * Nonzero if the cluster should be formatted prior to startup.
     */
    jboolean doFormat;

    /**
     * Whether or not to enable webhdfs in MiniDfsCluster
     */
    jboolean webhdfsEnabled;

    /**
     * The http port of the namenode in MiniDfsCluster
     */
    jint namenodeHttpPort;

    /**
     * Nonzero if we should configure short circuit.
     */
    jboolean configureShortCircuit;
};

/**
 * Create a NativeMiniDfsBuilder
 *
 * @param conf      (inout) The cluster configuration
 *
 * @return      a NativeMiniDfsBuilder, or a NULL pointer on error.
 */
struct NativeMiniDfsCluster* nmdCreate(struct NativeMiniDfsConf *conf);

/**
 * Wait until a MiniDFSCluster comes out of safe mode.
 *
 * @param cl        The cluster
 *
 * @return          0 on success; a non-zero error code if the cluster fails to
 *                  come out of safe mode.
 */
int nmdWaitClusterUp(struct NativeMiniDfsCluster *cl);

/**
 * Shut down a NativeMiniDFS cluster
 *
 * @param cl        The cluster
 *
 * @return          0 on success; a non-zero error code if an exception is
 *                  thrown.
 */
int nmdShutdown(struct NativeMiniDfsCluster *cl);

/**
 * Destroy a Native MiniDFSCluster
 *
 * @param cl        The cluster to destroy
 */
void nmdFree(struct NativeMiniDfsCluster* cl);

/**
 * Get the port that's in use by the given (non-HA) nativeMiniDfs
 *
 * @param cl        The initialized NativeMiniDfsCluster
 *
 * @return          the port, or a negative error code
 */
int nmdGetNameNodePort(const struct NativeMiniDfsCluster *cl); 

/**
 * Get the http address that's in use by the given (non-HA) nativeMiniDfs
 *
 * @param cl        The initialized NativeMiniDfsCluster
 * @param port      Used to capture the http port of the NameNode 
 *                  of the NativeMiniDfsCluster
 * @param hostName  Used to capture the http hostname of the NameNode
 *                  of the NativeMiniDfsCluster
 *
 * @return          0 on success; a non-zero error code if failing to
 *                  get the information.
 */
int nmdGetNameNodeHttpAddress(const struct NativeMiniDfsCluster *cl,
                               int *port, const char **hostName);

/**
 * Get domain socket path set for this cluster.
 *
 * @param cl        The cluster
 *
 * @return          A const string of domain socket path, or NULL if not set.
 */
const char *hdfsGetDomainSocketPath(const struct NativeMiniDfsCluster *cl);

#ifdef __cplusplus
}
#endif

#endif
