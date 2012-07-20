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

#ifndef __FUSE_CONNECT_H__
#define __FUSE_CONNECT_H__

struct fuse_context;
struct hdfsConn;
struct hdfs_internal;

/**
 * Initialize the fuse connection subsystem.
 *
 * This must be called before any of the other functions in this module.
 *
 * @param nnUri      The NameNode URI
 * @param port       The NameNode port
 *
 * @return           0 on success; error code otherwise
 */
int fuseConnectInit(const char *nnUri, int port);

/**
 * Get a libhdfs connection.
 *
 * If there is an existing connection, it will be reused.  If not, a new one
 * will be created.
 *
 * You must call hdfsConnRelease on the connection you get back!
 *
 * @param usrname    The username to use
 * @param ctx        The FUSE context to use (contains UID, PID of requestor)
 * @param conn       (out param) The HDFS connection
 *
 * @return           0 on success; error code otherwise
 */
int fuseConnect(const char *usrname, struct fuse_context *ctx,
                struct hdfsConn **out);

/**
 * Get a libhdfs connection.
 *
 * The same as fuseConnect, except the username will be determined from the FUSE
 * thread context.
 *
 * @param conn       (out param) The HDFS connection
 *
 * @return           0 on success; error code otherwise
 */
int fuseConnectAsThreadUid(struct hdfsConn **conn);

/**
 * Test whether we can connect to the HDFS cluster
 *
 * @return           0 on success; error code otherwise
 */
int fuseConnectTest(void);

/**
 * Get the hdfsFS associated with an hdfsConn.
 *
 * @param conn       The hdfsConn
 *
 * @return           the hdfsFS
 */
struct hdfs_internal* hdfsConnGetFs(struct hdfsConn *conn);

/**
 * Release an hdfsConn when we're done with it.
 *
 * @param conn       The hdfsConn
 */
void hdfsConnRelease(struct hdfsConn *conn);

#endif
