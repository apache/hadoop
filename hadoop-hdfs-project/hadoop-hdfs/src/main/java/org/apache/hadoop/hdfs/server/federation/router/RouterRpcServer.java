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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.service.AbstractService;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is responsible for handling all of the RPC calls to the It is
 * created, started, and stopped by {@link Router}. It implements the
 * {@link ClientProtocol} to mimic a
 * {@link org.apache.hadoop.hdfs.server.namenode.NameNode NameNode} and proxies
 * the requests to the active
 * {@link org.apache.hadoop.hdfs.server.namenode.NameNode NameNode}.
 */
public class RouterRpcServer extends AbstractService {

  /** The RPC server that listens to requests from clients. */
  private final Server rpcServer;

  /**
   * Construct a router RPC server.
   *
   * @param configuration HDFS Configuration.
   * @param nnResolver The NN resolver instance to determine active NNs in HA.
   * @param fileResolver File resolver to resolve file paths to subclusters.
   * @throws IOException If the RPC server could not be created.
   */
  public RouterRpcServer(Configuration configuration, Router router,
      ActiveNamenodeResolver nnResolver, FileSubclusterResolver fileResolver)
          throws IOException {
    super(RouterRpcServer.class.getName());

    this.rpcServer = null;
  }

  /**
   * Allow access to the client RPC server for testing.
   *
   * @return The RPC server.
   */
  @VisibleForTesting
  public Server getServer() {
    return this.rpcServer;
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    super.serviceInit(configuration);
  }

  /**
   * Start client and service RPC servers.
   */
  @Override
  protected void serviceStart() throws Exception {
    if (this.rpcServer != null) {
      this.rpcServer.start();
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.rpcServer != null) {
      this.rpcServer.stop();
    }
    super.serviceStop();
  }

  /**
   * Wait until the RPC servers have shutdown.
   */
  void join() throws InterruptedException {
    if (this.rpcServer != null) {
      this.rpcServer.join();
    }
  }
}
