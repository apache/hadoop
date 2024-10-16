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

import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncReturn;

public class RouterAsyncStoragePolicy extends RouterStoragePolicy {
  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;

  public RouterAsyncStoragePolicy(RouterRpcServer server) {
    super(server);
    this.rpcServer = server;
    this.rpcClient = this.rpcServer.getRPCClient();
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, true);

    List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(path, false, false);
    RemoteMethod method = new RemoteMethod("getStoragePolicy",
        new Class<?>[] {String.class},
        new RemoteParam());
    rpcClient.invokeSequential(locations, method);
    return asyncReturn(BlockStoragePolicy.class);
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getStoragePolicies");
    rpcServer.invokeAtAvailableNsAsync(method, BlockStoragePolicy[].class);
    return asyncReturn(BlockStoragePolicy[].class);
  }
}