/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

/**
 * Module that implements all the RPC calls in
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} related to
 * Storage Policy in the {@link RouterRpcServer}.
 */
public class RouterStoragePolicy {

  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;

  public RouterStoragePolicy(RouterRpcServer server) {
    this.rpcServer = server;
    this.rpcClient = this.rpcServer.getRPCClient();
  }

  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("setStoragePolicy",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(),
        policyName);
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getStoragePolicies");
    return rpcServer.invokeAtAvailableNs(method, BlockStoragePolicy[].class);
  }

  public void unsetStoragePolicy(String src) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, true);

    List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod method = new RemoteMethod("unsetStoragePolicy",
        new Class<?>[] {String.class},
        new RemoteParam());
    if (rpcServer.isInvokeConcurrent(src)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  public BlockStoragePolicy getStoragePolicy(String path)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, true);

    List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(path, false, false);
    RemoteMethod method = new RemoteMethod("getStoragePolicy",
        new Class<?>[] {String.class},
        new RemoteParam());
    return (BlockStoragePolicy) rpcClient.invokeSequential(locations, method);
  }

  public void satisfyStoragePolicy(String path) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, true);

    List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(path, true, false);
    RemoteMethod method = new RemoteMethod("satisfyStoragePolicy",
        new Class<?>[] {String.class},
        new RemoteParam());
    rpcClient.invokeSequential(locations, method);
  }
}
