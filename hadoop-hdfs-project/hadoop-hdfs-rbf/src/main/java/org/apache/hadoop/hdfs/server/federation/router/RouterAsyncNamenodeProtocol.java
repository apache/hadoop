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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.asyncReturn;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.getCompletableFuture;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.setCurCompletableFuture;

public class RouterAsyncNamenodeProtocol extends RouterNamenodeProtocol{
  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;

  public RouterAsyncNamenodeProtocol(RouterRpcServer server) {
    super(server);
    this.rpcServer = server;
    this.rpcClient =  this.rpcServer.getRPCClient();
  }

  @Override
  public BlocksWithLocations getBlocks(
      DatanodeInfo datanode, long size,
      long minBlockSize, long hotBlockTimeInterval, StorageType storageType) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // Get the namespace where the datanode is located
    rpcServer.getDatanodeStorageReportMapAsync(HdfsConstants.DatanodeReportType.ALL);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply((Function<Object, Object>) o -> {
      String nsId = null;
      Map<String, DatanodeStorageReport[]> map = (Map<String, DatanodeStorageReport[]>) o;
      for (Map.Entry<String, DatanodeStorageReport[]> entry : map.entrySet()) {
        DatanodeStorageReport[] dns = entry.getValue();
        for (DatanodeStorageReport dn : dns) {
          DatanodeInfo dnInfo = dn.getDatanodeInfo();
          if (dnInfo.getDatanodeUuid().equals(datanode.getDatanodeUuid())) {
            nsId = entry.getKey();
            break;
          }
        }
        // Break the loop if already found
        if (nsId != null) {
          break;
        }
      }
      return nsId;
    }).thenCompose(o -> {
      // Forward to the proper namenode
      if (o != null) {
        String nsId = (String) o;
        try {
          RemoteMethod method = new RemoteMethod(
              NamenodeProtocol.class, "getBlocks", new Class<?>[]
              {DatanodeInfo.class, long.class, long.class, long.class, StorageType.class},
              datanode, size, minBlockSize, hotBlockTimeInterval, storageType);
          rpcClient.invokeSingle(nsId, method, BlocksWithLocations.class);
          return getCompletableFuture();
        } catch (IOException e) {
          throw new CompletionException(e);
        }
      }
      return CompletableFuture.completedFuture(null);
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(BlocksWithLocations.class);
  }

  @Override
  public ExportedBlockKeys getBlockKeys() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getBlockKeys");
    rpcServer.invokeAtAvailableNsAsync(method, ExportedBlockKeys.class);
    return asyncReturn(ExportedBlockKeys.class);
  }

  @Override
  public long getTransactionID() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getTransactionID");
    rpcServer.invokeAtAvailableNsAsync(method, long.class);
    return asyncReturn(Long.class);
  }

  @Override
  public long getMostRecentCheckpointTxId() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getMostRecentCheckpointTxId");
    rpcServer.invokeAtAvailableNsAsync(method, long.class);
    return asyncReturn(Long.class);
  }

  @Override
  public long getMostRecentNameNodeFileTxId(NNStorage.NameNodeFile nnf)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getMostRecentNameNodeFileTxId",
            new Class<?>[] {NNStorage.NameNodeFile.class}, nnf);
    rpcServer.invokeAtAvailableNsAsync(method, long.class);
    return asyncReturn(Long.class);
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "versionRequest");
    rpcServer.invokeAtAvailableNsAsync(method, NamespaceInfo.class);
    return asyncReturn(NamespaceInfo.class);
  }
}
