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
package org.apache.hadoop.hdfs.server.federation.async;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RouterNamenodeProtocol;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.router.async.AsyncApplyFunction;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncComplete;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncReturn;

/**
 * Module that implements all the asynchronous RPC calls in {@link NamenodeProtocol} in the
 * {@link RouterRpcServer}.
 */
public class RouterAsyncNamenodeProtocol extends RouterNamenodeProtocol {

  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;

  public RouterAsyncNamenodeProtocol(RouterRpcServer server) {
    super(server);
    this.rpcServer = server;
    this.rpcClient =  this.rpcServer.getRPCClient();
  }

  /**
   * Asynchronously get a list of blocks belonging to <code>datanode</code>
   * whose total size equals <code>size</code>.
   *
   * @see org.apache.hadoop.hdfs.server.balancer.Balancer
   * @param datanode  a data node
   * @param size      requested size
   * @param minBlockSize each block should be of this minimum Block Size
   * @param hotBlockTimeInterval prefer to get blocks which are belong to
   * the cold files accessed before the time interval
   * @param storageType the given storage type {@link StorageType}
   * @return BlocksWithLocations a list of blocks &amp; their locations
   * @throws IOException if size is less than or equal to 0 or
  datanode does not exist
   */
  @Override
  public BlocksWithLocations getBlocks(
      DatanodeInfo datanode, long size,
      long minBlockSize, long hotBlockTimeInterval, StorageType storageType) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // Get the namespace where the datanode is located
    rpcServer.getDatanodeStorageReportMapAsync(HdfsConstants.DatanodeReportType.ALL);
    asyncApply((AsyncApplyFunction<Map<String, DatanodeStorageReport[]>, Object>) map -> {
      String nsId = null;
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
      // Forward to the proper namenode
      if (nsId != null) {
        RemoteMethod method = new RemoteMethod(
            NamenodeProtocol.class, "getBlocks", new Class<?>[]
            {DatanodeInfo.class, long.class, long.class, long.class, StorageType.class},
            datanode, size, minBlockSize, hotBlockTimeInterval, storageType);
        rpcClient.invokeSingle(nsId, method, BlocksWithLocations.class);
      } else {
        asyncComplete(null);
      }
    });
    return asyncReturn(BlocksWithLocations.class);
  }

  /**
   * Asynchronously get the current block keys.
   *
   * @return ExportedBlockKeys containing current block keys
   * @throws IOException if there is no namespace available or other ioExceptions.
   */
  @Override
  public ExportedBlockKeys getBlockKeys() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getBlockKeys");
    rpcServer.invokeAtAvailableNsAsync(method, ExportedBlockKeys.class);
    return asyncReturn(ExportedBlockKeys.class);
  }

  /**
   * Asynchronously get the most recent transaction ID.
   *
   * @return The most recent transaction ID that has been synced to
   * persistent storage, or applied from persistent storage in the
   * case of a non-active node.
   * @throws IOException if there is no namespace available or other ioExceptions.
   */
  @Override
  public long getTransactionID() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getTransactionID");
    rpcServer.invokeAtAvailableNsAsync(method, long.class);
    return asyncReturn(Long.class);
  }

  /**
   * Asynchronously get the transaction ID of the most recent checkpoint.
   *
   * @return The transaction ID of the most recent checkpoint.
   * @throws IOException if there is no namespace available or other ioExceptions.
   */
  @Override
  public long getMostRecentCheckpointTxId() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getMostRecentCheckpointTxId");
    rpcServer.invokeAtAvailableNsAsync(method, long.class);
    return asyncReturn(Long.class);
  }

  /**
   * Asynchronously get the transaction ID of the most recent checkpoint
   * for the given NameNodeFile.
   *
   * @return The transaction ID of the most recent checkpoint
   * for the given NameNodeFile.
   * @throws IOException if there is no namespace available or other ioExceptions.
   */
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

  /**
   * Asynchronously request name-node version and storage information.
   *
   * @return {@link NamespaceInfo} identifying versions and storage information
   *          of the name-node.
   * @throws IOException if there is no namespace available or other ioExceptions.
   */
  @Override
  public NamespaceInfo versionRequest() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "versionRequest");
    rpcServer.invokeAtAvailableNsAsync(method, NamespaceInfo.class);
    return asyncReturn(NamespaceInfo.class);
  }
}
