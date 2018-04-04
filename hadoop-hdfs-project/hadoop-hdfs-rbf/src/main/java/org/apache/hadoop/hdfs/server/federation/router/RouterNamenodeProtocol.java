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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

/**
 * Module that implements all the RPC calls in {@link NamenodeProtocol} in the
 * {@link RouterRpcServer}.
 */
public class RouterNamenodeProtocol implements NamenodeProtocol {

  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;
  /** Interface to map global name space to HDFS subcluster name spaces. */
  private final FileSubclusterResolver subclusterResolver;


  public RouterNamenodeProtocol(RouterRpcServer server) {
    this.rpcServer = server;
    this.rpcClient =  this.rpcServer.getRPCClient();
    this.subclusterResolver = this.rpcServer.getSubclusterResolver();
  }

  @Override
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size,
      long minBlockSize) throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    // Get the namespace where the datanode is located
    Map<String, DatanodeStorageReport[]> map =
        rpcServer.getDatanodeStorageReportMap(DatanodeReportType.ALL);
    String nsId = null;
    for (Entry<String, DatanodeStorageReport[]> entry : map.entrySet()) {
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
          NamenodeProtocol.class, "getBlocks",
          new Class<?>[] {DatanodeInfo.class, long.class, long.class},
          datanode, size, minBlockSize);
      return rpcClient.invokeSingle(nsId, method, BlocksWithLocations.class);
    }
    return null;
  }

  @Override
  public ExportedBlockKeys getBlockKeys() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    // We return the information from the default name space
    String defaultNsId = subclusterResolver.getDefaultNamespace();
    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getBlockKeys");
    return rpcClient.invokeSingle(defaultNsId, method, ExportedBlockKeys.class);
  }

  @Override
  public long getTransactionID() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    // We return the information from the default name space
    String defaultNsId = subclusterResolver.getDefaultNamespace();
    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getTransactionID");
    return rpcClient.invokeSingle(defaultNsId, method, long.class);
  }

  @Override
  public long getMostRecentCheckpointTxId() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    // We return the information from the default name space
    String defaultNsId = subclusterResolver.getDefaultNamespace();
    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getMostRecentCheckpointTxId");
    return rpcClient.invokeSingle(defaultNsId, method, long.class);
  }

  @Override
  public CheckpointSignature rollEditLog() throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE, false);
    return null;
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    // We return the information from the default name space
    String defaultNsId = subclusterResolver.getDefaultNamespace();
    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "versionRequest");
    return rpcClient.invokeSingle(defaultNsId, method, NamespaceInfo.class);
  }

  @Override
  public void errorReport(NamenodeRegistration registration, int errorCode,
      String msg) throws IOException {
    rpcServer.checkOperation(OperationCategory.UNCHECKED, false);
  }

  @Override
  public NamenodeRegistration registerSubordinateNamenode(
      NamenodeRegistration registration) throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE, false);
    return null;
  }

  @Override
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE, false);
    return null;
  }

  @Override
  public void endCheckpoint(NamenodeRegistration registration,
      CheckpointSignature sig) throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE, false);
  }

  @Override
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override
  public boolean isUpgradeFinalized() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ, false);
    return false;
  }

  @Override
  public boolean isRollingUpgrade() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ, false);
    return false;
  }
}
