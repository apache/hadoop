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
package org.apache.hadoop.hdfs.protocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InitReplicaRecoveryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InitReplicaRecoveryResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryRequestProto;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * This class is the client side translator to translate the requests made on
 * {@link InterDatanodeProtocol} interfaces to the RPC server implementing
 * {@link InterDatanodeProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class InterDatanodeProtocolTranslatorPB implements
    ProtocolMetaInterface, InterDatanodeProtocol, Closeable {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  final private InterDatanodeProtocolPB rpcProxy;

  public InterDatanodeProtocolTranslatorPB(InetSocketAddress addr,
      UserGroupInformation ugi, Configuration conf, SocketFactory factory,
      int socketTimeout)
      throws IOException {
    RPC.setProtocolEngine(conf, InterDatanodeProtocolPB.class,
        ProtobufRpcEngine2.class);
    rpcProxy = RPC.getProxy(InterDatanodeProtocolPB.class,
        RPC.getProtocolVersion(InterDatanodeProtocolPB.class), addr, ugi, conf,
        factory, socketTimeout);
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
      throws IOException {
    InitReplicaRecoveryRequestProto req = InitReplicaRecoveryRequestProto
        .newBuilder().setBlock(PBHelper.convert(rBlock)).build();
    InitReplicaRecoveryResponseProto resp;
    try {
      resp = rpcProxy.initReplicaRecovery(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    if (!resp.getReplicaFound()) {
      // No replica found on the remote node.
      return null;
    } else {
      if (!resp.hasBlock() || !resp.hasState()) {
        throw new IOException("Replica was found but missing fields. " +
            "Req: " + req + "\n" +
            "Resp: " + resp);
      }
    }
    
    BlockProto b = resp.getBlock();
    return new ReplicaRecoveryInfo(b.getBlockId(), b.getNumBytes(),
        b.getGenStamp(), PBHelper.convert(resp.getState()));
  }

  @Override
  public String updateReplicaUnderRecovery(ExtendedBlock oldBlock,
      long recoveryId, long newBlockId, long newLength) throws IOException {
    UpdateReplicaUnderRecoveryRequestProto req = 
        UpdateReplicaUnderRecoveryRequestProto.newBuilder()
        .setBlock(PBHelperClient.convert(oldBlock))
        .setNewLength(newLength).setNewBlockId(newBlockId)
        .setRecoveryId(recoveryId).build();
    try {
      return rpcProxy.updateReplicaUnderRecovery(NULL_CONTROLLER, req
          ).getStorageUuid();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        InterDatanodeProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(InterDatanodeProtocolPB.class), methodName);
  }
}
