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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InitReplicaRecoveryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InitReplicaRecoveryResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryResponseProto;
import org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link InterDatanodeProtocolPB} to the
 * {@link InterDatanodeProtocol} server implementation.
 */
@InterfaceAudience.Private
public class InterDatanodeProtocolServerSideTranslatorPB implements
    InterDatanodeProtocolPB {
  private final InterDatanodeProtocol impl;

  public InterDatanodeProtocolServerSideTranslatorPB(InterDatanodeProtocol impl) {
    this.impl = impl;
  }

  @Override
  public InitReplicaRecoveryResponseProto initReplicaRecovery(
      RpcController unused, InitReplicaRecoveryRequestProto request)
      throws ServiceException {
    RecoveringBlock b = PBHelper.convert(request.getBlock());
    ReplicaRecoveryInfo r;
    try {
      r = impl.initReplicaRecovery(b);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return InitReplicaRecoveryResponseProto.newBuilder()
        .setBlock(PBHelper.convert(r)).build();
  }

  @Override
  public UpdateReplicaUnderRecoveryResponseProto updateReplicaUnderRecovery(
      RpcController unused, UpdateReplicaUnderRecoveryRequestProto request)
      throws ServiceException {
    ExtendedBlock b;
    try {
      b = impl.updateReplicaUnderRecovery(PBHelper.convert(request.getBlock()),
          request.getRecoveryId(), request.getNewLength());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return UpdateReplicaUnderRecoveryResponseProto.newBuilder()
        .setBlock(PBHelper.convert(b)).build();
  }

  /** @see VersionedProtocol#getProtocolVersion */
  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return RPC.getProtocolVersion(InterDatanodeProtocolPB.class);
  }
  
  /**
   * The client side will redirect getProtocolSignature to
   * getProtocolSignature2.
   * 
   * However the RPC layer below on the Server side will call getProtocolVersion
   * and possibly in the future getProtocolSignature. Hence we still implement
   * it even though the end client will never call this method.
   * 
   * @see VersionedProtocol#getProtocolVersion
   */
  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link InterDatanodeProtocol}
     */
    if (!protocol.equals(RPC.getProtocolName(InterDatanodeProtocol.class))) {
      throw new IOException("Namenode Serverside implements " +
          RPC.getProtocolName(InterDatanodeProtocol.class) +
          ". The following requested protocol is unknown: " + protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        RPC.getProtocolVersion(InterDatanodeProtocolPB.class),
        InterDatanodeProtocolPB.class);
  }


  @Override
  public ProtocolSignatureWritable getProtocolSignature2(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link InterDatanodeProtocol}
     */
    return ProtocolSignatureWritable.convert(
        this.getProtocolSignature(protocol, clientVersion, clientMethodsHash));
  }
}