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
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InitReplicaRecoveryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InitReplicaRecoveryResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryResponseProto;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;

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
    
    if (r == null) {
      return InitReplicaRecoveryResponseProto.newBuilder()
          .setReplicaFound(false)
          .build();
    } else {
      return InitReplicaRecoveryResponseProto.newBuilder()
          .setReplicaFound(true)
          .setBlock(PBHelper.convert(r))
          .setState(PBHelper.convert(r.getOriginalReplicaState())).build();
    }
  }

  @Override
  public UpdateReplicaUnderRecoveryResponseProto updateReplicaUnderRecovery(
      RpcController unused, UpdateReplicaUnderRecoveryRequestProto request)
      throws ServiceException {
    final String storageID;
    try {
      storageID = impl.updateReplicaUnderRecovery(
          PBHelper.convert(request.getBlock()),
          request.getRecoveryId(), request.getNewLength());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return UpdateReplicaUnderRecoveryResponseProto.newBuilder()
        .setStorageUuid(storageID).build();
  }
}