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
package org.apache.hadoop.hdfs.server.protocolR23Compatible;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolR23Compatible.ExtendedBlockWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class forwards InterDatanodeProtocol calls as RPC to the DN server while
 * translating from the parameter types used in InterDatanodeProtocol to those
 * used in protocolR23Compatile.*.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class InterDatanodeProtocolTranslatorR23 implements
    InterDatanodeProtocol {

  final private InterDatanodeWireProtocol rpcProxy;

  /** used for testing */
  public InterDatanodeProtocolTranslatorR23(InetSocketAddress addr,
      UserGroupInformation ugi, Configuration conf, SocketFactory factory,
      int socketTimeout)
      throws IOException {
    rpcProxy = createInterDatanodeProtocolProxy(addr, ugi, conf, factory,
        socketTimeout);
  }

  static InterDatanodeWireProtocol createInterDatanodeProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ugi, Configuration conf,
      SocketFactory factory, int socketTimeout) throws IOException {
    return RPC.getProxy(InterDatanodeWireProtocol.class,
        InterDatanodeWireProtocol.versionID, addr, ugi, conf, factory,
        socketTimeout);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocolName,
      long clientVersion, int clientMethodHash) throws IOException {
    return ProtocolSignatureWritable.convert(rpcProxy.getProtocolSignature2(
        protocolName, clientVersion, clientMethodHash));
  }

  @Override
  public long getProtocolVersion(String protocolName, long clientVersion)
      throws IOException {
    return rpcProxy.getProtocolVersion(protocolName, clientVersion);
  }

  @Override
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
      throws IOException {
    return rpcProxy
        .initReplicaRecovery(RecoveringBlockWritable.convert(rBlock)).convert();
  }

  @Override
  public ExtendedBlock updateReplicaUnderRecovery(ExtendedBlock oldBlock,
      long recoveryId, long newLength) throws IOException {
    ExtendedBlockWritable eb = ExtendedBlockWritable
        .convertExtendedBlock(oldBlock);
    ExtendedBlockWritable b = rpcProxy.updateReplicaUnderRecovery(eb,
        recoveryId, newLength);
    return ExtendedBlockWritable.convertExtendedBlock(b);
  }
}
