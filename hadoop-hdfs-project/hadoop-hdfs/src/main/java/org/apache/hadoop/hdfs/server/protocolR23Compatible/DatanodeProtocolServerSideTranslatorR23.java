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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocolR23Compatible.DatanodeIDWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.ExtendedBlockWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.LocatedBlockWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.NamespaceInfoWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

/**
 * This class is used on the server side. Calls come across the wire for the
 * protocol family of Release 23 onwards. This class translates the R23 data
 * types to the native data types used inside the NN as specified in the generic
 * DatanodeProtocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class DatanodeProtocolServerSideTranslatorR23 implements
    DatanodeWireProtocol {
  final private DatanodeProtocol server;

  /**
   * Constructor
   * @param server - the NN server
   * @throws IOException
   */
  public DatanodeProtocolServerSideTranslatorR23(DatanodeProtocol server)
      throws IOException {
    this.server = server;
  }

  /**
   * The client side will redirect getProtocolSignature to
   * getProtocolSignature2.
   * 
   * However the RPC layer below on the Server side will call getProtocolVersion
   * and possibly in the future getProtocolSignature. Hence we still implement
   * it even though the end client's call will never reach here.
   */
  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link DatanodeProtocol}
     * 
     */
    if (!protocol.equals(RPC.getProtocolName(DatanodeWireProtocol.class))) {
      throw new IOException("Namenode Serverside implements " +
          RPC.getProtocolName(DatanodeWireProtocol.class) +
          ". The following requested protocol is unknown: " + protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        DatanodeWireProtocol.versionID, DatanodeWireProtocol.class);
  }

  @Override
  public ProtocolSignatureWritable 
          getProtocolSignature2(
      String protocol, long clientVersion, int clientMethodsHash)
      throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link DatanodeProtocol}
     */
    return ProtocolSignatureWritable.convert(
        this.getProtocolSignature(protocol, clientVersion, clientMethodsHash));
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(RPC.getProtocolName(DatanodeWireProtocol.class))) {
      return DatanodeWireProtocol.versionID;
    }
    throw new IOException("Namenode Serverside implements " +
        RPC.getProtocolName(DatanodeWireProtocol.class) +
        ". The following requested protocol is unknown: " + protocol);
  }

  @Override
  public DatanodeRegistrationWritable registerDatanode(
      DatanodeRegistrationWritable registration) throws IOException {
    return DatanodeRegistrationWritable.convert(server
        .registerDatanode(registration.convert()));
  }

  @Override
  public HeartbeatResponseWritable sendHeartbeat(
      DatanodeRegistrationWritable registration, long capacity, long dfsUsed,
      long remaining, long blockPoolUsed, int xmitsInProgress,
      int xceiverCount, int failedVolumes) throws IOException {
    return HeartbeatResponseWritable.convert(server.sendHeartbeat(
        registration.convert(), capacity, dfsUsed, remaining, blockPoolUsed,
        xmitsInProgress, xceiverCount, failedVolumes));
  }

  @Override
  public DatanodeCommandWritable blockReport(
      DatanodeRegistrationWritable registration, String poolId, long[] blocks)
      throws IOException {
    return DatanodeCommandHelper.convert(server.blockReport(
        registration.convert(), poolId, blocks));
  }

  @Override
  public void blockReceivedAndDeleted(
      DatanodeRegistrationWritable registration, String poolId,
      ReceivedDeletedBlockInfoWritable[] receivedAndDeletedBlocks)
      throws IOException {
    server.blockReceivedAndDeleted(registration.convert(), poolId,
        ReceivedDeletedBlockInfoWritable.convert(receivedAndDeletedBlocks));
  }

  @Override
  public void errorReport(DatanodeRegistrationWritable registration,
      int errorCode, String msg) throws IOException {
    server.errorReport(registration.convert(), errorCode, msg);
  }

  @Override
  public NamespaceInfoWritable versionRequest() throws IOException {
    return NamespaceInfoWritable.convert(server.versionRequest());
  }

  @Override
  public UpgradeCommandWritable processUpgradeCommand(
      UpgradeCommandWritable comm) throws IOException {
    return UpgradeCommandWritable.convert(server.processUpgradeCommand(comm.convert()));
  }

  @Override
  public void reportBadBlocks(LocatedBlockWritable[] blocks) throws IOException {
    server.reportBadBlocks(LocatedBlockWritable.convertLocatedBlock(blocks));
  }

  @Override
  public void commitBlockSynchronization(ExtendedBlockWritable block,
      long newgenerationstamp, long newlength, boolean closeFile,
      boolean deleteblock, DatanodeIDWritable[] newtargets) throws IOException {
    server.commitBlockSynchronization(
        ExtendedBlockWritable.convertExtendedBlock(block), newgenerationstamp,
        newlength, closeFile, deleteblock,
        DatanodeIDWritable.convertDatanodeID(newtargets));
  }
}
