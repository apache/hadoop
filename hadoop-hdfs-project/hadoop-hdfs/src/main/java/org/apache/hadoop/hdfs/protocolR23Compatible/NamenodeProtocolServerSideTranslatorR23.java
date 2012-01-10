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
package org.apache.hadoop.hdfs.protocolR23Compatible;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

/**
 * This class is used on the server side.
 * Calls come across the wire for the protocol family of Release 23 onwards.
 * This class translates the R23 data types to the internal data types used
 * inside the DN as specified in the generic NamenodeProtocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class NamenodeProtocolServerSideTranslatorR23 implements
  NamenodeWireProtocol {
  
  final private NamenodeProtocol server;

  /**
   * @param server - the NN server
   * @throws IOException
   */
  public NamenodeProtocolServerSideTranslatorR23(
      NamenodeProtocol server) throws IOException {
    this.server = server;
  }
  
  /**
   * the client side will redirect getProtocolSignature to 
   * getProtocolSignature2.
   * 
   * However the RPC layer below on the Server side will call
   * getProtocolVersion and possibly in the future getProtocolSignature.
   * Hence we still implement it even though the end client's call will
   * never reach here.
   */
  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and
     * signature is that of  {@link NamenodeProtocol}
     */
    if (!protocol.equals(RPC.getProtocolName(
        NamenodeWireProtocol.class))) {
      throw new IOException("Namenode Serverside implements " + 
          NamenodeWireProtocol.class + 
          ". The following requested protocol is unknown: " + protocol);
    }
    
    return ProtocolSignature.getProtocolSignature(clientMethodsHash, 
        NamenodeWireProtocol.versionID, 
        NamenodeWireProtocol.class);
  }

  @Override
  public ProtocolSignatureWritable getProtocolSignature2(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and
     * signature is that of  {@link ClientNamenodeProtocol}
     */
   return ProtocolSignatureWritable.convert(
        this.getProtocolSignature(protocol, clientVersion, clientMethodsHash));
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(RPC.getProtocolName(
        NamenodeWireProtocol.class))) {
      return NamenodeWireProtocol.versionID; 
    }
    throw new IOException("Datanode Serverside implements " + 
        NamenodeWireProtocol.class + 
        ". The following requested protocol is unknown: " + protocol);
  }

  @Override
  public BlocksWithLocationsWritable getBlocks(DatanodeInfoWritable datanode,
      long size) throws IOException {
    BlocksWithLocations locs = server.getBlocks(
        DatanodeInfoWritable.convertDatanodeInfo(datanode), size);
    return BlocksWithLocationsWritable.convert(locs);
  }

  @Override
  public ExportedBlockKeysWritable getBlockKeys() throws IOException {
    return ExportedBlockKeysWritable.convert(server.getBlockKeys());
  }

  @Override
  public long getTransactionID() throws IOException {
    return server.getTransactionID();
  }

  @Override
  @SuppressWarnings("deprecation")
  public CheckpointSignatureWritable rollEditLog() throws IOException {
    return CheckpointSignatureWritable.convert(server.rollEditLog());
  }

  @Override
  public NamespaceInfoWritable versionRequest() throws IOException {
    return NamespaceInfoWritable.convert(server.versionRequest());
  }

  @Override
  public void errorReport(NamenodeRegistrationWritable registration,
      int errorCode, String msg) throws IOException {
    server.errorReport(registration.convert(), errorCode, msg);
  }

  @Override
  public NamenodeRegistrationWritable register(
      NamenodeRegistrationWritable registration) throws IOException {
    return NamenodeRegistrationWritable.convert(server
        .register(registration.convert()));
  }

  @Override
  public NamenodeCommandWritable startCheckpoint(
      NamenodeRegistrationWritable registration) throws IOException {
    return NamenodeCommandWritable.convert(server.startCheckpoint(registration
        .convert()));
  }

  @Override
  public void endCheckpoint(NamenodeRegistrationWritable registration,
      CheckpointSignatureWritable sig) throws IOException {
    server.endCheckpoint(registration.convert(), sig.convert());
  }

  @Override
  public RemoteEditLogManifestWritable getEditLogManifest(long sinceTxId)
      throws IOException {
    return RemoteEditLogManifestWritable.convert(server
        .getEditLogManifest(sinceTxId));
  }
}