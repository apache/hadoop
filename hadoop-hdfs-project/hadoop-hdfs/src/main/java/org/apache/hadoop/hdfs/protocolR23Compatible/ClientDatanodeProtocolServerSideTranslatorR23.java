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
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.token.Token;

/**
 * This class is used on the server side.
 * Calls come across the wire for the protocol family of Release 23 onwards.
 * This class translates the R23 data types to the internal data types used
 * inside the DN as specified in the generic ClientDatanodeProtocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ClientDatanodeProtocolServerSideTranslatorR23 implements
  ClientDatanodeWireProtocol {
  
  final private ClientDatanodeProtocol server;

  /**
   * 
   * @param server - the NN server
   * @throws IOException
   */
  public ClientDatanodeProtocolServerSideTranslatorR23(
      ClientDatanodeProtocol server) throws IOException {
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
     * signature is that of  {@link ClientDatanodeProtocol}
     */
    if (!protocol.equals(RPC.getProtocolName(
        ClientDatanodeWireProtocol.class))) {
      throw new IOException("Datanode Serverside implements " + 
          ClientDatanodeWireProtocol.class + 
          ". The following requested protocol is unknown: " + protocol);
    }
    
    return ProtocolSignature.getProtocolSignature(clientMethodsHash, 
        ClientDatanodeWireProtocol.versionID, 
        ClientDatanodeWireProtocol.class);
  }

  @Override
  public ProtocolSignatureWritable 
    getProtocolSignature2(
        String protocol, long clientVersion, int clientMethodsHash)
      throws IOException {
    /**
     * Don't forward this to the server. The protocol version and
     * signature is that of  {@link ClientNamenodeProtocol}
     */
   return ProtocolSignatureWritable.convert(
        this.getProtocolSignature(protocol, clientVersion, clientMethodsHash));

  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    if (protocol.equals(RPC.getProtocolName(
        ClientDatanodeWireProtocol.class))) {
      return ClientDatanodeWireProtocol.versionID; 
    }
    throw new IOException("Datanode Serverside implements " + 
        ClientDatanodeWireProtocol.class + 
        ". The following requested protocol is unknown: " + protocol);
  }

  @Override
  public long getReplicaVisibleLength(ExtendedBlockWritable b) throws IOException {
    return 
        server.getReplicaVisibleLength(ExtendedBlockWritable.convertExtendedBlock(b));
  }

  @Override
  public void refreshNamenodes() throws IOException {
    server.refreshNamenodes();
  }

  @Override
  public void deleteBlockPool(String bpid, boolean force) throws IOException {
    server.deleteBlockPool(bpid, force);
  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block,
      Token<BlockTokenIdentifier> token) throws IOException {
    return server.getBlockLocalPathInfo(block, token);
  }
}