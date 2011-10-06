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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSelector;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;

/** 
 * This class defines the actual protocol used to communicate with the
 * DN via RPC using writable types.
 * The parameters in the methods which are specified in the
 * package are separate from those used internally in the DN and DFSClient
 * and hence need to be converted using {@link ClientDatanodeProtocolTranslatorR23}
 * and {@link ClientDatanodeProtocolServerSideTranslatorR23}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY)
@TokenInfo(BlockTokenSelector.class)
@ProtocolInfo(protocolName = HdfsConstants.CLIENT_DATANODE_PROTOCOL_NAME)
public interface ClientDatanodeWireProtocol extends VersionedProtocol {
  public static final Log LOG = 
      LogFactory.getLog(ClientDatanodeWireProtocol.class);

  /**
   * The  rules for changing this protocol are the same as that for
   * {@link ClientNamenodeWireProtocol} - see that java file for details.
   * 9: Added deleteBlockPool method
   * 10 Moved the R23 protocol
   */
  public static final long versionID = 10L;

  /**
   * The specification of this method matches that of
   * 
   * {@link org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol
   * #getReplicaVisibleLength(org.apache.hadoop.hdfs.protocol.ExtendedBlock)}
   */
  long getReplicaVisibleLength(ExtendedBlockWritable b) throws IOException;
  
  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol#refreshNamenodes()}
   */
  void refreshNamenodes() throws IOException;

  /**
   * The specification of this method matches that of
   * {@link org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol#deleteBlockPool(String, boolean)}
   */
  void deleteBlockPool(String bpid, boolean force) throws IOException; 
  
  /**
   * This method is defined to get the protocol signature using 
   * the R23 protocol - hence we have added the suffix of 2 to the method name
   * to avoid conflict.
   */
  public org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable
           getProtocolSignature2(String protocol, 
      long clientVersion,
      int clientMethodsHash) throws IOException;
}
