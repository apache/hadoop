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
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSelector;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;

/** An client-datanode protocol for block recovery
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY)
@TokenInfo(BlockTokenSelector.class)
public interface ClientDatanodeProtocol extends VersionedProtocol {
  /**
   * Until version 9, this class ClientDatanodeProtocol served as both
   * the client interface to the DN AND the RPC protocol used to 
   * communicate with the NN.
   * 
   * Post version 10 (release 23 of Hadoop), the protocol is implemented in
   * {@literal ../protocolR23Compatible/ClientDatanodeWireProtocol}
   * 
   * This class is used by both the DFSClient and the 
   * DN server side to insulate from the protocol serialization.
   * 
   * If you are adding/changing DN's interface then you need to 
   * change both this class and ALSO
   * {@link org.apache.hadoop.hdfs.protocolR23Compatible.ClientDatanodeWireProtocol}.
   * These changes need to be done in a compatible fashion as described in 
   * {@link org.apache.hadoop.hdfs.protocolR23Compatible.ClientNamenodeWireProtocol}
   * 
   * The log of historical changes can be retrieved from the svn).
   * 9: Added deleteBlockPool method
   * 
   * 9 is the last version id when this class was used for protocols
   *  serialization. DO not update this version any further. 
   *  Changes are recorded in R23 classes.
   */
  public static final long versionID = 9L;

  /** Return the visible length of a replica. */
  long getReplicaVisibleLength(ExtendedBlock b) throws IOException;
  
  /**
   * Refresh the list of federated namenodes from updated configuration
   * Adds new namenodes and stops the deleted namenodes.
   * 
   * @throws IOException on error
   **/
  void refreshNamenodes() throws IOException;

  /**
   * Delete the block pool directory. If force is false it is deleted only if
   * it is empty, otherwise it is deleted along with its contents.
   * 
   * @param bpid Blockpool id to be deleted.
   * @param force If false blockpool directory is deleted only if it is empty 
   *          i.e. if it doesn't contain any block files, otherwise it is 
   *          deleted along with its contents.
   * @throws IOException
   */
  void deleteBlockPool(String bpid, boolean force) throws IOException;
  
  /**
   * Retrieves the path names of the block file and metadata file stored on the
   * local file system.
   * 
   * In order for this method to work, one of the following should be satisfied:
   * <ul>
   * <li>
   * The client user must be configured at the datanode to be able to use this
   * method.</li>
   * <li>
   * When security is enabled, kerberos authentication must be used to connect
   * to the datanode.</li>
   * </ul>
   * 
   * @param block
   *          the specified block on the local datanode
   * @param token
   *          the block access token.
   * @return the BlockLocalPathInfo of a block
   * @throws IOException
   *           on error
   */
  BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block,
      Token<BlockTokenIdentifier> token) throws IOException;
}
