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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocolR23Compatible.ClientNamenodeWireProtocol;
import org.apache.hadoop.hdfs.protocolR23Compatible.ExtendedBlockWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;

/** An inter-datanode protocol for updating generation stamp
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY,
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY)
@InterfaceAudience.Private
public interface InterDatanodeWireProtocol extends VersionedProtocol {
  public static final Log LOG = 
      LogFactory.getLog(InterDatanodeWireProtocol.class);
  /**
   * The  rules for changing this protocol are the same as that for
   * {@link ClientNamenodeWireProtocol} - see that java file for details.
   * 6: Add block pool ID to Block
   */
  public static final long versionID = 6L;

  /**
   * Initialize a replica recovery.
   * 
   * @return actual state of the replica on this data-node or 
   * null if data-node does not have the replica.
   */
  ReplicaRecoveryInfoWritable initReplicaRecovery(RecoveringBlockWritable rBlock)
      throws IOException;

  /**
   * Update replica with the new generation stamp and length.  
   */
  ExtendedBlockWritable updateReplicaUnderRecovery(
      ExtendedBlockWritable oldBlock, long recoveryId, long newLength)
      throws IOException;
  
  /**
   * This method is defined to get the protocol signature using 
   * the R23 protocol - hence we have added the suffix of 2 to the method name
   * to avoid conflict.
   */
  public ProtocolSignatureWritable getProtocolSignature2(
      String protocol, long clientVersion, int clientMethodsHash)
      throws IOException;
}
