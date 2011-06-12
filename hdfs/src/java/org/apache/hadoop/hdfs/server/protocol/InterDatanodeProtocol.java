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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;

/** An inter-datanode protocol for updating generation stamp
 */
@KerberosInfo(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY)
@InterfaceAudience.Private
public interface InterDatanodeProtocol extends VersionedProtocol {
  public static final Log LOG = LogFactory.getLog(InterDatanodeProtocol.class);

  /**
   * 5: getBlockMetaDataInfo(), updateBlock() removed.
   */
  public static final long versionID = 5L;

  /**
   * Initialize a replica recovery.
   * 
   * @return actual state of the replica on this data-node or 
   * null if data-node does not have the replica.
   */
  ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
  throws IOException;

  /**
   * Update replica with the new generation stamp and length.  
   */
  Block updateReplicaUnderRecovery(Block oldBlock,
                                   long recoveryId,
                                   long newLength) throws IOException;
}
