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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocolR23Compatible.ClientNamenodeWireProtocol;
import org.apache.hadoop.hdfs.protocolR23Compatible.JournalWireProtocol;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol used to journal edits to a remote node. Currently,
 * this is used to publish edits from the NameNode to a BackupNode.
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
    clientPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY)
@InterfaceAudience.Private
public interface JournalProtocol extends VersionedProtocol {
  /**
   * 
   * This class is used by both the Namenode (client) and BackupNode (server) 
   * to insulate from the protocol serialization.
   * 
   * If you are adding/changing DN's interface then you need to 
   * change both this class and ALSO
   * {@link JournalWireProtocol}.
   * These changes need to be done in a compatible fashion as described in 
   * {@link ClientNamenodeWireProtocol}
   */
  public static final long versionID = 1L;

  /**
   * Journal edit records.
   * This message is sent by the active name-node to the backup node
   * via {@code EditLogBackupOutputStream} in order to synchronize meta-data
   * changes with the backup namespace image.
   * 
   * @param registration active node registration
   * @param firstTxnId the first transaction of this batch
   * @param numTxns number of transactions
   * @param records byte array containing serialized journal records
   */
  public void journal(NamenodeRegistration registration,
                      long firstTxnId,
                      int numTxns,
                      byte[] records) throws IOException;

  /**
   * Notify the BackupNode that the NameNode has rolled its edit logs
   * and is now writing a new log segment.
   * @param registration the registration of the active NameNode
   * @param txid the first txid in the new log
   */
  public void startLogSegment(NamenodeRegistration registration,
      long txid) throws IOException;
}
