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
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol used to journal edits to a remote node. Currently,
 * this is used to publish edits from the NameNode to a BackupNode.
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface JournalProtocol {
  /**
   * 
   * This class is used by both the Namenode (client) and BackupNode (server) 
   * to insulate from the protocol serialization.
   * 
   * If you are adding/changing DN's interface then you need to 
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in JournalProtocol.proto.
   * 
   * For more details on protocol buffer wire protocol, please see 
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   */
  public static final long versionID = 1L;

  /**
   * Journal edit records.
   * This message is sent by the active name-node to the backup node
   * via {@code EditLogBackupOutputStream} in order to synchronize meta-data
   * changes with the backup namespace image.
   * 
   * @param journalInfo journal information
   * @param epoch marks beginning a new journal writer
   * @param firstTxnId the first transaction of this batch
   * @param numTxns number of transactions
   * @param records byte array containing serialized journal records
   * @throws FencedException if the resource has been fenced
   */
  public void journal(JournalInfo journalInfo,
                      long epoch,
                      long firstTxnId,
                      int numTxns,
                      byte[] records) throws IOException;

  /**
   * Notify the BackupNode that the NameNode has rolled its edit logs
   * and is now writing a new log segment.
   * @param journalInfo journal information
   * @param epoch marks beginning a new journal writer
   * @param txid the first txid in the new log
   * @throws FencedException if the resource has been fenced
   */
  public void startLogSegment(JournalInfo journalInfo, long epoch,
      long txid) throws IOException;
  
  /**
   * Request to fence any other journal writers.
   * Older writers with at previous epoch will be fenced and can no longer
   * perform journal operations.
   * 
   * @param journalInfo journal information
   * @param epoch marks beginning a new journal writer
   * @param fencerInfo info about fencer for debugging purposes
   * @throws FencedException if the resource has been fenced
   */
  public FenceResponse fence(JournalInfo journalInfo, long epoch,
      String fencerInfo) throws IOException;
}
