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
package org.apache.hadoop.hdfs.qjournal.protocol;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol used to communicate between {@link QuorumJournalManager}
 * and each {@link JournalNode}.
 * 
 * This is responsible for sending edits as well as coordinating
 * recovery of the nodes.
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface QJournalProtocol {
  public static final long versionID = 1L;

  /**
   * @return true if the given journal has been formatted and
   * contains valid data.
   */
  boolean isFormatted(String journalId,
                      String nameServiceId) throws IOException;

  /**
   * Get the current state of the journal, including the most recent
   * epoch number and the HTTP port.
   */
  GetJournalStateResponseProto getJournalState(String journalId,
                                               String nameServiceId)
      throws IOException;
  
  /**
   * Format the underlying storage for the given namespace.
   */
  void format(String journalId, String nameServiceId,
      NamespaceInfo nsInfo, boolean force) throws IOException;

  /**
   * Begin a new epoch. See the HDFS-3077 design doc for details.
   */
  NewEpochResponseProto newEpoch(String journalId,
                                        String nameServiceId,
                                        NamespaceInfo nsInfo,
                                        long epoch) throws IOException;
  
  /**
   * Journal edit records.
   * This message is sent by the active name-node to the JournalNodes
   * to write edits to their local logs.
   */
  public void journal(RequestInfo reqInfo,
                      long segmentTxId,
                      long firstTxnId,
                      int numTxns,
                      byte[] records) throws IOException;

  
  /**
   * Heartbeat.
   * This is a no-op on the server, except that it verifies that the
   * caller is in fact still the active writer, and provides up-to-date
   * information on the most recently committed txid.
   */
  public void heartbeat(RequestInfo reqInfo) throws IOException;
  
  /**
   * Start writing to a new log segment on the JournalNode.
   * Before calling this, one should finalize the previous segment
   * using {@link #finalizeLogSegment(RequestInfo, long, long)}.
   * 
   * @param txid the first txid in the new log
   * @param layoutVersion the LayoutVersion of the new log
   */
  public void startLogSegment(RequestInfo reqInfo,
      long txid, int layoutVersion) throws IOException;

  /**
   * Finalize the given log segment on the JournalNode. The segment
   * is expected to be in-progress and starting at the given startTxId.
   *
   * @param startTxId the starting transaction ID of the log
   * @param endTxId the expected last transaction in the given log
   * @throws IOException if no such segment exists
   */
  public void finalizeLogSegment(RequestInfo reqInfo,
      long startTxId, long endTxId) throws IOException;

  /**
   * @throws IOException 
   * @see JournalManager#purgeLogsOlderThan(long)
   */
  public void purgeLogsOlderThan(RequestInfo requestInfo, long minTxIdToKeep)
      throws IOException;
  
  /**
   * @param jid the journal from which to enumerate edits
   * @param sinceTxId the first transaction which the client cares about
   * @param inProgressOk whether or not to check the in-progress edit log 
   *        segment       
   * @return a list of edit log segments since the given transaction ID.
   */
  GetEditLogManifestResponseProto getEditLogManifest(String jid,
                                                     String nameServiceId,
                                                     long sinceTxId,
                                                     boolean inProgressOk)
      throws IOException;

  /**
   * Fetch edit logs present in the Journal's in-memory cache of edits
   * ({@link org.apache.hadoop.hdfs.qjournal.server.JournaledEditsCache}).
   * To enable this cache, in-progress edit log tailing must be enabled via the
   * {@value DFSConfigKeys#DFS_HA_TAILEDITS_INPROGRESS_KEY} configuration key.
   *
   * @param jid The ID of the journal from which to fetch edits.
   * @param nameServiceId The ID of the namespace for which to fetch edits.
   * @param sinceTxId Fetch edits starting at this transaction ID
   * @param maxTxns Request at most this many transactions to be returned
   * @throws IOException If there was an issue encountered while fetching edits
   *     from the cache, including a cache miss (cache does not contain the
   *     requested edits). The caller should then attempt to fetch the edits via
   *     the streaming mechanism (starting with
   *     {@link #getEditLogManifest(String, String, long, boolean)}).
   * @return Response containing serialized edits to be loaded
   * @see org.apache.hadoop.hdfs.qjournal.server.JournaledEditsCache
   */
  GetJournaledEditsResponseProto getJournaledEdits(String jid,
      String nameServiceId, long sinceTxId, int maxTxns) throws IOException;

  /**
   * Begin the recovery process for a given segment. See the HDFS-3077
   * design document for details.
   */
  public PrepareRecoveryResponseProto prepareRecovery(RequestInfo reqInfo,
      long segmentTxId) throws IOException;

  /**
   * Accept a proposed recovery for the given transaction ID.
   */
  public void acceptRecovery(RequestInfo reqInfo,
      SegmentStateProto stateToAccept, URL fromUrl) throws IOException;

  void doPreUpgrade(String journalId) throws IOException;

  public void doUpgrade(String journalId, StorageInfo sInfo) throws IOException;

  void doFinalize(String journalId,
                         String nameServiceid) throws IOException;

  Boolean canRollBack(String journalId, String nameServiceid,
                      StorageInfo storage, StorageInfo prevStorage,
                      int targetLayoutVersion) throws IOException;

  void doRollback(String journalId,
                         String nameServiceid) throws IOException;

  /**
   * Discard journal segments whose first TxId is greater than or equal to the
   * given txid.
   */
  @Idempotent
  void discardSegments(String journalId,
                       String nameServiceId,
                       long startTxId)
      throws IOException;

  Long getJournalCTime(String journalId,
                       String nameServiceId) throws IOException;
}
