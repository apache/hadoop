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
package org.apache.hadoop.hdfs.qjournal.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocolProtos.InterQJournalProtocolService;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.QJournalProtocolService;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.qjournal.protocolPB.InterQJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.InterQJournalProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_RPC_BIND_HOST_KEY;


@InterfaceAudience.Private
@VisibleForTesting
public class JournalNodeRpcServer implements QJournalProtocol,
    InterQJournalProtocol {
  private static final Logger LOG = JournalNode.LOG;
  private static final int HANDLER_COUNT = 5;
  private final JournalNode jn;
  private Server server;

  JournalNodeRpcServer(Configuration conf, JournalNode jn) throws IOException {
    this.jn = jn;
    
    Configuration confCopy = new Configuration(conf);
    
    // Ensure that nagling doesn't kick in, which could cause latency issues.
    confCopy.setBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_KEY,
        true);
    
    InetSocketAddress addr = getAddress(confCopy);
    String bindHost = conf.getTrimmed(DFS_JOURNALNODE_RPC_BIND_HOST_KEY, null);
    if (bindHost == null) {
      bindHost = addr.getHostName();
    }
    LOG.info("RPC server is binding to " + bindHost + ":" + addr.getPort());

    RPC.setProtocolEngine(confCopy, QJournalProtocolPB.class,
        ProtobufRpcEngine.class);
    QJournalProtocolServerSideTranslatorPB translator =
        new QJournalProtocolServerSideTranslatorPB(this);
    BlockingService service = QJournalProtocolService
        .newReflectiveBlockingService(translator);
    
    this.server = new RPC.Builder(confCopy)
        .setProtocol(QJournalProtocolPB.class)
        .setInstance(service)
        .setBindAddress(bindHost)
        .setPort(addr.getPort())
        .setNumHandlers(HANDLER_COUNT)
        .setVerbose(false)
        .build();


    //Adding InterQJournalProtocolPB to server
    InterQJournalProtocolServerSideTranslatorPB
        qJournalProtocolServerSideTranslatorPB = new
        InterQJournalProtocolServerSideTranslatorPB(this);

    BlockingService interQJournalProtocolService = InterQJournalProtocolService
        .newReflectiveBlockingService(qJournalProtocolServerSideTranslatorPB);

    DFSUtil.addPBProtocol(confCopy, InterQJournalProtocolPB.class,
        interQJournalProtocolService, server);


    // set service-level authorization security policy
    if (confCopy.getBoolean(
      CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
          server.refreshServiceAcl(confCopy, new HDFSPolicyProvider());
    }
    this.server.setTracer(jn.tracer);
  }

  void start() {
    this.server.start();
  }

  public InetSocketAddress getAddress() {
    return server.getListenerAddress();
  }
  
  void join() throws InterruptedException {
    this.server.join();
  }
  
  void stop() {
    this.server.stop();
  }
  
  static InetSocketAddress getAddress(Configuration conf) {
    String addr = conf.get(
        DFSConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr, 0,
        DFSConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY);
  }

  @Override
  public boolean isFormatted(String journalId,
                             String nameServiceId) throws IOException {
    return jn.getOrCreateJournal(journalId, nameServiceId).isFormatted();
  }

  @SuppressWarnings("deprecation")
  @Override
  public GetJournalStateResponseProto getJournalState(String journalId,
                                                      String nameServiceId)
        throws IOException {
    long epoch = jn.getOrCreateJournal(journalId,
        nameServiceId).getLastPromisedEpoch();
    return GetJournalStateResponseProto.newBuilder()
        .setLastPromisedEpoch(epoch)
        .setHttpPort(jn.getBoundHttpAddress().getPort())
        .setFromURL(jn.getHttpServerURI())
        .build();
  }

  @Override
  public NewEpochResponseProto newEpoch(String journalId,
                                        String nameServiceId,
                                        NamespaceInfo nsInfo,
      long epoch) throws IOException {
    return jn.getOrCreateJournal(journalId,
        nameServiceId).newEpoch(nsInfo, epoch);
  }

  @Override
  public void format(String journalId,
                     String nameServiceId,
                     NamespaceInfo nsInfo,
                     boolean force)
      throws IOException {
    jn.getOrCreateJournal(journalId, nameServiceId).format(nsInfo, force);
  }

  @Override
  public void journal(RequestInfo reqInfo,
      long segmentTxId, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    jn.getOrCreateJournal(reqInfo.getJournalId(), reqInfo.getNameServiceId())
       .journal(reqInfo, segmentTxId, firstTxnId, numTxns, records);
  }
  
  @Override
  public void heartbeat(RequestInfo reqInfo) throws IOException {
    jn.getOrCreateJournal(reqInfo.getJournalId(), reqInfo.getNameServiceId())
      .heartbeat(reqInfo);
  }

  @Override
  public void startLogSegment(RequestInfo reqInfo, long txid, int layoutVersion)
      throws IOException {
    jn.getOrCreateJournal(reqInfo.getJournalId(), reqInfo.getNameServiceId())
      .startLogSegment(reqInfo, txid, layoutVersion);
  }

  @Override
  public void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    jn.getOrCreateJournal(reqInfo.getJournalId(), reqInfo.getNameServiceId())
      .finalizeLogSegment(reqInfo, startTxId, endTxId);
  }

  @Override
  public void purgeLogsOlderThan(RequestInfo reqInfo, long minTxIdToKeep)
      throws IOException {
    jn.getOrCreateJournal(reqInfo.getJournalId(), reqInfo.getNameServiceId())
      .purgeLogsOlderThan(reqInfo, minTxIdToKeep);
  }

  @SuppressWarnings("deprecation")
  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(
      String jid, String nameServiceId,
      long sinceTxId, boolean inProgressOk)
      throws IOException {
    
    RemoteEditLogManifest manifest = jn.getOrCreateJournal(jid, nameServiceId)
        .getEditLogManifest(sinceTxId, inProgressOk);
    
    return GetEditLogManifestResponseProto.newBuilder()
        .setManifest(PBHelper.convert(manifest))
        .setHttpPort(jn.getBoundHttpAddress().getPort())
        .setFromURL(jn.getHttpServerURI())
        .build();
  }

  @Override
  public GetJournaledEditsResponseProto getJournaledEdits(String jid,
      String nameServiceId, long sinceTxId, int maxTxns) throws IOException {
    return jn.getOrCreateJournal(jid, nameServiceId)
        .getJournaledEdits(sinceTxId, maxTxns);
  }

  @Override
  public PrepareRecoveryResponseProto prepareRecovery(RequestInfo reqInfo,
      long segmentTxId) throws IOException {
    return jn.getOrCreateJournal(reqInfo.getJournalId(),
        reqInfo.getNameServiceId())
        .prepareRecovery(reqInfo, segmentTxId);
  }

  @Override
  public void acceptRecovery(RequestInfo reqInfo, SegmentStateProto log,
      URL fromUrl) throws IOException {
    jn.getOrCreateJournal(reqInfo.getJournalId(), reqInfo.getNameServiceId())
        .acceptRecovery(reqInfo, log, fromUrl);
  }

  @Override
  public void doPreUpgrade(String journalId) throws IOException {
    jn.doPreUpgrade(journalId);
  }

  @Override
  public void doUpgrade(String journalId, StorageInfo sInfo) throws IOException {
    jn.doUpgrade(journalId, sInfo);
  }

  @Override
  public void doFinalize(String journalId,
                         String nameServiceId) throws IOException {
    jn.doFinalize(journalId, nameServiceId);
  }

  @Override
  public Boolean canRollBack(String journalId,
                             String nameServiceId, StorageInfo storage,
      StorageInfo prevStorage, int targetLayoutVersion)
      throws IOException {
    return jn.canRollBack(journalId, storage, prevStorage, targetLayoutVersion,
        nameServiceId);
  }

  @Override
  public void doRollback(String journalId,
                         String nameServiceId) throws IOException {
    jn.doRollback(journalId, nameServiceId);
  }

  @Override
  public void discardSegments(String journalId,
                              String nameServiceId, long startTxId)
      throws IOException {
    jn.discardSegments(journalId, startTxId, nameServiceId);
  }

  @Override
  public Long getJournalCTime(String journalId,
                              String nameServiceId) throws IOException {
    return jn.getJournalCTime(journalId, nameServiceId);
  }

  @SuppressWarnings("deprecation")
  @Override
  public GetEditLogManifestResponseProto getEditLogManifestFromJournal(
      String jid, String nameServiceId,
      long sinceTxId, boolean inProgressOk)
      throws IOException {

    RemoteEditLogManifest manifest = jn.getOrCreateJournal(jid, nameServiceId)
        .getEditLogManifest(sinceTxId, inProgressOk);

    return GetEditLogManifestResponseProto.newBuilder()
        .setManifest(PBHelper.convert(manifest))
        .setHttpPort(jn.getBoundHttpAddress().getPort())
        .setFromURL(jn.getHttpServerURI())
        .build();
  }

  /** Allow access to the RPC server for testing. */
  @VisibleForTesting
  Server getRpcServer() {
    return server;
  }
}
