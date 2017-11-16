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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos
  .JournalIdProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos
  .GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos
  .GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * A Journal Sync thread runs through the lifetime of the JN. It periodically
 * gossips with other journal nodes to compare edit log manifests and if it
 * detects any missing log segment, it downloads it from the other journal node
 */
@InterfaceAudience.Private
public class JournalNodeSyncer {
  public static final Logger LOG = LoggerFactory.getLogger(
      JournalNodeSyncer.class);
  private final JournalNode jn;
  private final Journal journal;
  private final String jid;
  private  String nameServiceId;
  private final JournalIdProto jidProto;
  private final JNStorage jnStorage;
  private final Configuration conf;
  private volatile Daemon syncJournalDaemon;
  private volatile boolean shouldSync = true;

  private List<JournalNodeProxy> otherJNProxies = Lists.newArrayList();
  private int numOtherJNs;
  private int journalNodeIndexForSync = 0;
  private final long journalSyncInterval;
  private final int logSegmentTransferTimeout;
  private final DataTransferThrottler throttler;
  private final JournalMetrics metrics;
  private boolean journalSyncerStarted;

  JournalNodeSyncer(JournalNode jouranlNode, Journal journal, String jid,
      Configuration conf, String nameServiceId) {
    this.jn = jouranlNode;
    this.journal = journal;
    this.jid = jid;
    this.nameServiceId = nameServiceId;
    this.jidProto = convertJournalId(this.jid);
    this.jnStorage = journal.getStorage();
    this.conf = conf;
    journalSyncInterval = conf.getLong(
        DFSConfigKeys.DFS_JOURNALNODE_SYNC_INTERVAL_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_SYNC_INTERVAL_DEFAULT);
    logSegmentTransferTimeout = conf.getInt(
        DFSConfigKeys.DFS_EDIT_LOG_TRANSFER_TIMEOUT_KEY,
        DFSConfigKeys.DFS_EDIT_LOG_TRANSFER_TIMEOUT_DEFAULT);
    throttler = getThrottler(conf);
    metrics = journal.getMetrics();
    journalSyncerStarted = false;
  }

  void stopSync() {
    shouldSync = false;
    // Delete the edits.sync directory
    File editsSyncDir = journal.getStorage().getEditsSyncDir();
    if (editsSyncDir.exists()) {
      FileUtil.fullyDelete(editsSyncDir);
    }
    if (syncJournalDaemon != null) {
      syncJournalDaemon.interrupt();
    }
  }

  public void start(String nsId) {
    if (nsId != null) {
      this.nameServiceId = nsId;
      journal.setTriedJournalSyncerStartedwithnsId(true);
    }
    if (!journalSyncerStarted && getOtherJournalNodeProxies()) {
      LOG.info("Starting SyncJournal daemon for journal " + jid);
      startSyncJournalsDaemon();
      journalSyncerStarted = true;
    }

  }

  public boolean isJournalSyncerStarted() {
    return journalSyncerStarted;
  }

  private boolean createEditsSyncDir() {
    File editsSyncDir = journal.getStorage().getEditsSyncDir();
    if (editsSyncDir.exists()) {
      LOG.info(editsSyncDir + " directory already exists.");
      return true;
    }
    return editsSyncDir.mkdir();
  }

  private boolean getOtherJournalNodeProxies() {
    List<InetSocketAddress> otherJournalNodes = getOtherJournalNodeAddrs();
    if (otherJournalNodes == null || otherJournalNodes.isEmpty()) {
      LOG.warn("Other JournalNode addresses not available. Journal Syncing " +
          "cannot be done");
      return false;
    }
    for (InetSocketAddress addr : otherJournalNodes) {
      try {
        otherJNProxies.add(new JournalNodeProxy(addr));
      } catch (IOException e) {
        LOG.warn("Could not add proxy for Journal at addresss " + addr, e);
      }
    }
    if (otherJNProxies.isEmpty()) {
      LOG.error("Cannot sync as there is no other JN available for sync.");
      return false;
    }
    numOtherJNs = otherJNProxies.size();
    return true;
  }

  private void startSyncJournalsDaemon() {
    syncJournalDaemon = new Daemon(() -> {
      // Wait for journal to be formatted to create edits.sync directory
      while(!journal.isFormatted()) {
        try {
          Thread.sleep(journalSyncInterval);
        } catch (InterruptedException e) {
          LOG.error("JournalNodeSyncer daemon received Runtime exception.", e);
          Thread.currentThread().interrupt();
          return;
        }
      }
      if (!createEditsSyncDir()) {
        LOG.error("Failed to create directory for downloading log " +
                "segments: %s. Stopping Journal Node Sync.",
            journal.getStorage().getEditsSyncDir());
        return;
      }
      while(shouldSync) {
        try {
          if (!journal.isFormatted()) {
            LOG.warn("Journal cannot sync. Not formatted.");
          } else {
            syncJournals();
          }
        } catch (Throwable t) {
          if (!shouldSync) {
            if (t instanceof InterruptedException) {
              LOG.info("Stopping JournalNode Sync.");
              Thread.currentThread().interrupt();
              return;
            } else {
              LOG.warn("JournalNodeSyncer received an exception while " +
                  "shutting down.", t);
            }
            break;
          } else {
            if (t instanceof InterruptedException) {
              LOG.warn("JournalNodeSyncer interrupted", t);
              Thread.currentThread().interrupt();
              return;
            }
          }
          LOG.error(
              "JournalNodeSyncer daemon received Runtime exception. ", t);
        }
        try {
          Thread.sleep(journalSyncInterval);
        } catch (InterruptedException e) {
          if (!shouldSync) {
            LOG.info("Stopping JournalNode Sync.");
          } else {
            LOG.warn("JournalNodeSyncer interrupted", e);
          }
          Thread.currentThread().interrupt();
          return;
        }
      }
    });
    syncJournalDaemon.start();
  }

  private void syncJournals() {
    syncWithJournalAtIndex(journalNodeIndexForSync);
    journalNodeIndexForSync = (journalNodeIndexForSync + 1) % numOtherJNs;
  }

  private void syncWithJournalAtIndex(int index) {
    LOG.info("Syncing Journal " + jn.getBoundIpcAddress().getAddress() + ":"
        + jn.getBoundIpcAddress().getPort() + " with "
        + otherJNProxies.get(index) + ", journal id: " + jid);
    final QJournalProtocolPB jnProxy = otherJNProxies.get(index).jnProxy;
    if (jnProxy == null) {
      LOG.error("JournalNode Proxy not found.");
      return;
    }

    List<RemoteEditLog> thisJournalEditLogs;
    try {
      thisJournalEditLogs = journal.getEditLogManifest(0, false).getLogs();
    } catch (IOException e) {
      LOG.error("Exception in getting local edit log manifest", e);
      return;
    }

    GetEditLogManifestResponseProto editLogManifest;
    try {
      editLogManifest = jnProxy.getEditLogManifest(null,
          GetEditLogManifestRequestProto.newBuilder().setJid(jidProto)
              .setSinceTxId(0)
              .setInProgressOk(false).build());
    } catch (ServiceException e) {
      LOG.error("Could not sync with Journal at " +
          otherJNProxies.get(journalNodeIndexForSync), e);
      return;
    }

    getMissingLogSegments(thisJournalEditLogs, editLogManifest,
        otherJNProxies.get(index));
  }

  private List<InetSocketAddress> getOtherJournalNodeAddrs() {
    String uriStr = "";
    try {
      uriStr = conf.getTrimmed(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY);

      if (uriStr == null || uriStr.isEmpty()) {
        if (nameServiceId != null) {
          uriStr = conf.getTrimmed(DFSConfigKeys
              .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + "." + nameServiceId);
        }
      }

      if (uriStr == null || uriStr.isEmpty()) {
        HashSet<String> sharedEditsUri = Sets.newHashSet();
        if (nameServiceId != null) {
          Collection<String> nnIds = DFSUtilClient.getNameNodeIds(
              conf, nameServiceId);
          for (String nnId : nnIds) {
            String suffix = nameServiceId + "." + nnId;
            uriStr = conf.getTrimmed(DFSConfigKeys
                .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + "." + suffix);
            sharedEditsUri.add(uriStr);
          }
          if (sharedEditsUri.size() > 1) {
            uriStr = null;
            LOG.error("The conf property " + DFSConfigKeys
                .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + " not set properly, " +
                "it has been configured with different journalnode values " +
                sharedEditsUri.toString() + " for a" +
                " single nameserviceId" + nameServiceId);
          }
        }
      }

      if (uriStr == null || uriStr.isEmpty()) {
        LOG.error("Could not construct Shared Edits Uri");
        return null;
      } else {
        return getJournalAddrList(uriStr);
      }

    } catch (URISyntaxException e) {
      LOG.error("The conf property " + DFSConfigKeys
          .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + " not set properly.");
    } catch (IOException e) {
      LOG.error("Could not parse JournalNode addresses: " + uriStr);
    }
    return null;
  }

  private List<InetSocketAddress> getJournalAddrList(String uriStr) throws
      URISyntaxException,
      IOException {
    URI uri = new URI(uriStr);
    return Util.getLoggerAddresses(uri,
        Sets.newHashSet(jn.getBoundIpcAddress()));
  }

  private JournalIdProto convertJournalId(String journalId) {
    return QJournalProtocolProtos.JournalIdProto.newBuilder()
      .setIdentifier(journalId)
      .build();
  }

  private void getMissingLogSegments(List<RemoteEditLog> thisJournalEditLogs,
      GetEditLogManifestResponseProto response,
      JournalNodeProxy remoteJNproxy) {

    List<RemoteEditLog> otherJournalEditLogs = PBHelper.convert(
        response.getManifest()).getLogs();
    if (otherJournalEditLogs == null || otherJournalEditLogs.isEmpty()) {
      LOG.warn("Journal at " + remoteJNproxy.jnAddr + " has no edit logs");
      return;
    }
    List<RemoteEditLog> missingLogs = getMissingLogList(thisJournalEditLogs,
        otherJournalEditLogs);

    if (!missingLogs.isEmpty()) {
      NamespaceInfo nsInfo = jnStorage.getNamespaceInfo();

      for (RemoteEditLog missingLog : missingLogs) {
        URL url = null;
        boolean success = false;
        try {
          if (remoteJNproxy.httpServerUrl == null) {
            if (response.hasFromURL()) {
              remoteJNproxy.httpServerUrl = getHttpServerURI(
                  response.getFromURL(), remoteJNproxy.jnAddr.getHostName());
            } else {
              LOG.error("EditLogManifest response does not have fromUrl " +
                  "field set. Aborting current sync attempt");
              break;
            }
          }

          String urlPath = GetJournalEditServlet.buildPath(jid, missingLog
              .getStartTxId(), nsInfo, false);
          url = new URL(remoteJNproxy.httpServerUrl, urlPath);
          success = downloadMissingLogSegment(url, missingLog);
        } catch (URISyntaxException e) {
          LOG.error("EditLogManifest's fromUrl field syntax incorrect", e);
        } catch (MalformedURLException e) {
          LOG.error("MalformedURL when download missing log segment", e);
        } catch (Exception e) {
          LOG.error("Exception in downloading missing log segment from url " +
              url, e);
        }
        if (!success) {
          LOG.error("Aborting current sync attempt.");
          break;
        }
      }
    }
  }

  /**
   *  Returns the logs present in otherJournalEditLogs and missing from
   *  thisJournalEditLogs.
   */
  private List<RemoteEditLog> getMissingLogList(
      List<RemoteEditLog> thisJournalEditLogs,
      List<RemoteEditLog> otherJournalEditLogs) {
    if (thisJournalEditLogs.isEmpty()) {
      return otherJournalEditLogs;
    }

    List<RemoteEditLog> missingEditLogs = Lists.newArrayList();

    int localJnIndex = 0, remoteJnIndex = 0;
    int localJnNumLogs = thisJournalEditLogs.size();
    int remoteJnNumLogs = otherJournalEditLogs.size();

    while (localJnIndex < localJnNumLogs && remoteJnIndex < remoteJnNumLogs) {
      long localJNstartTxId = thisJournalEditLogs.get(localJnIndex)
          .getStartTxId();
      long remoteJNstartTxId = otherJournalEditLogs.get(remoteJnIndex)
          .getStartTxId();

      if (localJNstartTxId == remoteJNstartTxId) {
        localJnIndex++;
        remoteJnIndex++;
      } else if (localJNstartTxId > remoteJNstartTxId) {
        missingEditLogs.add(otherJournalEditLogs.get(remoteJnIndex));
        remoteJnIndex++;
      } else {
        localJnIndex++;
      }
    }

    if (remoteJnIndex < remoteJnNumLogs) {
      for (; remoteJnIndex < remoteJnNumLogs; remoteJnIndex++) {
        missingEditLogs.add(otherJournalEditLogs.get(remoteJnIndex));
      }
    }

    return missingEditLogs;
  }

  private URL getHttpServerURI(String fromUrl, String hostAddr)
      throws URISyntaxException, MalformedURLException {
    URI uri = new URI(fromUrl);
    return new URL(uri.getScheme(), hostAddr, uri.getPort(), "");
  }

  /**
   * Transfer an edit log from one journal node to another for sync-up.
   */
  private boolean downloadMissingLogSegment(URL url, RemoteEditLog log)
      throws IOException {
    LOG.info("Downloading missing Edit Log from " + url + " to " + jnStorage
        .getRoot());

    assert log.getStartTxId() > 0 && log.getEndTxId() > 0 : "bad log: " + log;
    File finalEditsFile = jnStorage.getFinalizedEditsFile(log.getStartTxId(),
        log.getEndTxId());

    if (finalEditsFile.exists() && FileUtil.canRead(finalEditsFile)) {
      LOG.info("Skipping download of remote edit log " + log + " since it's" +
          " already stored locally at " + finalEditsFile);
      return true;
    }

    // Download the log segment to current.tmp directory first.
    File tmpEditsFile = jnStorage.getTemporaryEditsFile(
        log.getStartTxId(), log.getEndTxId());

    try {
      Util.doGetUrl(url, ImmutableList.of(tmpEditsFile), jnStorage, false,
          logSegmentTransferTimeout, throttler);
    } catch (IOException e) {
      LOG.error("Download of Edit Log file for Syncing failed. Deleting temp " +
          "file: " + tmpEditsFile);
      if (!tmpEditsFile.delete()) {
        LOG.warn("Deleting " + tmpEditsFile + " has failed");
      }
      return false;
    }
    LOG.info("Downloaded file " + tmpEditsFile.getName() + " of size " +
        tmpEditsFile.length() + " bytes.");

    boolean moveSuccess = false;
    try {
      moveSuccess = journal.moveTmpSegmentToCurrent(tmpEditsFile,
          finalEditsFile, log.getEndTxId());
    } catch (IOException e) {
      LOG.info("Could not move %s to current directory.", tmpEditsFile);
    } finally {
      if (tmpEditsFile.exists() && !tmpEditsFile.delete()) {
        LOG.warn("Deleting " + tmpEditsFile + " has failed");
      }
    }
    if (moveSuccess) {
      metrics.incrNumEditLogsSynced();
      return true;
    } else {
      return false;
    }
  }

  private static DataTransferThrottler getThrottler(Configuration conf) {
    long transferBandwidth =
        conf.getLong(DFSConfigKeys.DFS_EDIT_LOG_TRANSFER_RATE_KEY,
            DFSConfigKeys.DFS_EDIT_LOG_TRANSFER_RATE_DEFAULT);
    DataTransferThrottler throttler = null;
    if (transferBandwidth > 0) {
      throttler = new DataTransferThrottler(transferBandwidth);
    }
    return throttler;
  }

  private class JournalNodeProxy {
    private final InetSocketAddress jnAddr;
    private final QJournalProtocolPB jnProxy;
    private URL httpServerUrl;

    JournalNodeProxy(InetSocketAddress jnAddr) throws IOException {
      this.jnAddr = jnAddr;
      this.jnProxy = RPC.getProxy(QJournalProtocolPB.class,
          RPC.getProtocolVersion(QJournalProtocolPB.class), jnAddr, conf);
    }

    @Override
    public String toString() {
      return jnAddr.toString();
    }
  }
}
