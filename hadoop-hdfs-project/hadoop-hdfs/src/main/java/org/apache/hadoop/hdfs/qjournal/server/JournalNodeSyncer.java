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
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
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

  JournalNodeSyncer(JournalNode jouranlNode, Journal journal, String jid,
      Configuration conf) {
    this.jn = jouranlNode;
    this.journal = journal;
    this.jid = jid;
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
  }

  void stopSync() {
    shouldSync = false;
    if (syncJournalDaemon != null) {
      syncJournalDaemon.interrupt();
    }
  }

  public void start() {
    LOG.info("Starting SyncJournal daemon for journal " + jid);
    if (getOtherJournalNodeProxies()) {
      startSyncJournalsDaemon();
    } else {
      LOG.warn("Failed to start SyncJournal daemon for journal " + jid);
    }
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
    syncJournalDaemon = new Daemon(new Runnable() {
      @Override
      public void run() {
        while(shouldSync) {
          try {
            if (!journal.isFormatted()) {
              LOG.warn("Journal not formatted. Cannot sync.");
            } else {
              syncJournals();
            }
            Thread.sleep(journalSyncInterval);
          } catch (Throwable t) {
            if (!shouldSync) {
              if (t instanceof InterruptedException) {
                LOG.info("Stopping JournalNode Sync.");
              } else {
                LOG.warn("JournalNodeSyncer received an exception while " +
                    "shutting down.", t);
              }
              break;
            } else {
              if (t instanceof InterruptedException) {
                LOG.warn("JournalNodeSyncer interrupted", t);
                break;
              }
            }
            LOG.error(
                "JournalNodeSyncer daemon received Runtime exception. ", t);
          }
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
    URI uri = null;
    try {
      String uriStr = conf.get(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
      if (uriStr == null || uriStr.isEmpty()) {
        LOG.warn("Could not construct Shared Edits Uri");
        return null;
      }
      uri = new URI(uriStr);
      return Util.getLoggerAddresses(uri,
          Sets.newHashSet(jn.getBoundIpcAddress()));
    } catch (URISyntaxException e) {
      LOG.error("The conf property " + DFSConfigKeys
          .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + " not set properly.");
    } catch (IOException e) {
      LOG.error("Could not parse JournalNode addresses: " + uri);
    }
    return null;
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
              URI uri = URI.create(response.getFromURL());
              remoteJNproxy.httpServerUrl = getHttpServerURI(uri.getScheme(),
                  uri.getHost(), uri.getPort());
            } else {
              remoteJNproxy.httpServerUrl = getHttpServerURI("http",
                  remoteJNproxy.jnAddr.getHostName(), response.getHttpPort());
            }
          }

          String urlPath = GetJournalEditServlet.buildPath(jid, missingLog
              .getStartTxId(), nsInfo);
          url = new URL(remoteJNproxy.httpServerUrl, urlPath);
          success = downloadMissingLogSegment(url, missingLog);
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

    int thisJnIndex = 0, otherJnIndex = 0;
    int thisJnNumLogs = thisJournalEditLogs.size();
    int otherJnNumLogs = otherJournalEditLogs.size();

    while (thisJnIndex < thisJnNumLogs && otherJnIndex < otherJnNumLogs) {
      long localJNstartTxId = thisJournalEditLogs.get(thisJnIndex)
          .getStartTxId();
      long remoteJNstartTxId = otherJournalEditLogs.get(otherJnIndex)
          .getStartTxId();

      if (localJNstartTxId == remoteJNstartTxId) {
        thisJnIndex++;
        otherJnIndex++;
      } else if (localJNstartTxId > remoteJNstartTxId) {
        missingEditLogs.add(otherJournalEditLogs.get(otherJnIndex));
        otherJnIndex++;
      } else {
        thisJnIndex++;
      }
    }

    if (otherJnIndex < otherJnNumLogs) {
      for (; otherJnIndex < otherJnNumLogs; otherJnIndex++) {
        missingEditLogs.add(otherJournalEditLogs.get(otherJnIndex));
      }
    }

    return missingEditLogs;
  }

  private URL getHttpServerURI(String scheme, String hostname, int port)
    throws MalformedURLException {
    return new URL(scheme, hostname, port, "");
  }

  /**
   * Transfer an edit log from one journal node to another for sync-up.
   */
  private boolean downloadMissingLogSegment(URL url, RemoteEditLog log) throws
      IOException {
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

    final long milliTime = Time.monotonicNow();
    File tmpEditsFile = jnStorage.getTemporaryEditsFile(log.getStartTxId(), log
        .getEndTxId(), milliTime);
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

    LOG.debug("Renaming " + tmpEditsFile.getName() + " to "
        + finalEditsFile.getName());
    boolean renameSuccess = journal.renameTmpSegment(tmpEditsFile,
        finalEditsFile, log.getEndTxId());
    if (!renameSuccess) {
      //If rename is not successful, delete the tmpFile
      LOG.debug("Renaming unsuccessful. Deleting temporary file: "
          + tmpEditsFile);
      if (!tmpEditsFile.delete()) {
        LOG.warn("Deleting " + tmpEditsFile + " has failed");
      }
      return false;
    }
    return true;
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
