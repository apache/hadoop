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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.DiskChecker;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.now;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.tracing.Tracer;
import org.eclipse.jetty.util.ajax.JSON;

import javax.management.ObjectName;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The JournalNode is a daemon which allows namenodes using
 * the QuorumJournalManager to log and retrieve edits stored
 * remotely. It is a thin wrapper around a local edit log
 * directory with the addition of facilities to participate
 * in the quorum protocol.
 */
@InterfaceAudience.Private
public class JournalNode implements Tool, Configurable, JournalNodeMXBean {
  public static final Logger LOG = LoggerFactory.getLogger(JournalNode.class);
  private Configuration conf;
  private JournalNodeRpcServer rpcServer;
  private JournalNodeHttpServer httpServer;
  private final Map<String, Journal> journalsById = Maps.newHashMap();
  private final Map<String, JournalNodeSyncer> journalSyncersById = Maps
      .newHashMap();
  private ObjectName journalNodeInfoBeanName;
  private String httpServerURI;
  private final ArrayList<File> localDir = Lists.newArrayList();
  Tracer tracer;
  private long startTime = 0;

  static {
    HdfsConfiguration.init();
  }
  
  /**
   * When stopped, the daemon will exit with this code. 
   */
  private int resultCode = 0;

  synchronized Journal getOrCreateJournal(String jid,
                                          String nameServiceId,
                                          StartupOption startOpt)
      throws IOException {
    QuorumJournalManager.checkJournalId(jid);
    
    Journal journal = journalsById.get(jid);
    if (journal == null) {
      File logDir = getLogDir(jid, nameServiceId);
      LOG.info("Initializing journal in directory " + logDir);
      journal = new Journal(conf, logDir, jid, startOpt, new ErrorReporter());
      journalsById.put(jid, journal);
      // Start SyncJouranl thread, if JournalNode Sync is enabled
      if (conf.getBoolean(
          DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_KEY,
          DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_DEFAULT)) {
        startSyncer(journal, jid, nameServiceId);
      }
    } else if (journalSyncersById.get(jid) != null &&
        !journalSyncersById.get(jid).isJournalSyncerStarted() &&
        !journalsById.get(jid).getTriedJournalSyncerStartedwithnsId() &&
        nameServiceId != null) {
      startSyncer(journal, jid, nameServiceId);
    }


    return journal;
  }

  @VisibleForTesting
  public JournalNodeSyncer getJournalSyncer(String jid) {
    return journalSyncersById.get(jid);
  }

  @VisibleForTesting
  public boolean getJournalSyncerStatus(String jid) {
    if (journalSyncersById.get(jid) != null) {
      return journalSyncersById.get(jid).isJournalSyncerStarted();
    } else {
      return false;
    }
  }

  private void startSyncer(Journal journal, String jid, String nameServiceId) {
    JournalNodeSyncer jSyncer = journalSyncersById.get(jid);
    if (jSyncer == null) {
      jSyncer = new JournalNodeSyncer(this, journal, jid, conf, nameServiceId);
      journalSyncersById.put(jid, jSyncer);
    }
    jSyncer.start(nameServiceId);
  }

  @VisibleForTesting
  public Journal getOrCreateJournal(String jid) throws IOException {
    return getOrCreateJournal(jid, null, StartupOption.REGULAR);
  }

  public Journal getOrCreateJournal(String jid,
                                    String nameServiceId)
      throws IOException {
    return getOrCreateJournal(jid, nameServiceId, StartupOption.REGULAR);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    String journalNodeDir = null;
    Collection<String> nameserviceIds;

    nameserviceIds = conf.getTrimmedStringCollection(
        DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);

    if (nameserviceIds.size() == 0) {
      nameserviceIds = conf.getTrimmedStringCollection(
          DFSConfigKeys.DFS_NAMESERVICES);
    }

    //if nameservicesIds size is less than 2, it means it is not a federated
    // setup
    if (nameserviceIds.size() < 2) {
      // Check in HA, if journal edit dir is set by appending with
      // nameserviceId
      for (String nameService : nameserviceIds) {
        journalNodeDir = conf.get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY +
        "." + nameService);
      }
      if (journalNodeDir == null) {
        journalNodeDir = conf.get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
            DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_DEFAULT);
      }
      localDir.add(new File(journalNodeDir.trim()));
    }

    if (this.tracer == null) {
      this.tracer = new Tracer.Builder().
          conf(TraceUtils.wrapHadoopConf("journalnode.htrace", conf)).
          build();
    }
  }

  private static void validateAndCreateJournalDir(File dir)
      throws IOException {

    if (!dir.isAbsolute()) {
      throw new IllegalArgumentException(
          "Journal dir '" + dir + "' should be an absolute path");
    }
    DiskChecker.checkDir(dir);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    start();
    return join();
  }

  /**
   * Start listening for edits via RPC.
   */
  public void start() throws IOException {
    Preconditions.checkState(!isStarted(), "JN already running");

    try {

      for (File journalDir : localDir) {
        validateAndCreateJournalDir(journalDir);
      }
      DefaultMetricsSystem.initialize("JournalNode");
      JvmMetrics.create("JournalNode",
          conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY),
          DefaultMetricsSystem.instance());

      InetSocketAddress socAddr = JournalNodeRpcServer.getAddress(conf);
      SecurityUtil.login(conf, DFSConfigKeys.DFS_JOURNALNODE_KEYTAB_FILE_KEY,
          DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY,
          socAddr.getHostName());

      registerJNMXBean();

      httpServer = new JournalNodeHttpServer(conf, this,
          getHttpServerBindAddress(conf));
      httpServer.start();

      httpServerURI = httpServer.getServerURI().toString();

      rpcServer = new JournalNodeRpcServer(conf, this);
      rpcServer.start();
      startTime = now();
    } catch (IOException ioe) {
      //Shutdown JournalNode of JournalNodeRpcServer fails to start
      LOG.error("Failed to start JournalNode.", ioe);
      this.stop(1);
      throw ioe;
    }
  }

  public boolean isStarted() {
    return rpcServer != null;
  }

  /**
   * @return the address the IPC server is bound to
   */
  public InetSocketAddress getBoundIpcAddress() {
    return rpcServer.getAddress();
  }

  public String getHttpServerURI() {
    return httpServerURI;
  }

  /**
   * Stop the daemon with the given status code
   * @param rc the status code with which to exit (non-zero
   * should indicate an error)
   */
  public void stop(int rc) {
    this.resultCode = rc;

    for (JournalNodeSyncer jSyncer : journalSyncersById.values()) {
      jSyncer.stopSync();
    }

    if (rpcServer != null) { 
      rpcServer.stop();
    }

    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (IOException ioe) {
        LOG.warn("Unable to stop HTTP server for " + this, ioe);
      }
    }
    
    for (Journal j : journalsById.values()) {
      IOUtils.cleanupWithLogger(LOG, j);
    }

    DefaultMetricsSystem.shutdown();

    if (journalNodeInfoBeanName != null) {
      MBeans.unregister(journalNodeInfoBeanName);
      journalNodeInfoBeanName = null;
    }
    if (tracer != null) {
      tracer.close();
      tracer = null;
    }
  }

  /**
   * Wait for the daemon to exit.
   * @return the result code (non-zero if error)
   */
  int join() throws InterruptedException {
    if (rpcServer != null) {
      rpcServer.join();
    }
    return resultCode;
  }
  
  public void stopAndJoin(int rc) throws InterruptedException {
    stop(rc);
    join();
  }

  /**
   * Return the directory inside our configured storage
   * dir which corresponds to a given journal. 
   * @param jid the journal identifier
   * @return the file, which may or may not exist yet
   */
  private File getLogDir(String jid, String nameServiceId) throws IOException{
    String dir = null;
    if (nameServiceId != null) {
      dir = conf.get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY + "." +
          nameServiceId);
    }
    if (dir == null) {
      dir = conf.get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
          DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_DEFAULT);
    }

    File journalDir = new File(dir.trim());
    if (!localDir.contains(journalDir)) {
      //It is a federated setup, we need to validate journalDir
      validateAndCreateJournalDir(journalDir);
      localDir.add(journalDir);
    }

    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty(),
        "bad journal identifier: %s", jid);
    assert jid != null;
    return new File(journalDir, jid);
  }



  @Override // JournalNodeMXBean
  public String getJournalsStatus() {
    // jid:{Formatted:True/False}
    Map<String, Map<String, String>> status = 
        new HashMap<String, Map<String, String>>();
    synchronized (this) {
      for (Map.Entry<String, Journal> entry : journalsById.entrySet()) {
        Map<String, String> jMap = new HashMap<String, String>();
        jMap.put("Formatted", Boolean.toString(entry.getValue().isFormatted()));
        status.put(entry.getKey(), jMap);
      }
    }
    
    // It is possible that some journals have been formatted before, while the 
    // corresponding journals are not in journalsById yet (because of restarting
    // JN, e.g.). For simplicity, let's just assume a journal is formatted if
    // there is a directory for it. We can also call analyzeStorage method for
    // these directories if necessary.
    // Also note that we do not need to check localDir here since
    // validateAndCreateJournalDir has been called before we register the
    // MXBean.
    for (File jDir : localDir) {
      File[] journalDirs = jDir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File file) {
          return file.isDirectory();
        }
      });

      if (journalDirs != null) {
        for (File journalDir : journalDirs) {
          String jid = journalDir.getName();
          if (!status.containsKey(jid)) {
            Map<String, String> jMap = new HashMap<String, String>();
            jMap.put("Formatted", "true");
            status.put(jid, jMap);
          }
        }
      }
    }

    return JSON.toString(status);
  }

  @Override // JournalNodeMXBean
  public String getHostAndPort() {
    return NetUtils.getHostPortString(rpcServer.getAddress());
  }

  @Override // JournalNodeMXBean
  public List<String> getClusterIds() {
    return journalsById.values().stream()
        .map(j -> j.getStorage().getClusterID())
        .filter(cid -> !Strings.isNullOrEmpty(cid))
        .distinct().collect(Collectors.toList());
  }

  @Override // JournalNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
  }

  @Override // JournalNodeMXBean
  public long getJNStartedTimeInMillis() {
    return this.startTime;
  }

  @Override
  // JournalNodeMXBean
  public List<String> getStorageInfos() {
    return journalsById.values().stream()
        .map(journal -> journal.getStorage().toMapString())
        .collect(Collectors.toList());
  }

  /**
   * Register JournalNodeMXBean
   */
  private void registerJNMXBean() {
    journalNodeInfoBeanName = MBeans.register("JournalNode", "JournalNodeInfo", this);
  }

  private class ErrorReporter implements StorageErrorReporter {
    @Override
    public void reportErrorOnFile(File f) {
      LOG.error("Error reported on file " + f + "... exiting",
          new Exception());
      stop(1);
    }
  }

  public static void main(String[] args) throws Exception {
    StringUtils.startupShutdownMessage(JournalNode.class, args, LOG);
    try {
      System.exit(ToolRunner.run(new JournalNode(), args));
    } catch (Throwable e) {
      LOG.error("Failed to start journalnode.", e);
      terminate(-1, e);
    }
  }

  public void doPreUpgrade(String journalId) throws IOException {
    getOrCreateJournal(journalId).doPreUpgrade();
  }

  public void doUpgrade(String journalId, StorageInfo sInfo) throws IOException {
    getOrCreateJournal(journalId).doUpgrade(sInfo);
  }

  public void doFinalize(String journalId,
                         String nameServiceId)
      throws IOException {
    getOrCreateJournal(journalId, nameServiceId).doFinalize();
  }

  public Boolean canRollBack(String journalId, StorageInfo storage,
      StorageInfo prevStorage, int targetLayoutVersion,
      String nameServiceId) throws IOException {
    return getOrCreateJournal(journalId,
        nameServiceId, StartupOption.ROLLBACK).canRollBack(
        storage, prevStorage, targetLayoutVersion);
  }

  public void doRollback(String journalId,
                         String nameServiceId) throws IOException {
    getOrCreateJournal(journalId,
        nameServiceId, StartupOption.ROLLBACK).doRollback();
  }

  public void discardSegments(String journalId, long startTxId,
                              String nameServiceId)
      throws IOException {
    getOrCreateJournal(journalId, nameServiceId).discardSegments(startTxId);
  }

  public Long getJournalCTime(String journalId,
                              String nameServiceId) throws IOException {
    return getOrCreateJournal(journalId, nameServiceId).getJournalCTime();
  }

  @VisibleForTesting
  public Journal getJournal(String  jid) {
    return journalsById.get(jid);
  }

  public static InetSocketAddress getHttpAddress(Configuration conf) {
    String addr = conf.get(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr,
        DFSConfigKeys.DFS_JOURNALNODE_HTTP_PORT_DEFAULT,
        DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY);
  }

  protected InetSocketAddress getHttpServerBindAddress(
      Configuration configuration) {
    InetSocketAddress bindAddress = getHttpAddress(configuration);

    // If DFS_JOURNALNODE_HTTP_BIND_HOST_KEY exists then it overrides the
    // host name portion of DFS_JOURNALNODE_HTTP_ADDRESS_KEY.
    final String bindHost = configuration.getTrimmed(
        DFS_JOURNALNODE_HTTP_BIND_HOST_KEY);
    if (bindHost != null && !bindHost.isEmpty()) {
      bindAddress = new InetSocketAddress(bindHost, bindAddress.getPort());
    }

    return bindAddress;
  }

  @VisibleForTesting
  public JournalNodeRpcServer getRpcServer() {
    return rpcServer;
  }


  /**
   * @return the actual JournalNode HTTP/HTTPS address.
   */
  public InetSocketAddress getBoundHttpAddress() {
    return httpServer.getAddress();
  }

  /**
   * @return JournalNode HTTP address
   */
  public InetSocketAddress getHttpAddress() {
    return httpServer.getHttpAddress();
  }

  /**
   * @return JournalNode HTTPS address
   */
  public InetSocketAddress getHttpsAddress() {
    return httpServer.getHttpsAddress();
  }
}
