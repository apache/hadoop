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

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.core.Tracer;
import org.eclipse.jetty.util.ajax.JSON;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * The JournalNode is a daemon which allows namenodes using
 * the QuorumJournalManager to log and retrieve edits stored
 * remotely. It is a thin wrapper around a local edit log
 * directory with the addition of facilities to participate
 * in the quorum protocol.
 */
@InterfaceAudience.Private
public class JournalNode implements Tool, Configurable, JournalNodeMXBean {
  public static final Log LOG = LogFactory.getLog(JournalNode.class);
  private Configuration conf;
  private JournalNodeRpcServer rpcServer;
  private JournalNodeHttpServer httpServer;
  private final Map<String, Journal> journalsById = Maps.newHashMap();
  private final Map<String, JournalNodeSyncer> journalSyncersById = Maps
      .newHashMap();
  private ObjectName journalNodeInfoBeanName;
  private String httpServerURI;
  private File localDir;
  Tracer tracer;

  static {
    HdfsConfiguration.init();
  }
  
  /**
   * When stopped, the daemon will exit with this code. 
   */
  private int resultCode = 0;

  synchronized Journal getOrCreateJournal(String jid, StartupOption startOpt)
      throws IOException {
    QuorumJournalManager.checkJournalId(jid);
    
    Journal journal = journalsById.get(jid);
    if (journal == null) {
      File logDir = getLogDir(jid);
      LOG.info("Initializing journal in directory " + logDir);      
      journal = new Journal(conf, logDir, jid, startOpt, new ErrorReporter());
      journalsById.put(jid, journal);

      // Start SyncJouranl thread, if JournalNode Sync is enabled
      if (conf.getBoolean(
          DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_KEY,
          DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_DEFAULT)) {
        startSyncer(journal, jid);
      }
    }

    return journal;
  }

  private void startSyncer(Journal journal, String jid) {
    JournalNodeSyncer jSyncer = new JournalNodeSyncer(this, journal, jid, conf);
    journalSyncersById.put(jid, jSyncer);
    jSyncer.start();
  }

  @VisibleForTesting
  public Journal getOrCreateJournal(String jid) throws IOException {
    return getOrCreateJournal(jid, StartupOption.REGULAR);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.localDir = new File(
        conf.get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_DEFAULT).trim());
    if (this.tracer == null) {
      this.tracer = new Tracer.Builder("JournalNode").
          conf(TraceUtils.wrapHadoopConf("journalnode.htrace", conf)).
          build();
    }
  }

  private static void validateAndCreateJournalDir(File dir) throws IOException {
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

      validateAndCreateJournalDir(localDir);

      DefaultMetricsSystem.initialize("JournalNode");
      JvmMetrics.create("JournalNode",
          conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY),
          DefaultMetricsSystem.instance());

      InetSocketAddress socAddr = JournalNodeRpcServer.getAddress(conf);
      SecurityUtil.login(conf, DFSConfigKeys.DFS_JOURNALNODE_KEYTAB_FILE_KEY,
          DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY,
          socAddr.getHostName());

      registerJNMXBean();

      httpServer = new JournalNodeHttpServer(conf, this);
      httpServer.start();

      httpServerURI = httpServer.getServerURI().toString();

      rpcServer = new JournalNodeRpcServer(conf, this);
      rpcServer.start();
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
  
  @Deprecated
  public InetSocketAddress getBoundHttpAddress() {
    return httpServer.getAddress();
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
      IOUtils.cleanup(LOG, j);
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
  private File getLogDir(String jid) {
    String dir = conf.get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_DEFAULT);
    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty(),
        "bad journal identifier: %s", jid);
    assert jid != null;
    return new File(new File(dir), jid);
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
    File[] journalDirs = localDir.listFiles(new FileFilter() {
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

    return JSON.toString(status);
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
      LOG.fatal("Error reported on file " + f + "... exiting",
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

  public void doFinalize(String journalId) throws IOException {
    getOrCreateJournal(journalId).doFinalize();
  }

  public Boolean canRollBack(String journalId, StorageInfo storage,
      StorageInfo prevStorage, int targetLayoutVersion) throws IOException {
    return getOrCreateJournal(journalId, StartupOption.ROLLBACK).canRollBack(
        storage, prevStorage, targetLayoutVersion);
  }

  public void doRollback(String journalId) throws IOException {
    getOrCreateJournal(journalId, StartupOption.ROLLBACK).doRollback();
  }

  public void discardSegments(String journalId, long startTxId)
      throws IOException {
    getOrCreateJournal(journalId).discardSegments(startTxId);
  }

  public Long getJournalCTime(String journalId) throws IOException {
    return getOrCreateJournal(journalId).getJournalCTime();
  }
}
