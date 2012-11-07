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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
public class JournalNode implements Tool, Configurable {
  public static final Log LOG = LogFactory.getLog(JournalNode.class);
  private Configuration conf;
  private JournalNodeRpcServer rpcServer;
  private JournalNodeHttpServer httpServer;
  private Map<String, Journal> journalsById = Maps.newHashMap();

  private File localDir;

  static {
    HdfsConfiguration.init();
  }
  
  /**
   * When stopped, the daemon will exit with this code. 
   */
  private int resultCode = 0;

  synchronized Journal getOrCreateJournal(String jid) throws IOException {
    QuorumJournalManager.checkJournalId(jid);
    
    Journal journal = journalsById.get(jid);
    if (journal == null) {
      File logDir = getLogDir(jid);
      LOG.info("Initializing journal in directory " + logDir);      
      journal = new Journal(logDir, jid, new ErrorReporter());
      journalsById.put(jid, journal);
    }
    
    return journal;
  }


  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.localDir = new File(
        conf.get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_DEFAULT).trim());
  }

  private static void validateAndCreateJournalDir(File dir) throws IOException {
    if (!dir.isAbsolute()) {
      throw new IllegalArgumentException(
          "Journal dir '" + dir + "' should be an absolute path");
    }

    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Could not create journal dir '" +
          dir + "'");
    } else if (!dir.isDirectory()) {
      throw new IOException("Journal directory '" + dir + "' is not " +
          "a directory");
    }
    
    if (!dir.canWrite()) {
      throw new IOException("Unable to write to journal dir '" +
          dir + "'");
    }
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
    
    validateAndCreateJournalDir(localDir);
    
    DefaultMetricsSystem.initialize("JournalNode");
    JvmMetrics.create("JournalNode",
        conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY),
        DefaultMetricsSystem.instance());

    InetSocketAddress socAddr = JournalNodeRpcServer.getAddress(conf);
    SecurityUtil.login(conf, DFSConfigKeys.DFS_JOURNALNODE_KEYTAB_FILE_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_USER_NAME_KEY, socAddr.getHostName());
    
    httpServer = new JournalNodeHttpServer(conf, this);
    httpServer.start();

    rpcServer = new JournalNodeRpcServer(conf, this);
    rpcServer.start();
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
  

  public InetSocketAddress getBoundHttpAddress() {
    return httpServer.getAddress();
  }


  /**
   * Stop the daemon with the given status code
   * @param rc the status code with which to exit (non-zero
   * should indicate an error)
   */
  public void stop(int rc) {
    this.resultCode = rc;
    
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
    return new File(new File(dir), jid);
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
    System.exit(ToolRunner.run(new JournalNode(), args));
  }
}
