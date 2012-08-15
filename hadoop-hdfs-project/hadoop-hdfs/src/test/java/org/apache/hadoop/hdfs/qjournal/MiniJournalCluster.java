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
package org.apache.hadoop.hdfs.qjournal;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class MiniJournalCluster {
  public static class Builder {
    private String baseDir;
    private int numJournalNodes = 3;
    private boolean format = true;
    private Configuration conf;
    
    public Builder(Configuration conf) {
      this.conf = conf;
    }
    
    public Builder baseDir(String d) {
      this.baseDir = d;
      return this;
    }
    
    public Builder numJournalNodes(int n) {
      this.numJournalNodes = n;
      return this;
    }

    public Builder format(boolean f) {
      this.format = f;
      return this;
    }

    public MiniJournalCluster build() throws IOException {
      return new MiniJournalCluster(this);
    }
  }

  private static final Log LOG = LogFactory.getLog(MiniJournalCluster.class);
  private File baseDir;
  private JournalNode nodes[];
  private InetSocketAddress ipcAddrs[];
  private InetSocketAddress httpAddrs[];
  
  private MiniJournalCluster(Builder b) throws IOException {
    LOG.info("Starting MiniJournalCluster with " +
        b.numJournalNodes + " journal nodes");
    
    if (b.baseDir != null) {
      this.baseDir = new File(b.baseDir);
    } else {
      this.baseDir = new File(MiniDFSCluster.getBaseDirectory());
    }
    
    nodes = new JournalNode[b.numJournalNodes];
    ipcAddrs = new InetSocketAddress[b.numJournalNodes];
    httpAddrs = new InetSocketAddress[b.numJournalNodes];
    for (int i = 0; i < b.numJournalNodes; i++) {
      if (b.format) {
        File dir = getStorageDir(i);
        LOG.debug("Fully deleting JN directory " + dir);
        FileUtil.fullyDelete(dir);
      }
      nodes[i] = new JournalNode();
      nodes[i].setConf(createConfForNode(b, i));
      nodes[i].start();

      ipcAddrs[i] = nodes[i].getBoundIpcAddress();
      httpAddrs[i] = nodes[i].getBoundHttpAddress();
    }
  }

  /**
   * Set up the given Configuration object to point to the set of JournalNodes 
   * in this cluster.
   */
  public URI getQuorumJournalURI(String jid) {
    List<String> addrs = Lists.newArrayList();
    for (InetSocketAddress addr : ipcAddrs) {
      addrs.add("127.0.0.1:" + addr.getPort());
    }
    String addrsVal = Joiner.on(";").join(addrs);
    LOG.debug("Setting logger addresses to: " + addrsVal);
    try {
      return new URI("qjournal://" + addrsVal + "/" + jid);
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Start the JournalNodes in the cluster.
   */
  public void start() throws IOException {
    for (JournalNode jn : nodes) {
      jn.start();
    }
  }

  /**
   * Shutdown all of the JournalNodes in the cluster.
   * @throws IOException if one or more nodes failed to stop
   */
  public void shutdown() throws IOException {
    boolean failed = false;
    for (JournalNode jn : nodes) {
      try {
        jn.stopAndJoin(0);
      } catch (Exception e) {
        failed = true;
        LOG.warn("Unable to stop journal node " + jn, e);
      }
    }
    if (failed) {
      throw new IOException("Unable to shut down. Check log for details");
    }
  }

  private Configuration createConfForNode(Builder b, int idx) {
    Configuration conf = new Configuration(b.conf);
    File logDir = getStorageDir(idx);
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY, logDir.toString());
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    return conf;
  }

  public File getStorageDir(int idx) {
    return new File(baseDir, "journalnode-" + idx).getAbsoluteFile();
  }
  
  public File getCurrentDir(int idx, String jid) {
    return new File(new File(getStorageDir(idx), jid), "current");
  }

  public JournalNode getJournalNode(int i) {
    return nodes[i];
  }
  
  public void restartJournalNode(int i) throws InterruptedException, IOException {
    Configuration conf = new Configuration(nodes[i].getConf());
    if (nodes[i].isStarted()) {
      nodes[i].stopAndJoin(0);
    }
    
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY, "127.0.0.1:" +
        ipcAddrs[i].getPort());
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY, "127.0.0.1:" +
        httpAddrs[i].getPort());
    
    JournalNode jn = new JournalNode();
    jn.setConf(conf);
    jn.start();
  }

  public int getQuorumSize() {
    return nodes.length / 2 + 1;
  }

  public int getNumNodes() {
    return nodes.length;
  }

}
