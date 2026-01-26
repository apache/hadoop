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

import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.FAKE_NSINFO;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import java.util.function.Supplier;

import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.test.GenericTestUtils;

public final class MiniJournalCluster implements Closeable {

  public static final String CLUSTER_WAITACTIVE_URI = "waitactive";
  public static class Builder {
    private String baseDir;
    private int numJournalNodes = 3;
    private boolean format = true;
    private final Configuration conf;
    private int[] httpPorts = null;
    private int[] rpcPorts = null;

    static {
      DefaultMetricsSystem.setMiniClusterMode(true);
    }
    
    public Builder(Configuration conf) {
      this.conf = conf;
    }

    public Builder(Configuration conf, File baseDir) {
      this.conf = conf;
      baseDir(baseDir.toString());
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

    public Builder setHttpPorts(int... ports) {
      this.httpPorts = ports;
      return this;
    }

    public Builder setRpcPorts(int... ports) {
      this.rpcPorts = ports;
      return this;
    }

    public MiniJournalCluster build() throws IOException {
      return new MiniJournalCluster(this);
    }
  }

  private static final class JNInfo {
    private JournalNode node;
    private final InetSocketAddress ipcAddr;
    private final String httpServerURI;

    private JNInfo(JournalNode node) {
      this.node = node;
      this.ipcAddr = node.getBoundIpcAddress();
      this.httpServerURI = node.getHttpServerURI();
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniJournalCluster.class);
  private final File baseDir;
  private final JNInfo[] nodes;
  
  private MiniJournalCluster(Builder b) throws IOException {

    if (b.httpPorts != null && b.httpPorts.length != b.numJournalNodes) {
      throw new IllegalArgumentException(
          "Num of http ports (" + b.httpPorts.length + ") should match num of JournalNodes ("
              + b.numJournalNodes + ")");
    }

    if (b.rpcPorts != null && b.rpcPorts.length != b.numJournalNodes) {
      throw new IllegalArgumentException(
          "Num of rpc ports (" + b.rpcPorts.length + ") should match num of JournalNodes ("
              + b.numJournalNodes + ")");
    }

    LOG.info("Starting MiniJournalCluster with " +
        b.numJournalNodes + " journal nodes");
    
    if (b.baseDir != null) {
      this.baseDir = new File(b.baseDir);
    } else {
      this.baseDir = new File(MiniDFSCluster.getBaseDirectory());
    }

    nodes = new JNInfo[b.numJournalNodes];

    for (int i = 0; i < b.numJournalNodes; i++) {
      if (b.format) {
        File dir = getStorageDir(i);
        LOG.debug("Fully deleting JN directory " + dir);
        FileUtil.fullyDelete(dir);
      }
      JournalNode jn = new JournalNode();
      jn.setConf(createConfForNode(b, i));
      jn.start();
      nodes[i] = new JNInfo(jn);
    }
  }

  /**
   * Set up the given Configuration object to point to the set of JournalNodes 
   * in this cluster.
   */
  public URI getQuorumJournalURI(String jid) {
    List<String> addrs = Lists.newArrayList();
    for (JNInfo info : nodes) {
      addrs.add("127.0.0.1:" + info.ipcAddr.getPort());
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
    for (JNInfo info : nodes) {
      info.node.start();
    }
  }

  /**
   * Shutdown all of the JournalNodes in the cluster.
   * @throws IOException if one or more nodes failed to stop
   */
  public void shutdown() throws IOException {
    boolean failed = false;
    for (JNInfo info : nodes) {
      try {
        info.node.stopAndJoin(0);
      } catch (Exception e) {
        failed = true;
        LOG.warn("Unable to stop journal node " + info.node, e);
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
    int httpPort = b.httpPorts != null ? b.httpPorts[idx] : 0;
    int rpcPort = b.rpcPorts != null ? b.rpcPorts[idx] : 0;
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY, "localhost:" + rpcPort);
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY, "localhost:" + httpPort);
    return conf;
  }

  public File getStorageDir(int idx) {
    return new File(baseDir, "journalnode-" + idx).getAbsoluteFile();
  }
  
  public File getJournalDir(int idx, String jid) {
    return new File(getStorageDir(idx), jid);
  }
  
  public File getCurrentDir(int idx, String jid) {
    return new File(getJournalDir(idx, jid), "current");
  }
  
  public File getPreviousDir(int idx, String jid) {
    return new File(getJournalDir(idx, jid), "previous");
  }

  public JournalNode getJournalNode(int i) {
    return nodes[i].node;
  }

  public String getJournalNodeIpcAddress(int i) {
    return nodes[i].ipcAddr.toString();
  }

  public void restartJournalNode(int i) throws InterruptedException, IOException {
    JNInfo info = nodes[i];
    JournalNode jn = info.node;
    Configuration conf = new Configuration(jn.getConf());
    if (jn.isStarted()) {
      jn.stopAndJoin(0);
    }
    
    conf.set(DFSConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY,
        NetUtils.getHostPortString(info.ipcAddr));

    final String uri = info.httpServerURI;
    if (uri.startsWith("http://")) {
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY,
          uri.substring(("http://".length())));
    } else if (info.httpServerURI.startsWith("https://")) {
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_KEY,
          uri.substring(("https://".length())));
    }

    JournalNode newJN = new JournalNode();
    newJN.setConf(conf);
    newJN.start();
    info.node = newJN;
  }

  public int getQuorumSize() {
    return nodes.length / 2 + 1;
  }

  public int getNumNodes() {
    return nodes.length;
  }

  /**
   * Wait until all the journalnodes start.
   */
  public void waitActive() throws IOException {
    for (int i = 0; i < nodes.length; i++) {
      final int index = i;
      try {
        GenericTestUtils.waitFor(new Supplier<Boolean>() {
          // wait until all JN's IPC server is running
          @Override public Boolean get() {
            try {
              QuorumJournalManager qjm =
                  new QuorumJournalManager(nodes[index].node.getConf(),
                      getQuorumJournalURI(CLUSTER_WAITACTIVE_URI), FAKE_NSINFO);
              qjm.hasSomeData();
              qjm.close();
            } catch (IOException e) {
              // Exception from IPC call, likely due to server not ready yet.
              return false;
            }
            return true;
          }
        }, 50, 3000);
      } catch (TimeoutException e) {
        throw new AssertionError("Time out while waiting for journal node " + index +
            " to start.");
      } catch (InterruptedException ite) {
        LOG.warn("Thread interrupted when waiting for node start", ite);
      }
    }
  }

  public void setNamenodeSharedEditsConf(String jid) {
    URI quorumJournalURI = getQuorumJournalURI(jid);
    for (int i = 0; i < nodes.length; i++) {
      nodes[i].node.getConf().set(DFSConfigKeys
          .DFS_NAMENODE_SHARED_EDITS_DIR_KEY, quorumJournalURI.toString());
    }
  }

  @Override
  public void close() throws IOException {
    this.shutdown();
  }

}
