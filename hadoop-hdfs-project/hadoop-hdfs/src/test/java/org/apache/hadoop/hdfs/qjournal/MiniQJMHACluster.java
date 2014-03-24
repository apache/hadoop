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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;

public class MiniQJMHACluster {
  private MiniDFSCluster cluster;
  private MiniJournalCluster journalCluster;
  private final Configuration conf;
  
  public static final String NAMESERVICE = "ns1";
  private static final String NN1 = "nn1";
  private static final String NN2 = "nn2";
  private static final int NN1_IPC_PORT = 10000;
  private static final int NN1_INFO_PORT = 10001;
  private static final int NN2_IPC_PORT = 10002;
  private static final int NN2_INFO_PORT = 10003;

  public static class Builder {
    private final Configuration conf;
    private StartupOption startOpt = null;
    private final MiniDFSCluster.Builder dfsBuilder;
    
    public Builder(Configuration conf) {
      this.conf = conf;
      this.dfsBuilder = new MiniDFSCluster.Builder(conf);
    }

    public MiniDFSCluster.Builder getDfsBuilder() {
      return dfsBuilder;
    }
    
    public MiniQJMHACluster build() throws IOException {
      return new MiniQJMHACluster(this);
    }

    public void startupOption(StartupOption startOpt) {
      this.startOpt = startOpt;
    }
  }
  
  public static MiniDFSNNTopology createDefaultTopology() {
    return new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf(NAMESERVICE).addNN(
        new MiniDFSNNTopology.NNConf("nn1").setIpcPort(NN1_IPC_PORT)
            .setHttpPort(NN1_INFO_PORT)).addNN(
        new MiniDFSNNTopology.NNConf("nn2").setIpcPort(NN2_IPC_PORT)
            .setHttpPort(NN2_INFO_PORT)));
  }
  
  private MiniQJMHACluster(Builder builder) throws IOException {
    this.conf = builder.conf;
    // start 3 journal nodes
    journalCluster = new MiniJournalCluster.Builder(conf).format(true)
        .build();
    URI journalURI = journalCluster.getQuorumJournalURI(NAMESERVICE);
    
    // start cluster with 2 NameNodes
    MiniDFSNNTopology topology = createDefaultTopology();
    
    initHAConf(journalURI, builder.conf);
    
    // First start up the NNs just to format the namespace. The MinIDFSCluster
    // has no way to just format the NameNodes without also starting them.
    cluster = builder.dfsBuilder.nnTopology(topology)
        .manageNameDfsSharedDirs(false).build();
    cluster.waitActive();
    cluster.shutdown();
    
    // initialize the journal nodes
    Configuration confNN0 = cluster.getConfiguration(0);
    NameNode.initializeSharedEdits(confNN0, true);
    
    cluster.getNameNodeInfos()[0].setStartOpt(builder.startOpt);
    cluster.getNameNodeInfos()[1].setStartOpt(builder.startOpt);
    
    // restart the cluster
    cluster.restartNameNodes();
  }
  
  private Configuration initHAConf(URI journalURI, Configuration conf) {
    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
        journalURI.toString());
    
    String address1 = "127.0.0.1:" + NN1_IPC_PORT;
    String address2 = "127.0.0.1:" + NN2_IPC_PORT;
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
        NAMESERVICE, NN1), address1);
    conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
        NAMESERVICE, NN2), address2);
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, NAMESERVICE);
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, NAMESERVICE),
        NN1 + "," + NN2);
    conf.set(DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + NAMESERVICE,
        ConfiguredFailoverProxyProvider.class.getName());
    conf.set("fs.defaultFS", "hdfs://" + NAMESERVICE);
    
    return conf;
  }

  public MiniDFSCluster getDfsCluster() {
    return cluster;
  }
  
  public MiniJournalCluster getJournalCluster() {
    return journalCluster;
  }

  public void shutdown() throws IOException {
    cluster.shutdown();
    journalCluster.shutdown();
  }
}
