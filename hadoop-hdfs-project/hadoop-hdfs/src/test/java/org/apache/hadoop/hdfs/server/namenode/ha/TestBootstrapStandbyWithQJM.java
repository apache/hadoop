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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Test BootstrapStandby when QJM is used for shared edits. 
 */
public class TestBootstrapStandbyWithQJM {
  
  private static final String NAMESERVICE = "ns1";
  private static final String NN1 = "nn1";
  private static final String NN2 = "nn2";
  private static final int NUM_JN = 3;
  private static final int NN1_IPC_PORT = 10000;
  private static final int NN1_INFO_PORT = 10001;
  private static final int NN2_IPC_PORT = 10002;
  private static final int NN2_INFO_PORT = 10003;
  
  private MiniDFSCluster cluster;
  private MiniJournalCluster jCluster;
  
  @Before
  public void setup() throws Exception {
    // start 3 journal nodes
    jCluster = new MiniJournalCluster.Builder(new Configuration()).format(true)
        .numJournalNodes(NUM_JN).build();
    URI journalURI = jCluster.getQuorumJournalURI(NAMESERVICE);
    
    // start cluster with 2 NameNodes
    MiniDFSNNTopology topology = new MiniDFSNNTopology()
        .addNameservice(new MiniDFSNNTopology.NSConf(NAMESERVICE).addNN(
            new MiniDFSNNTopology.NNConf("nn1").setIpcPort(NN1_IPC_PORT)
                .setHttpPort(NN1_INFO_PORT)).addNN(
            new MiniDFSNNTopology.NNConf("nn2").setIpcPort(NN2_IPC_PORT)
                .setHttpPort(NN2_INFO_PORT)));
    
    Configuration conf = initHAConf(journalURI);
    cluster = new MiniDFSCluster.Builder(conf).nnTopology(topology)
        .numDataNodes(1).manageNameDfsSharedDirs(false).build();
    cluster.waitActive();
    
    Configuration confNN0 = new Configuration(conf);
    cluster.shutdown();
    // initialize the journal nodes
    confNN0.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");
    NameNode.initializeSharedEdits(confNN0, true);
    
    // restart the cluster
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .nnTopology(topology).numDataNodes(1).manageNameDfsSharedDirs(false)
        .build();
    cluster.waitActive();
    
    // make nn0 active
    cluster.transitionToActive(0);
    // do sth to generate in-progress edit log data
    DistributedFileSystem dfs = (DistributedFileSystem) 
        HATestUtil.configureFailoverFs(cluster, conf);
    dfs.mkdirs(new Path("/test2"));
    dfs.close();
  }
  
  @After
  public void cleanup() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    if (jCluster != null) {
      jCluster.shutdown();
    }
  }
  
  private Configuration initHAConf(URI journalURI) {
    Configuration conf = new Configuration();
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

  /** BootstrapStandby when the existing NN is standby */
  @Test
  public void testBootstrapStandbyWithStandbyNN() throws Exception {
    // make the first NN in standby state
    cluster.transitionToStandby(0);
    Configuration confNN1 = cluster.getConfiguration(1);
    
    // shut down nn1
    cluster.shutdownNameNode(1);
    
    int rc = BootstrapStandby.run(new String[] { "-force" }, confNN1);
    assertEquals(0, rc);
    
    // Should have copied over the namespace from the standby
    FSImageTestUtil.assertNNHasCheckpoints(cluster, 1,
        ImmutableList.of(0));
    FSImageTestUtil.assertNNFilesMatch(cluster);
  }
  
  /** BootstrapStandby when the existing NN is active */
  @Test
  public void testBootstrapStandbyWithActiveNN() throws Exception {
    // make the first NN in active state
    cluster.transitionToActive(0);
    Configuration confNN1 = cluster.getConfiguration(1);
    
    // shut down nn1
    cluster.shutdownNameNode(1);
    
    int rc = BootstrapStandby.run(new String[] { "-force" }, confNN1);
    assertEquals(0, rc);
    
    // Should have copied over the namespace from the standby
    FSImageTestUtil.assertNNHasCheckpoints(cluster, 1,
        ImmutableList.of(0));
    FSImageTestUtil.assertNNFilesMatch(cluster);
  }
}
