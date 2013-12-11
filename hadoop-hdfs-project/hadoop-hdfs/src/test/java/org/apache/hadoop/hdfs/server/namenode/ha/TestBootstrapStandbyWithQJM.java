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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Test BootstrapStandby when QJM is used for shared edits. 
 */
public class TestBootstrapStandbyWithQJM {  
  private MiniQJMHACluster miniQjmHaCluster;
  private MiniDFSCluster cluster;
  private MiniJournalCluster jCluster;
  
  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    // Turn off IPC client caching, so that the suite can handle
    // the restart of the daemons between test cases.
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);

    miniQjmHaCluster = new MiniQJMHACluster.Builder(conf).build();
    cluster = miniQjmHaCluster.getDfsCluster();
    jCluster = miniQjmHaCluster.getJournalCluster();
    
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
