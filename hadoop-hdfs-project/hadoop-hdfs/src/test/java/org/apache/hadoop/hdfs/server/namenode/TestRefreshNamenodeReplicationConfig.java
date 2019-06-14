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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the replication related parameters in the namenode can
 * be refreshed dynamically, without a namenode restart.
 */
public class TestRefreshNamenodeReplicationConfig {
  private MiniDFSCluster cluster = null;
  private BlockManager bm;

  @Before
  public void setup() throws IOException {
    Configuration config = new Configuration();
    config.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 8);
    config.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY, 10);
    config.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION,
        12);

    cluster = new MiniDFSCluster.Builder(config)
        .nnTopology(MiniDFSNNTopology.simpleSingleNN(0, 0))
        .numDataNodes(0).build();
    cluster.waitActive();
    bm = cluster.getNameNode().getNamesystem().getBlockManager();
  }

  @After
  public void teardown() throws IOException {
    cluster.shutdown();
  }

  /**
   * Tests to ensure each of the block replication parameters can be passed
   * updated successfully.
   */
  @Test(timeout = 90000)
  public void testParamsCanBeReconfigured() throws ReconfigurationException {

    assertEquals(8, bm.getMaxReplicationStreams());
    assertEquals(10, bm.getReplicationStreamsHardLimit());
    assertEquals(12, bm.getBlocksReplWorkMultiplier());

    cluster.getNameNode().reconfigurePropertyImpl(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, "20");
    cluster.getNameNode().reconfigurePropertyImpl(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY,
        "22");
    cluster.getNameNode().reconfigurePropertyImpl(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION,
        "24");

    assertEquals(20, bm.getMaxReplicationStreams());
    assertEquals(22, bm.getReplicationStreamsHardLimit());
    assertEquals(24, bm.getBlocksReplWorkMultiplier());
  }

  /**
   * Tests to ensure reconfiguration fails with a negative, zero or string value
   * value for each parameter.
   */
  @Test(timeout = 90000)
  public void testReconfigureFailsWithInvalidValues() throws Exception {
    String[] keys = new String[]{
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION
    };

    // Ensure we cannot set any of the parameters negative
    for (String key : keys) {
      ReconfigurationException e =
          LambdaTestUtils.intercept(ReconfigurationException.class,
              () -> cluster.getNameNode().reconfigurePropertyImpl(key, "-20"));
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertEquals(key+" = '-20' is invalid. It should be a "
          +"positive, non-zero integer value.", e.getCause().getMessage());
    }
    // Ensure none of the values were updated from the defaults
    assertEquals(8, bm.getMaxReplicationStreams());
    assertEquals(10, bm.getReplicationStreamsHardLimit());
    assertEquals(12, bm.getBlocksReplWorkMultiplier());

    for (String key : keys) {
      ReconfigurationException e =
          LambdaTestUtils.intercept(ReconfigurationException.class,
              () -> cluster.getNameNode().reconfigurePropertyImpl(key, "0"));
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertEquals(key+" = '0' is invalid. It should be a "
          +"positive, non-zero integer value.", e.getCause().getMessage());
    }

    // Ensure none of the values were updated from the defaults
    assertEquals(8, bm.getMaxReplicationStreams());
    assertEquals(10, bm.getReplicationStreamsHardLimit());
    assertEquals(12, bm.getBlocksReplWorkMultiplier());

    // Ensure none of the parameters can be set to a string value
    for (String key : keys) {
      ReconfigurationException e =
          LambdaTestUtils.intercept(ReconfigurationException.class,
              () -> cluster.getNameNode().reconfigurePropertyImpl(key, "str"));
      assertTrue(e.getCause() instanceof NumberFormatException);
    }

    // Ensure none of the values were updated from the defaults
    assertEquals(8, bm.getMaxReplicationStreams());
    assertEquals(10, bm.getReplicationStreamsHardLimit());
    assertEquals(12, bm.getBlocksReplWorkMultiplier());
  }
}