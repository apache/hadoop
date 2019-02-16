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

package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to reconfigure some parameters for DataNode without restart
 */
public class TestDataNodeReconfiguration {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestBlockRecovery.class);
  private static final String DATA_DIR = MiniDFSCluster.getBaseDirectory()
      + "data";
  private final static InetSocketAddress NN_ADDR = new InetSocketAddress(
      "localhost", 5020);
  private final int NUM_NAME_NODE = 1;
  private final int NUM_DATA_NODE = 10;
  private MiniDFSCluster cluster;

  @Before
  public void Setup() throws IOException {
    startDFSCluster(NUM_NAME_NODE, NUM_DATA_NODE);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }

    File dir = new File(DATA_DIR);
    if (dir.exists())
      Assert.assertTrue("Cannot delete data-node dirs",
          FileUtil.fullyDelete(dir));
  }

  private void startDFSCluster(int numNameNodes, int numDataNodes)
      throws IOException {
    Configuration conf = new Configuration();

    MiniDFSNNTopology nnTopology = MiniDFSNNTopology
        .simpleFederatedTopology(numNameNodes);

    cluster = new MiniDFSCluster.Builder(conf).nnTopology(nnTopology)
        .numDataNodes(numDataNodes).build();
    cluster.waitActive();
  }

  /**
   * Starts an instance of DataNode
   *
   * @throws IOException
   */
  public DataNode[] createDNsForTest(int numDateNode) throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, DATA_DIR);
    conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "0.0.0.0:0");
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);

    DataNode[] result = new DataNode[numDateNode];
    for (int i = 0; i < numDateNode; i++) {
      result[i] = InternalDataNodeTestUtils.startDNWithMockNN(conf, NN_ADDR, DATA_DIR);
    }
    return result;
  }

  @Test
  public void testMaxConcurrentMoversReconfiguration()
      throws ReconfigurationException, IOException {
    int maxConcurrentMovers = 10;
    for (int i = 0; i < NUM_DATA_NODE; i++) {
      DataNode dn = cluster.getDataNodes().get(i);

      // try invalid values
      try {
        dn.reconfigureProperty(
            DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY, "text");
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting NumberFormatException",
            expected.getCause() instanceof NumberFormatException);
      }
      try {
        dn.reconfigureProperty(
            DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
            String.valueOf(-1));
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting IllegalArgumentException",
            expected.getCause() instanceof IllegalArgumentException);
      }
      try {
        dn.reconfigureProperty(
            DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
            String.valueOf(0));
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting IllegalArgumentException",
            expected.getCause() instanceof IllegalArgumentException);
      }

      // change properties
      dn.reconfigureProperty(DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
          String.valueOf(maxConcurrentMovers));

      // verify change
      assertEquals(String.format("%s has wrong value",
          DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY),
          maxConcurrentMovers, dn.xserver.balanceThrottler.getMaxConcurrentMovers());

      assertEquals(String.format("%s has wrong value",
          DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY),
          maxConcurrentMovers, Integer.parseInt(dn.getConf().get(
              DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY)));

      // revert to default
      dn.reconfigureProperty(DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
          null);

      // verify default
      assertEquals(String.format("%s has wrong value",
          DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY),
          DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT,
          dn.xserver.balanceThrottler.getMaxConcurrentMovers());

      assertEquals(String.format("expect %s is not configured",
          DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY), null, dn
          .getConf().get(DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY));
    }
  }

  @Test
  public void testAcquireWithMaxConcurrentMoversGreaterThanDefault()
      throws IOException, ReconfigurationException {
    final DataNode[] dns = createDNsForTest(1);
    try {
      testAcquireOnMaxConcurrentMoversReconfiguration(dns[0], 10);
    } finally {
      dns[0].shutdown();
    }
  }

  @Test
  public void testAcquireWithMaxConcurrentMoversLessThanDefault()
      throws IOException, ReconfigurationException {
    final DataNode[] dns = createDNsForTest(1);
    try {
      testAcquireOnMaxConcurrentMoversReconfiguration(dns[0], 3);
    } finally {
      dns[0].shutdown();
    }
  }

  /**
   * Simulates a scenario where the DataNode has been reconfigured with fewer
   * mover threads, but all of the current treads are busy and therefore the
   * DataNode is unable to honor this request within a reasonable amount of
   * time. The DataNode eventually gives up and returns a flag indicating that
   * the request was not honored.
   */
  @Test
  public void testFailedDecreaseConcurrentMovers()
      throws IOException, ReconfigurationException {
    final DataNode[] dns = createDNsForTest(1);
    final DataNode dataNode = dns[0];
    try {
      // Set the current max to 2
      dataNode.xserver.updateBalancerMaxConcurrentMovers(2);

      // Simulate grabbing 2 threads
      dataNode.xserver.balanceThrottler.acquire();
      dataNode.xserver.balanceThrottler.acquire();

      dataNode.xserver.setMaxReconfigureWaitTime(1);

      // Attempt to set new maximum to 1
      final boolean success =
          dataNode.xserver.updateBalancerMaxConcurrentMovers(1);
      Assert.assertFalse(success);
    } finally {
      dataNode.shutdown();
    }
  }

  /**
   * Test with invalid configuration.
   */
  @Test(expected = ReconfigurationException.class)
  public void testFailedDecreaseConcurrentMoversReconfiguration()
      throws IOException, ReconfigurationException {
    final DataNode[] dns = createDNsForTest(1);
    final DataNode dataNode = dns[0];
    try {
      // Set the current max to 2
      dataNode.xserver.updateBalancerMaxConcurrentMovers(2);

      // Simulate grabbing 2 threads
      dataNode.xserver.balanceThrottler.acquire();
      dataNode.xserver.balanceThrottler.acquire();

      dataNode.xserver.setMaxReconfigureWaitTime(1);

      // Now try reconfigure maximum downwards with threads released
      dataNode.reconfigurePropertyImpl(
          DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY, "1");
    } catch (ReconfigurationException e) {
      Assert.assertEquals(DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
          e.getProperty());
      Assert.assertEquals("1", e.getNewValue());
      throw e;
    } finally {
      dataNode.shutdown();
    }
  }

  private void testAcquireOnMaxConcurrentMoversReconfiguration(
      DataNode dataNode, int maxConcurrentMovers) throws IOException,
      ReconfigurationException {
    final int defaultMaxThreads = dataNode.getConf().getInt(
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);

    /** Test that the default setup is working */

    for (int i = 0; i < defaultMaxThreads; i++) {
      assertEquals("should be able to get thread quota", true,
          dataNode.xserver.balanceThrottler.acquire());
    }

    assertEquals("should not be able to get thread quota", false,
        dataNode.xserver.balanceThrottler.acquire());

    // Give back the threads
    for (int i = 0; i < defaultMaxThreads; i++) {
      dataNode.xserver.balanceThrottler.release();
    }

    /** Test that the change is applied correctly */

    // change properties
    dataNode.reconfigureProperty(
        DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        String.valueOf(maxConcurrentMovers));

    assertEquals("thread quota is wrong", maxConcurrentMovers,
        dataNode.xserver.balanceThrottler.getMaxConcurrentMovers());

    for (int i = 0; i < maxConcurrentMovers; i++) {
      assertEquals("should be able to get thread quota", true,
          dataNode.xserver.balanceThrottler.acquire());
    }

    assertEquals("should not be able to get thread quota", false,
        dataNode.xserver.balanceThrottler.acquire());
  }
}
