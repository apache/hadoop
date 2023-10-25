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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DU_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DU_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_GETSPACEUSED_JITTER_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_GETSPACEUSED_JITTER_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_GETSPACEUSED_CLASSNAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DISK_BALANCER_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DISK_BALANCER_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.BlockPoolSlice;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to reconfigure some parameters for DataNode without restart
 */
public class TestDataNodeReconfiguration {

  private static final String DATA_DIR = MiniDFSCluster.getBaseDirectory()
      + "data";
  private final static InetSocketAddress NN_ADDR = new InetSocketAddress(
      "localhost", 5020);
  private final int NUM_NAME_NODE = 1;
  private final int NUM_DATA_NODE = 10;
  private MiniDFSCluster cluster;
  private static long counter = 0;

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
    conf.setBoolean(DFS_DATANODE_PEER_STATS_ENABLED_KEY, true);

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

  @Test
  public void testBlockReportIntervalReconfiguration()
      throws ReconfigurationException {
    int blockReportInterval = 300 * 1000;
    String[] blockReportParameters = {
        DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY,
        DFS_BLOCKREPORT_INITIAL_DELAY_KEY};

    for (int i = 0; i < NUM_DATA_NODE; i++) {
      DataNode dn = cluster.getDataNodes().get(i);
      BlockPoolManager blockPoolManager = dn.getBlockPoolManager();

      // Try invalid values.
      for (String blockReportParameter : blockReportParameters) {
        try {
          dn.reconfigureProperty(blockReportParameter, "text");
          fail("ReconfigurationException expected");
        } catch (ReconfigurationException expected) {
          assertTrue("expecting NumberFormatException",
              expected.getCause() instanceof NumberFormatException);
        }
      }

      try {
        dn.reconfigureProperty(DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, String.valueOf(-1));
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting IllegalArgumentException",
            expected.getCause() instanceof IllegalArgumentException);
      }
      try {
        dn.reconfigureProperty(DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY, String.valueOf(-1));
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting IllegalArgumentException",
            expected.getCause() instanceof IllegalArgumentException);
      }
      dn.reconfigureProperty(DFS_BLOCKREPORT_INITIAL_DELAY_KEY, String.valueOf(-1));
      assertEquals(0, dn.getDnConf().initialBlockReportDelayMs);

      // Change properties and verify the change.
      dn.reconfigureProperty(DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
          String.valueOf(blockReportInterval));
      for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
        if (bpos != null) {
          for (BPServiceActor actor : bpos.getBPServiceActors()) {
            assertEquals(String.format("%s has wrong value",
                DFS_BLOCKREPORT_INTERVAL_MSEC_KEY),
                blockReportInterval,
                actor.getScheduler().getBlockReportIntervalMs());
          }
        }
      }

      dn.reconfigureProperty(DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY, String.valueOf(123));
      assertEquals(123, dn.getDnConf().blockReportSplitThreshold);

      dn.reconfigureProperty(DFS_BLOCKREPORT_INITIAL_DELAY_KEY, "123");
      assertEquals(123000, dn.getDnConf().initialBlockReportDelayMs);

      // Revert to default and verify default.
      dn.reconfigureProperty(DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, null);
      for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
        if (bpos != null) {
          for (BPServiceActor actor : bpos.getBPServiceActors()) {
            assertEquals(String.format("%s has wrong value",
                DFS_BLOCKREPORT_INTERVAL_MSEC_KEY),
                DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT,
                actor.getScheduler().getBlockReportIntervalMs());
          }
        }
      }
      assertNull(String.format("expect %s is not configured", DFS_BLOCKREPORT_INTERVAL_MSEC_KEY),
          dn.getConf().get(DFS_BLOCKREPORT_INTERVAL_MSEC_KEY));

      dn.reconfigureProperty(DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY, null);
      assertNull(String.format("expect %s is not configured", DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY),
          dn.getConf().get(DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY));

      dn.reconfigureProperty(DFS_BLOCKREPORT_INITIAL_DELAY_KEY, null);
      assertNull(String.format("expect %s is not configured", DFS_BLOCKREPORT_INITIAL_DELAY_KEY),
          dn.getConf().get(DFS_BLOCKREPORT_INITIAL_DELAY_KEY));
    }
  }

  @Test
  public void testDataXceiverReconfiguration()
      throws ReconfigurationException {
    for (int i = 0; i < NUM_DATA_NODE; i++) {
      DataNode dn = cluster.getDataNodes().get(i);

      // Try invalid values.
      try {
        dn.reconfigureProperty(DFS_DATANODE_MAX_RECEIVER_THREADS_KEY, "text");
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting NumberFormatException",
            expected.getCause() instanceof NumberFormatException);
      }
      try {
        dn.reconfigureProperty(DFS_DATANODE_MAX_RECEIVER_THREADS_KEY, String.valueOf(-1));
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting IllegalArgumentException",
            expected.getCause() instanceof IllegalArgumentException);
      }
      try {
        dn.reconfigureProperty(DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY, "text");
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting NumberFormatException",
            expected.getCause() instanceof NumberFormatException);
      }
      try {
        dn.reconfigureProperty(DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY, "text");
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting NumberFormatException",
            expected.getCause() instanceof NumberFormatException);
      }
      try {
        dn.reconfigureProperty(DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY, "text");
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting NumberFormatException",
            expected.getCause() instanceof NumberFormatException);
      }

      // Change properties and verify change.
      dn.reconfigureProperty(DFS_DATANODE_MAX_RECEIVER_THREADS_KEY, String.valueOf(123));
      assertEquals(String.format("%s has wrong value", DFS_DATANODE_MAX_RECEIVER_THREADS_KEY),
          123, dn.getXferServer().getMaxXceiverCount());

      dn.reconfigureProperty(DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY,
          String.valueOf(1000));
      assertEquals(String.format("%s has wrong value",
              DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY),
          1000, dn.getXferServer().getTransferThrottler().getBandwidth());

      dn.reconfigureProperty(DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY,
          String.valueOf(1000));
      assertEquals(String.format("%s has wrong value",
              DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY),
          1000, dn.getXferServer().getWriteThrottler().getBandwidth());

      dn.reconfigureProperty(DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY,
          String.valueOf(1000));
      assertEquals(String.format("%s has wrong value",
              DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY),
          1000, dn.getXferServer().getReadThrottler().getBandwidth());

      // Revert to default.
      dn.reconfigureProperty(DFS_DATANODE_MAX_RECEIVER_THREADS_KEY, null);
      assertEquals(String.format("%s has wrong value", DFS_DATANODE_MAX_RECEIVER_THREADS_KEY),
          DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT, dn.getXferServer().getMaxXceiverCount());
      assertNull(String.format("expect %s is not configured",
          DFS_DATANODE_MAX_RECEIVER_THREADS_KEY),
          dn.getConf().get(DFS_DATANODE_MAX_RECEIVER_THREADS_KEY));

      dn.reconfigureProperty(DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY, null);
      assertEquals(String.format("%s has wrong value",
              DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY),
          null, dn.getXferServer().getTransferThrottler());
      assertNull(String.format("expect %s is not configured",
              DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY),
          dn.getConf().get(DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY));

      dn.reconfigureProperty(DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY, null);
      assertEquals(String.format("%s has wrong value",
              DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY),
          null, dn.getXferServer().getWriteThrottler());
      assertNull(String.format("expect %s is not configured",
              DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY),
          dn.getConf().get(DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY));

      dn.reconfigureProperty(DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY, null);
      assertEquals(String.format("%s has wrong value",
              DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY),
          null, dn.getXferServer().getReadThrottler());
      assertNull(String.format("expect %s is not configured",
              DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY),
          dn.getConf().get(DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY));
    }
  }

  @Test
  public void testCacheReportReconfiguration()
      throws ReconfigurationException {
    int cacheReportInterval = 300 * 1000;
    for (int i = 0; i < NUM_DATA_NODE; i++) {
      DataNode dn = cluster.getDataNodes().get(i);

      // Try invalid values.
      try {
        dn.reconfigureProperty(DFS_CACHEREPORT_INTERVAL_MSEC_KEY, "text");
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting NumberFormatException",
            expected.getCause() instanceof NumberFormatException);
      }
      try {
        dn.reconfigureProperty(DFS_CACHEREPORT_INTERVAL_MSEC_KEY, String.valueOf(-1));
        fail("ReconfigurationException expected");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting IllegalArgumentException",
            expected.getCause() instanceof IllegalArgumentException);
      }

      // Change properties.
      dn.reconfigureProperty(DFS_CACHEREPORT_INTERVAL_MSEC_KEY,
          String.valueOf(cacheReportInterval));

      // Verify change.
      assertEquals(String.format("%s has wrong value", DFS_CACHEREPORT_INTERVAL_MSEC_KEY),
          cacheReportInterval, dn.getDnConf().getCacheReportInterval());

      // Revert to default.
      dn.reconfigureProperty(DFS_CACHEREPORT_INTERVAL_MSEC_KEY, null);
      assertEquals(String.format("%s has wrong value", DFS_CACHEREPORT_INTERVAL_MSEC_KEY),
          DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT, dn.getDnConf().getCacheReportInterval());

      assertNull(String.format("expect %s is not configured", DFS_CACHEREPORT_INTERVAL_MSEC_KEY),
          dn.getConf().get(DFS_CACHEREPORT_INTERVAL_MSEC_KEY));
    }
  }

  @Test
  public void testSlowPeerParameters() throws Exception {
    String[] slowPeersParameters = {
        DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY,
        DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY,
        DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY};

    for (int i = 0; i < NUM_DATA_NODE; i++) {
      DataNode dn = cluster.getDataNodes().get(i);

      // Try invalid values.
      LambdaTestUtils.intercept(ReconfigurationException.class,
          "Could not change property dfs.datanode.peer.stats.enabled from 'true' to 'text'",
          () -> dn.reconfigureProperty(DFS_DATANODE_PEER_STATS_ENABLED_KEY, "text"));

      for (String parameter : slowPeersParameters) {
        try {
          dn.reconfigureProperty(parameter, "text");
          fail("ReconfigurationException expected");
        } catch (ReconfigurationException expected) {
          assertTrue("expecting NumberFormatException",
              expected.getCause() instanceof NumberFormatException);
        }

        try {
          dn.reconfigureProperty(parameter, String.valueOf(-1));
          fail("ReconfigurationException expected");
        } catch (ReconfigurationException expected) {
          assertTrue("expecting IllegalArgumentException",
              expected.getCause() instanceof IllegalArgumentException);
        }
      }

      // Change and verify properties.
      dn.reconfigureProperty(DFS_DATANODE_PEER_STATS_ENABLED_KEY, "false");
      assertFalse(dn.getDnConf().peerStatsEnabled);

      // Reset DFS_DATANODE_PEER_STATS_ENABLED_KEY to true.
      dn.reconfigureProperty(DFS_DATANODE_PEER_STATS_ENABLED_KEY, "true");
      for (String parameter : slowPeersParameters) {
        dn.reconfigureProperty(parameter, "123");
      }
      assertEquals(123, dn.getPeerMetrics().getMinOutlierDetectionNodes());
      assertEquals(123, dn.getPeerMetrics().getLowThresholdMs());
      assertEquals(123, dn.getPeerMetrics().getMinOutlierDetectionSamples());
      assertEquals(123,
          dn.getPeerMetrics().getSlowNodeDetector().getMinOutlierDetectionNodes());
      assertEquals(123,
          dn.getPeerMetrics().getSlowNodeDetector().getLowThresholdMs());

      // Revert to default and verify.
      dn.reconfigureProperty(DFS_DATANODE_PEER_STATS_ENABLED_KEY, null);
      assertEquals(String.format("expect %s is not configured",
          DFS_DATANODE_PEER_STATS_ENABLED_KEY), null,
          dn.getConf().get(DFS_DATANODE_PEER_STATS_ENABLED_KEY));

      // Reset DFS_DATANODE_PEER_STATS_ENABLED_KEY to true.
      dn.reconfigureProperty(DFS_DATANODE_PEER_STATS_ENABLED_KEY, "true");

      for (String parameter : slowPeersParameters) {
        dn.reconfigureProperty(parameter, null);
      }
      assertEquals(String.format("expect %s is not configured",
          DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY), null,
          dn.getConf().get(DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY));
      assertEquals(String.format("expect %s is not configured",
          DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY), null,
          dn.getConf().get(DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY));
      assertEquals(String.format("expect %s is not configured",
          DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY), null,
          dn.getConf().get(DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY));
      assertEquals(dn.getPeerMetrics().getSlowNodeDetector().getMinOutlierDetectionNodes(),
          DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_DEFAULT);
      assertEquals(dn.getPeerMetrics().getSlowNodeDetector().getLowThresholdMs(),
          DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_DEFAULT);
    }
  }

  @Test
  public void testSlowDiskParameters() throws ReconfigurationException, IOException {
    String[] slowDisksParameters1 = {
        DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY,
        DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY};

    for (int i = 0; i < NUM_DATA_NODE; i++) {
      DataNode dn = cluster.getDataNodes().get(i);

      // Try invalid values.
      try {
        dn.reconfigureProperty(DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY, "text");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting NumberFormatException",
            expected.getCause() instanceof NumberFormatException);
      }

      try {
        dn.reconfigureProperty(DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY, "text");
      } catch (ReconfigurationException expected) {
        assertTrue("expecting NumberFormatException",
            expected.getCause() instanceof NumberFormatException);
      }

      // Enable disk stats, make DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY > 0.
      dn.reconfigureProperty(DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY, "1");
      for (String parameter : slowDisksParameters1) {
        try {
          dn.reconfigureProperty(parameter, "text");
          fail("ReconfigurationException expected");
        } catch (ReconfigurationException expected) {
          assertTrue("expecting NumberFormatException",
              expected.getCause() instanceof NumberFormatException);
        }

        try {
          dn.reconfigureProperty(parameter, String.valueOf(-1));
          fail("ReconfigurationException expected");
        } catch (ReconfigurationException expected) {
          assertTrue("expecting IllegalArgumentException",
              expected.getCause() instanceof IllegalArgumentException);
        }
      }

      // Change and verify properties.
      dn.reconfigureProperty(DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY, "1ms");
      assertEquals(1, dn.getDnConf().outliersReportIntervalMs);

      BlockPoolManager blockPoolManager = new BlockPoolManager(dn);
      blockPoolManager.refreshNamenodes(dn.getConf());
      for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
        if (bpos != null) {
          for (BPServiceActor actor : bpos.getBPServiceActors()) {
            assertEquals(String.format("%s has wrong value",
                DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY),
                1, actor.getScheduler().getOutliersReportIntervalMs());
          }
        }
      }

      String[] slowDisksParameters2 = {
          DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY,
          DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY,
          DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY,
          DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY};
      for (String parameter : slowDisksParameters2) {
        dn.reconfigureProperty(parameter, "99");
      }
      // Assert diskMetrics.
      assertEquals(99, dn.getDiskMetrics().getMinOutlierDetectionDisks());
      assertEquals(99, dn.getDiskMetrics().getLowThresholdMs());
      assertEquals(99, dn.getDiskMetrics().getMaxSlowDisksToExclude());
      // Assert dnConf.
      assertTrue(dn.getDnConf().diskStatsEnabled);
      // Assert profilingEventHook.
      assertTrue(dn.getFileIoProvider().getProfilingEventHook().getDiskStatsEnabled());
      assertEquals((int) ((double) 99 / 100 * Integer.MAX_VALUE),
          dn.getFileIoProvider().getProfilingEventHook().getSampleRangeMax());
      // Assert slowDiskDetector.
      assertEquals(99,
          dn.getDiskMetrics().getSlowDiskDetector().getMinOutlierDetectionNodes());
      assertEquals(99,
          dn.getDiskMetrics().getSlowDiskDetector().getLowThresholdMs());

      // Revert to default and verify.
      dn.reconfigureProperty(DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY, null);
      assertEquals(String.format("expect %s is not configured",
          DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY), null,
          dn.getConf().get(DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY));

      dn.reconfigureProperty(DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY, null);
      assertEquals(String.format("expect %s is not configured",
          DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY), null,
          dn.getConf().get(DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY));
      assertFalse(dn.getFileIoProvider().getProfilingEventHook().getDiskStatsEnabled());
      assertEquals(0,
          dn.getFileIoProvider().getProfilingEventHook().getSampleRangeMax());

      // Enable disk stats, make DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY > 0.
      dn.reconfigureProperty(DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY, "1");
      dn.reconfigureProperty(DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY, null);
      dn.reconfigureProperty(DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY, null);
      dn.reconfigureProperty(DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY, null);
      assertEquals(String.format("expect %s is not configured",
          DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY), null,
          dn.getConf().get(DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY));
      assertEquals(String.format("expect %s is not configured",
          DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY), null,
          dn.getConf().get(DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY));
      assertEquals(String.format("expect %s is not configured",
          DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY), null,
          dn.getConf().get(DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY));
      assertEquals(DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_DEFAULT,
          dn.getDiskMetrics().getSlowDiskDetector().getMinOutlierDetectionNodes());
      assertEquals(DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_DEFAULT,
          dn.getDiskMetrics().getSlowDiskDetector().getLowThresholdMs());
    }
  }

  @Test
  public void testDfsUsageParameters() throws ReconfigurationException {
    String[] dfsUsageParameters = {
        FS_DU_INTERVAL_KEY,
        FS_GETSPACEUSED_JITTER_KEY};

    for (int i = 0; i < NUM_DATA_NODE; i++) {
      DataNode dn = cluster.getDataNodes().get(i);

      // Try invalid values.
      for (String parameter : dfsUsageParameters) {
        try {
          dn.reconfigureProperty(parameter, "text");
          fail("ReconfigurationException expected");
        } catch (ReconfigurationException expected) {
          assertTrue("expecting NumberFormatException",
              expected.getCause() instanceof NumberFormatException);
        }

        try {
          dn.reconfigureProperty(parameter, String.valueOf(-1));
          fail("ReconfigurationException expected");
        } catch (ReconfigurationException expected) {
          assertTrue("expecting IllegalArgumentException",
              expected.getCause() instanceof IllegalArgumentException);
        }
      }

      // Change and verify properties.
      for (String parameter : dfsUsageParameters) {
        dn.reconfigureProperty(parameter, "99");
      }
      List<FsVolumeImpl> volumeList = dn.data.getVolumeList();
      for (FsVolumeImpl fsVolume : volumeList) {
        Map<String, BlockPoolSlice> blockPoolSlices = fsVolume.getBlockPoolSlices();
        for (Map.Entry<String, BlockPoolSlice> entry : blockPoolSlices.entrySet()) {
          GetSpaceUsed dfsUsage = entry.getValue().getDfsUsage();
          if (dfsUsage instanceof CachingGetSpaceUsed) {
            assertEquals(99,
                ((CachingGetSpaceUsed) entry.getValue().getDfsUsage()).getRefreshInterval());
            assertEquals(99,
                ((CachingGetSpaceUsed) entry.getValue().getDfsUsage()).getJitter());
          }
        }
      }

      // Revert to default and verify.
      for (String parameter : dfsUsageParameters) {
        dn.reconfigureProperty(parameter, null);
      }
      for (FsVolumeImpl fsVolume : volumeList) {
        Map<String, BlockPoolSlice> blockPoolSlices = fsVolume.getBlockPoolSlices();
        for (Map.Entry<String, BlockPoolSlice> entry : blockPoolSlices.entrySet()) {
          GetSpaceUsed dfsUsage = entry.getValue().getDfsUsage();
          if (dfsUsage instanceof CachingGetSpaceUsed) {
            assertEquals(String.format("expect %s is not configured",
                FS_DU_INTERVAL_KEY), FS_DU_INTERVAL_DEFAULT,
                ((CachingGetSpaceUsed) entry.getValue().getDfsUsage()).getRefreshInterval());
            assertEquals(String.format("expect %s is not configured",
                FS_GETSPACEUSED_JITTER_KEY), FS_GETSPACEUSED_JITTER_DEFAULT,
                ((CachingGetSpaceUsed) entry.getValue().getDfsUsage()).getJitter());
          }
          assertEquals(String.format("expect %s is not configured",
              FS_DU_INTERVAL_KEY), null,
              dn.getConf().get(FS_DU_INTERVAL_KEY));
          assertEquals(String.format("expect %s is not configured",
              FS_GETSPACEUSED_JITTER_KEY), null,
              dn.getConf().get(FS_GETSPACEUSED_JITTER_KEY));
        }
      }
    }
  }

  public static class DummyCachingGetSpaceUsed extends CachingGetSpaceUsed {
    public DummyCachingGetSpaceUsed(Builder builder) throws IOException {
      super(builder.setInterval(1000).setJitter(0L));
    }

    @Override
    protected void refresh() {
      counter++;
    }
  }

  @Test
  public void testDfsUsageKlass() throws ReconfigurationException, InterruptedException {

    long lastCounter = counter;
    Thread.sleep(5000);
    assertEquals(lastCounter, counter);

    for (int i = 0; i < NUM_DATA_NODE; i++) {
      DataNode dn = cluster.getDataNodes().get(i);
      dn.reconfigurePropertyImpl(FS_GETSPACEUSED_CLASSNAME,
              DummyCachingGetSpaceUsed.class.getName());
    }

    lastCounter = counter;
    Thread.sleep(5000);
    assertTrue(counter > lastCounter);
  }

  @Test
  public void testDiskBalancerParameters() throws Exception {
    for (int i = 0; i < NUM_DATA_NODE; i++) {
      DataNode dn = cluster.getDataNodes().get(i);

      // Verify DFS_DISK_BALANCER_ENABLED.
      // Try invalid values.
      LambdaTestUtils.intercept(ReconfigurationException.class,
          "Could not change property dfs.disk.balancer.enabled from 'true' to 'text'",
          () -> dn.reconfigureProperty(DFS_DISK_BALANCER_ENABLED, "text"));

      // Set default value.
      dn.reconfigureProperty(DFS_DISK_BALANCER_ENABLED, null);
      assertEquals(dn.getConf().getBoolean(DFS_DISK_BALANCER_ENABLED,
              DFS_DISK_BALANCER_ENABLED_DEFAULT), dn.getDiskBalancer().isDiskBalancerEnabled());

      // Set DFS_DISK_BALANCER_ENABLED to false.
      dn.reconfigureProperty(DFS_DISK_BALANCER_ENABLED, "false");
      assertFalse(dn.getDiskBalancer().isDiskBalancerEnabled());

      // Set DFS_DISK_BALANCER_ENABLED to true.
      dn.reconfigureProperty(DFS_DISK_BALANCER_ENABLED, "true");
      assertTrue(dn.getDiskBalancer().isDiskBalancerEnabled());

      // Verify DFS_DISK_BALANCER_PLAN_VALID_INTERVAL.
      // Try invalid values.
      LambdaTestUtils.intercept(ReconfigurationException.class,
          "Could not change property dfs.disk.balancer.plan.valid.interval from " +
              "'1d' to 'text'",
          () -> dn.reconfigureProperty(DFS_DISK_BALANCER_PLAN_VALID_INTERVAL, "text"));

      // Set default value.
      dn.reconfigureProperty(DFS_DISK_BALANCER_PLAN_VALID_INTERVAL, null);
      assertEquals(dn.getConf().getTimeDuration(DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
          DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS),
          dn.getDiskBalancer().getPlanValidityInterval());
      assertEquals(dn.getConf().getTimeDuration(DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
              DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS),
          dn.getDiskBalancer().getPlanValidityIntervalInConfig());

      // Set value is 6 then 6 milliseconds.
      dn.reconfigureProperty(DFS_DISK_BALANCER_PLAN_VALID_INTERVAL, "" + 6);
      assertEquals(6, dn.getDiskBalancer().getPlanValidityInterval());
      assertEquals(6, dn.getDiskBalancer().getPlanValidityIntervalInConfig());

      // Set value with time unit.
      dn.reconfigureProperty(DFS_DISK_BALANCER_PLAN_VALID_INTERVAL, "1m");
      assertEquals(60000, dn.getDiskBalancer().getPlanValidityInterval());
      assertEquals(60000, dn.getDiskBalancer().getPlanValidityIntervalInConfig());
    }
  }
}
