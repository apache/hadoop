/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.AutoCloseableLock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Test part 2 for sync all replicas in block recovery.
 */
public class TestBlockRecovery2 {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestBlockRecovery2.class);

  private static final String DATA_DIR =
      MiniDFSCluster.getBaseDirectory() + "data";

  private DataNode dn;
  private Configuration conf;
  private boolean tearDownDone;

  private final static String CLUSTER_ID = "testClusterID";
  private final static String POOL_ID = "BP-TEST";
  private final static InetSocketAddress NN_ADDR = new InetSocketAddress(
      "localhost", 5020);

  @Rule
  public TestName currentTestName = new TestName();

  static {
    GenericTestUtils.setLogLevel(FSNamesystem.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(LOG, Level.TRACE);
  }

  /**
   * Starts an instance of DataNode.
   * @throws IOException
   */
  @Before
  public void startUp() throws IOException {
    tearDownDone = false;
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, DATA_DIR);
    conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "0.0.0.0:0");
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    FileSystem.setDefaultUri(conf,
        "hdfs://" + NN_ADDR.getHostName() + ":" + NN_ADDR.getPort());
    List<StorageLocation> locations = new ArrayList<>();
    File dataDir = new File(DATA_DIR);
    FileUtil.fullyDelete(dataDir);
    dataDir.mkdirs();
    StorageLocation location = StorageLocation.parse(dataDir.getPath());
    locations.add(location);
    final DatanodeProtocolClientSideTranslatorPB namenode =
        mock(DatanodeProtocolClientSideTranslatorPB.class);

    Mockito.doAnswer(
        (Answer<DatanodeRegistration>) invocation ->
            (DatanodeRegistration) invocation.getArguments()[0])
        .when(namenode)
        .registerDatanode(Mockito.any(DatanodeRegistration.class));

    when(namenode.versionRequest())
        .thenReturn(new NamespaceInfo(1, CLUSTER_ID, POOL_ID, 1L));

    when(namenode.sendHeartbeat(
        Mockito.any(),
        Mockito.any(),
        Mockito.anyLong(),
        Mockito.anyLong(),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(),
        Mockito.anyBoolean(),
        Mockito.any(),
        Mockito.any()))
        .thenReturn(new HeartbeatResponse(
            new DatanodeCommand[0],
            new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.ACTIVE, 1),
            null, ThreadLocalRandom.current().nextLong() | 1L));

    dn = new DataNode(conf, locations, null, null) {
      @Override
      DatanodeProtocolClientSideTranslatorPB connectToNN(
          InetSocketAddress nnAddr) throws IOException {
        Assert.assertEquals(NN_ADDR, nnAddr);
        return namenode;
      }
    };
    // Trigger a heartbeat so that it acknowledges the NN as active.
    dn.getAllBpOs().get(0).triggerHeartbeatForTests();
    waitForActiveNN();
  }

  /**
   * Wait for active NN up to 15 seconds.
   */
  private void waitForActiveNN() {
    try {
      GenericTestUtils.waitFor(() ->
          dn.getAllBpOs().get(0).getActiveNN() != null, 1000, 15 * 1000);
    } catch (TimeoutException e) {
      // Here its not failing, will again do the assertions for activeNN after
      // this waiting period and fails there if BPOS has not acknowledged
      // any NN as active.
      LOG.warn("Failed to get active NN", e);
    } catch (InterruptedException e) {
      LOG.warn("InterruptedException while waiting to see active NN", e);
    }
    Assert.assertNotNull("Failed to get ActiveNN",
        dn.getAllBpOs().get(0).getActiveNN());
  }

  /**
   * Cleans the resources and closes the instance of datanode.
   * @throws IOException if an error occurred
   */
  @After
  public void tearDown() throws IOException {
    if (!tearDownDone && dn != null) {
      try {
        dn.shutdown();
      } catch(Exception e) {
        LOG.error("Cannot close: ", e);
      } finally {
        File dir = new File(DATA_DIR);
        if (dir.exists()) {
          Assert.assertTrue(
              "Cannot delete data-node dirs", FileUtil.fullyDelete(dir));
        }
      }
      tearDownDone = true;
    }
  }

  /**
   * Test to verify the race between finalizeBlock and Lease recovery.
   *
   * @throws Exception
   */
  @Test(timeout = 20000)
  public void testRaceBetweenReplicaRecoveryAndFinalizeBlock()
      throws Exception {
    // Stop the Mocked DN started in startup()
    tearDown();

    Configuration configuration = new HdfsConfiguration();
    configuration.setLong(
        DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY, 5000L);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(configuration)
        .numDataNodes(1).build();
    try {
      cluster.waitClusterUp();
      DistributedFileSystem fs = cluster.getFileSystem();
      Path path = new Path("/test");
      FSDataOutputStream out = fs.create(path);
      out.writeBytes("data");
      out.hsync();

      List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(fs.open(path));
      final LocatedBlock block = blocks.get(0);
      final DataNode dataNode = cluster.getDataNodes().get(0);

      final AtomicBoolean recoveryInitResult = new AtomicBoolean(true);
      Thread recoveryThread = new Thread(() -> {
        try {
          DatanodeInfo[] locations = block.getLocations();
          final BlockRecoveryCommand.RecoveringBlock recoveringBlock =
              new BlockRecoveryCommand.RecoveringBlock(block.getBlock(),
                  locations, block.getBlock().getGenerationStamp() + 1);
          try(AutoCloseableLock lock = dataNode.data.acquireDatasetLock()) {
            Thread.sleep(2000);
            dataNode.initReplicaRecovery(recoveringBlock);
          }
        } catch (Exception e) {
          LOG.error("Something went wrong.", e);
          recoveryInitResult.set(false);
        }
      });
      recoveryThread.start();
      try {
        out.close();
      } catch (IOException e) {
        Assert.assertTrue("Writing should fail",
            e.getMessage().contains("are bad. Aborting..."));
      } finally {
        recoveryThread.join();
      }
      Assert.assertTrue("Recovery should be initiated successfully",
          recoveryInitResult.get());

      dataNode.updateReplicaUnderRecovery(block.getBlock(), block.getBlock()
              .getGenerationStamp() + 1, block.getBlock().getBlockId(),
          block.getBlockSize());
    } finally {
      if (null != cluster) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test for block recovery timeout. All recovery attempts will be delayed
   * and the first attempt will be lost to trigger recovery timeout and retry.
   */
  @Test(timeout = 300000L)
  public void testRecoveryTimeout() throws Exception {
    tearDown(); // Stop the Mocked DN started in startup()
    final Random r = new Random();

    // Make sure first commitBlockSynchronization call from the DN gets lost
    // for the recovery timeout to expire and new recovery attempt
    // to be started.
    GenericTestUtils.SleepAnswer delayer =
        new GenericTestUtils.SleepAnswer(3000) {
      private final AtomicBoolean callRealMethod = new AtomicBoolean();

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        boolean interrupted = false;
        try {
          Thread.sleep(r.nextInt(3000) + 6000);
        } catch (InterruptedException ie) {
          interrupted = true;
        }
        try {
          if (callRealMethod.get()) {
            return invocation.callRealMethod();
          }
          callRealMethod.set(true);
          return null;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
    };
    TestBlockRecovery.testRecoveryWithDatanodeDelayed(delayer);
  }

  /**
   * Test for block recovery taking longer than the heartbeat interval.
   */
  @Test(timeout = 300000L)
  public void testRecoverySlowerThanHeartbeat() throws Exception {
    tearDown(); // Stop the Mocked DN started in startup()

    GenericTestUtils.SleepAnswer delayer =
        new GenericTestUtils.SleepAnswer(3000, 6000);
    TestBlockRecovery.testRecoveryWithDatanodeDelayed(delayer);
  }

  @Test(timeout = 60000)
  public void testEcRecoverBlocks() throws Throwable {
    // Stop the Mocked DN started in startup()
    tearDown();
    ErasureCodingPolicy ecPolicy = StripedFileTestUtil.getDefaultECPolicy();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(8).build();

    try {
      cluster.waitActive();
      NamenodeProtocols preSpyNN = cluster.getNameNodeRpc();
      NamenodeProtocols spyNN = spy(preSpyNN);

      // Delay completeFile
      GenericTestUtils.DelayAnswer delayer =
          new GenericTestUtils.DelayAnswer(LOG);
      doAnswer(delayer).when(spyNN).complete(anyString(), anyString(), any(),
          anyLong());
      String topDir = "/myDir";
      DFSClient client = new DFSClient(null, spyNN, conf, null);
      Path file = new Path(topDir + "/testECLeaseRecover");
      client.mkdirs(topDir, null, false);
      client.enableErasureCodingPolicy(ecPolicy.getName());
      client.setErasureCodingPolicy(topDir, ecPolicy.getName());
      OutputStream stm = client.create(file.toString(), true);

      // write 5MB File
      AppendTestUtil.write(stm, 0, 1024 * 1024 * 5);
      final AtomicReference<Throwable> err = new AtomicReference<>();
      Thread t = new Thread(() -> {
        try {
          stm.close();
        } catch (Throwable t1) {
          err.set(t1);
        }
      });
      t.start();

      // Waiting for close to get to latch
      delayer.waitForCall();
      GenericTestUtils.waitFor(() -> {
        try {
          return client.getNamenode().recoverLease(file.toString(),
              client.getClientName());
        } catch (IOException e) {
          return false;
        }
      }, 5000, 24000);
      delayer.proceed();
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test that block will be recovered even if there are less than the
   * specified minReplication datanodes involved in its recovery.
   *
   * Check that, after recovering, the block will be successfully replicated.
   */
  @Test(timeout = 300000L)
  public void testRecoveryWillIgnoreMinReplication() throws Exception {
    tearDown(); // Stop the Mocked DN started in startup()

    final int blockSize = 4096;
    final int numReplicas = 3;
    final String filename = "/testIgnoreMinReplication";
    final Path filePath = new Path(filename);
    Configuration configuration = new HdfsConfiguration();
    configuration.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 2000);
    configuration.setInt(DFS_NAMENODE_REPLICATION_MIN_KEY, 2);
    configuration.setLong(DFS_BLOCK_SIZE_KEY, blockSize);
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(configuration).numDataNodes(5)
          .build();
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FSNamesystem fsn = cluster.getNamesystem();

      // Create a file and never close the output stream to trigger recovery
      FSDataOutputStream out = dfs.create(filePath, (short) numReplicas);
      out.write(AppendTestUtil.randomBytes(0, blockSize));
      out.hsync();

      DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost",
          cluster.getNameNodePort()), configuration);
      LocatedBlock blk = dfsClient.getNamenode().
          getBlockLocations(filename, 0, blockSize).
          getLastLocatedBlock();

      // Kill 2 out of 3 datanodes so that only 1 alive, thus < minReplication
      List<DatanodeInfo> dataNodes = Arrays.asList(blk.getLocations());
      assertEquals(dataNodes.size(), numReplicas);
      for (DatanodeInfo dataNode : dataNodes.subList(0, numReplicas - 1)) {
        cluster.stopDataNode(dataNode.getName());
      }

      GenericTestUtils.waitFor(() -> fsn.getNumDeadDataNodes() == 2,
          300, 300000);

      // Make sure hard lease expires to trigger replica recovery
      cluster.setLeasePeriod(100L, 100L);

      // Wait for recovery to succeed
      GenericTestUtils.waitFor(() -> {
        try {
          return dfs.isFileClosed(filePath);
        } catch (IOException e) {
          LOG.info("Something went wrong.", e);
        }
        return false;
      }, 300, 300000);

      // Wait for the block to be replicated
      DFSTestUtil.waitForReplication(cluster, DFSTestUtil.getFirstBlock(
          dfs, filePath), 1, numReplicas, 0);

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

}
