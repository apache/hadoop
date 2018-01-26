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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that StoragePolicySatisfier is able to work with HA enabled.
 */
public class TestStoragePolicySatisfierWithHA {
  private MiniDFSCluster cluster = null;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestStoragePolicySatisfierWithHA.class);

  private final Configuration config = new HdfsConfiguration();
  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private DistributedFileSystem dfs = null;

  private StorageType[][] allDiskTypes =
      new StorageType[][]{{StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK}};
  private int numOfDatanodes = 3;
  private int storagesPerDatanode = 2;
  private long capacity = 2 * 256 * 1024 * 1024;
  private int nnIndex = 0;

  private void createCluster() throws IOException {
    config.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    config.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.INTERNAL.toString());
    startCluster(config, allDiskTypes, numOfDatanodes, storagesPerDatanode,
        capacity);
    dfs = cluster.getFileSystem(nnIndex);
  }

  private void startCluster(final Configuration conf,
      StorageType[][] storageTypes, int numberOfDatanodes, int storagesPerDn,
      long nodeCapacity) throws IOException {
    long[][] capacities = new long[numberOfDatanodes][storagesPerDn];
    for (int i = 0; i < numberOfDatanodes; i++) {
      for (int j = 0; j < storagesPerDn; j++) {
        capacities[i][j] = nodeCapacity;
      }
    }
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(numberOfDatanodes).storagesPerDatanode(storagesPerDn)
        .storageTypes(storageTypes).storageCapacities(capacities).build();
    cluster.waitActive();
    cluster.transitionToActive(0);
  }

  /**
   * Tests to verify that SPS should run/stop automatically when NN state
   * changes between Standby and Active.
   */
  @Test(timeout = 90000)
  public void testWhenNNHAStateChanges() throws IOException {
    try {
      createCluster();
      boolean running;

      dfs = cluster.getFileSystem(1);

      try {
        dfs.getClient().isStoragePolicySatisfierRunning();
        Assert.fail("Call this function to Standby NN should "
            + "raise an exception.");
      } catch (RemoteException e) {
        IOException cause = e.unwrapRemoteException();
        if (!(cause instanceof StandbyException)) {
          Assert.fail("Unexpected exception happened " + e);
        }
      }

      cluster.transitionToActive(0);
      dfs = cluster.getFileSystem(0);
      running = dfs.getClient().isStoragePolicySatisfierRunning();
      Assert.assertTrue("StoragePolicySatisfier should be active "
          + "when NN transits from Standby to Active mode.", running);

      // NN transits from Active to Standby
      cluster.transitionToStandby(0);
      try {
        dfs.getClient().isStoragePolicySatisfierRunning();
        Assert.fail("NN in Standby again, call this function should "
            + "raise an exception.");
      } catch (RemoteException e) {
        IOException cause = e.unwrapRemoteException();
        if (!(cause instanceof StandbyException)) {
          Assert.fail("Unexpected exception happened " + e);
        }
      }

      try {
        cluster.getNameNode(0).reconfigurePropertyImpl(
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
            StoragePolicySatisfierMode.EXTERNAL.toString());
        Assert.fail("It's not allowed to enable or disable"
            + " StoragePolicySatisfier on Standby NameNode");
      } catch (ReconfigurationException e) {
        GenericTestUtils.assertExceptionContains("Could not change property "
            + DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY
            + " from 'INTERNAL' to 'EXTERNAL'", e);
        GenericTestUtils.assertExceptionContains(
            "Enabling or disabling storage policy satisfier service on "
                + "standby NameNode is not allowed", e.getCause());
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test to verify that during namenode switch over will add
   * DNA_DROP_SPS_WORK_COMMAND to all the datanodes. Later, this will ensure to
   * drop all the SPS queues at datanode.
   */
  @Test(timeout = 90000)
  public void testNamenodeSwitchoverShouldDropSPSWork() throws Exception {
    try {
      createCluster();

      FSNamesystem fsn = cluster.getNamesystem(0);
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();
      List<DatanodeDescriptor> listOfDns = new ArrayList<>();
      for (DataNode dn : dataNodes) {
        DatanodeDescriptor dnd = NameNodeAdapter.getDatanode(fsn,
            dn.getDatanodeId());
        listOfDns.add(dnd);
      }
      cluster.shutdownDataNodes();

      cluster.transitionToStandby(0);
      LOG.info("**Transition to Active**");
      cluster.transitionToActive(1);

      // Verify that Standby-to-Active transition should set drop SPS flag to
      // true. This will ensure that DNA_DROP_SPS_WORK_COMMAND will be
      // propagated to datanode during heartbeat response.
      int retries = 20;
      boolean dropSPSWork = false;
      while (retries > 0) {
        for (DatanodeDescriptor dnd : listOfDns) {
          dropSPSWork = dnd.shouldDropSPSWork();
          if (!dropSPSWork) {
            retries--;
            Thread.sleep(250);
            break;
          }
        }
        if (dropSPSWork) {
          break;
        }
      }
      Assert.assertTrue("Didn't drop SPS work", dropSPSWork);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test to verify that SPS work will be dropped once the datanode is marked as
   * expired. Internally 'dropSPSWork' flag is set as true while expiration and
   * at the time of reconnection, will send DNA_DROP_SPS_WORK_COMMAND to that
   * datanode.
   */
  @Test(timeout = 90000)
  public void testDeadDatanode() throws Exception {
    int heartbeatExpireInterval = 2 * 2000;
    config.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        3000);
    config.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1000L);
    createCluster();

    DataNode dn = cluster.getDataNodes().get(0);
    DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);

    FSNamesystem fsn = cluster.getNamesystem(0);
    DatanodeDescriptor dnd = NameNodeAdapter.getDatanode(fsn,
        dn.getDatanodeId());
    boolean isDead = false;
    int retries = 20;
    while (retries > 0) {
      isDead = dnd.getLastUpdateMonotonic() < (monotonicNow()
          - heartbeatExpireInterval);
      if (isDead) {
        break;
      }
      retries--;
      Thread.sleep(250);
    }
    Assert.assertTrue("Datanode is alive", isDead);
    // Disable datanode heartbeat, so that the datanode will get expired after
    // the recheck interval and become dead.
    DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, false);

    // Verify that datanode expiration will set drop SPS flag to
    // true. This will ensure that DNA_DROP_SPS_WORK_COMMAND will be
    // propagated to datanode during reconnection.
    boolean dropSPSWork = false;
    retries = 50;
    while (retries > 0) {
      dropSPSWork = dnd.shouldDropSPSWork();
      if (dropSPSWork) {
        break;
      }
      retries--;
      Thread.sleep(100);
    }
    Assert.assertTrue("Didn't drop SPS work", dropSPSWork);
  }
}
