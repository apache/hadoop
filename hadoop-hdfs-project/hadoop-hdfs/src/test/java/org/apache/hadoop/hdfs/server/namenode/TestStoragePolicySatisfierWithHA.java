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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests that StoragePolicySatisfier is able to work with HA enabled.
 */
public class TestStoragePolicySatisfierWithHA {
  private MiniDFSCluster cluster = null;

  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(1)
        .build();
  }

  /**
   * Tests to verify that SPS should run/stop automatically when NN state
   * changes between Standby and Active.
   */
  @Test(timeout = 100000)
  public void testWhenNNHAStateChanges() throws IOException {
    try {
      DistributedFileSystem fs;
      boolean running;

      cluster.waitActive();
      fs = cluster.getFileSystem(0);

      try {
        fs.getClient().isStoragePolicySatisfierRunning();
        Assert.fail("Call this function to Standby NN should "
            + "raise an exception.");
      } catch (RemoteException e) {
        IOException cause = e.unwrapRemoteException();
        if (!(cause instanceof StandbyException)) {
          Assert.fail("Unexpected exception happened " + e);
        }
      }

      cluster.transitionToActive(0);
      running = fs.getClient().isStoragePolicySatisfierRunning();
      Assert.assertTrue("StoragePolicySatisfier should be active "
          + "when NN transits from Standby to Active mode.", running);

      // NN transits from Active to Standby
      cluster.transitionToStandby(0);
      try {
        fs.getClient().isStoragePolicySatisfierRunning();
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
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ACTIVATE_KEY, "false");
        Assert.fail("It's not allowed to activate or deactivate"
            + " StoragePolicySatisfier on Standby NameNode");
      } catch (ReconfigurationException e) {
        GenericTestUtils.assertExceptionContains("Could not change property "
            + DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_ACTIVATE_KEY
            + " from 'true' to 'false'", e);
        GenericTestUtils.assertExceptionContains(
            "Activating or deactivating storage policy satisfier service on "
                + "standby NameNode is not allowed", e.getCause());
      }
    } finally {
      cluster.shutdown();
    }
  }
}
