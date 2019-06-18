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

import static org.apache.hadoop.fs.CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NN_NOT_BECOME_ACTIVE_IN_SAFEMODE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.MockNameNodeResourceChecker;
import org.apache.hadoop.hdfs.tools.NNHAServiceTarget;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNNHealthCheck {

  private MiniDFSCluster cluster;
  private Configuration conf;

  @Before
  public void setup() {
    conf = new Configuration();
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testNNHealthCheck() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .build();
    doNNHealthCheckTest();
  }

  @Test
  public void testNNHealthCheckWithLifelineAddress() throws IOException {
    conf.set(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY, "0.0.0.0:0");
    cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(0)
          .nnTopology(MiniDFSNNTopology.simpleHATopology())
          .build();
    doNNHealthCheckTest();
  }

  @Test
  public void testNNHealthCheckWithSafemodeAsUnhealthy() throws Exception {
    conf.setBoolean(DFS_HA_NN_NOT_BECOME_ACTIVE_IN_SAFEMODE, true);

    // now bring up just the NameNode.
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).build();
    cluster.waitActive();

    // manually set safemode.
    cluster.getFileSystem(0)
        .setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);

    NNHAServiceTarget haTarget = new NNHAServiceTarget(conf,
        DFSUtil.getNamenodeNameServiceId(conf), "nn1");
    final String expectedTargetString = haTarget.getAddress().toString();

    assertTrue("Expected haTarget " + haTarget + " containing " +
            expectedTargetString,
        haTarget.toString().contains(expectedTargetString));
    HAServiceProtocol rpc = haTarget.getHealthMonitorProxy(conf, 5000);

    LambdaTestUtils.intercept(RemoteException.class,
        "The NameNode is configured to report UNHEALTHY to ZKFC in Safemode.",
        () -> rpc.monitorHealth());
  }

  private void doNNHealthCheckTest() throws IOException {
    MockNameNodeResourceChecker mockResourceChecker =
        new MockNameNodeResourceChecker(conf);
    cluster.getNameNode(0).getNamesystem()
        .setNNResourceChecker(mockResourceChecker);

    NNHAServiceTarget haTarget = new NNHAServiceTarget(conf,
        DFSUtil.getNamenodeNameServiceId(conf), "nn1");
    final String expectedTargetString;
    if (conf.get(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + "." +
        DFSUtil.getNamenodeNameServiceId(conf) + ".nn1") != null) {
      expectedTargetString = haTarget.getHealthMonitorAddress().toString();
    } else {
      expectedTargetString = haTarget.getAddress().toString();
    }
    assertTrue("Expected haTarget " + haTarget + " containing " +
        expectedTargetString,
        haTarget.toString().contains(expectedTargetString));
    HAServiceProtocol rpc = haTarget.getHealthMonitorProxy(conf, conf.getInt(
        HA_HM_RPC_TIMEOUT_KEY, HA_HM_RPC_TIMEOUT_DEFAULT));

    // Should not throw error, which indicates healthy.
    rpc.monitorHealth();

    mockResourceChecker.setResourcesAvailable(false);

    try {
      // Should throw error - NN is unhealthy.
      rpc.monitorHealth();
      fail("Should not have succeeded in calling monitorHealth");
    } catch (HealthCheckFailedException hcfe) {
      GenericTestUtils.assertExceptionContains(
          "The NameNode has no resources available", hcfe);
    } catch (RemoteException re) {
      GenericTestUtils.assertExceptionContains(
          "The NameNode has no resources available",
          re.unwrapRemoteException(HealthCheckFailedException.class));
    }
  }
}
