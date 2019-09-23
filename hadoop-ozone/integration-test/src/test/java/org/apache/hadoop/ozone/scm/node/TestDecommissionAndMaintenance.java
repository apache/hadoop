/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.scm.node;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.client.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.TestCase.assertEquals;
import static org.apache.hadoop.hdds.HddsConfigKeys.*;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.*;

/**
 * Test from the scmclient for decommission and maintenance.
 */

public class TestDecommissionAndMaintenance {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDecommissionAndMaintenance.class);

  private static int numOfDatanodes = 5;
  private MiniOzoneCluster cluster;

  private ContainerOperationClient scmClient;

  @Before
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final int interval = 100;

    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        interval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfDatanodes)
        .build();
    cluster.waitForClusterToBeReady();
    scmClient = new ContainerOperationClient(cluster
        .getStorageContainerLocationClient(),
        new XceiverClientManager(conf));
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testNodeCanBeDecommMaintAndRecommissioned()
      throws IOException {
    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();

    List<DatanodeDetails> dns = nm.getAllNodes();
    scmClient.decommissionNodes(Arrays.asList(getDNHostAndPort(dns.get(0))));

    // Ensure one node is decommissioning
    List<DatanodeDetails> decomNodes = nm.getNodes(
        HddsProtos.NodeOperationalState.DECOMMISSIONING,
        HddsProtos.NodeState.HEALTHY);
    assertEquals(1, decomNodes.size());

    scmClient.recommissionNodes(Arrays.asList(getDNHostAndPort(dns.get(0))));

    // Ensure zero nodes are now decommissioning
    decomNodes = nm.getNodes(
        HddsProtos.NodeOperationalState.DECOMMISSIONING,
        HddsProtos.NodeState.HEALTHY);
    assertEquals(0, decomNodes.size());

    scmClient.startMaintenanceNodes(Arrays.asList(
        getDNHostAndPort(dns.get(0))), 10);

    // None are decommissioning
    decomNodes = nm.getNodes(
        HddsProtos.NodeOperationalState.DECOMMISSIONING,
        HddsProtos.NodeState.HEALTHY);
    assertEquals(0, decomNodes.size());

    // One is in Maintenance
    decomNodes = nm.getNodes(
        HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        HddsProtos.NodeState.HEALTHY);
    assertEquals(1, decomNodes.size());

    scmClient.recommissionNodes(Arrays.asList(getDNHostAndPort(dns.get(0))));

    // None are in maintenance
    decomNodes = nm.getNodes(
        HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
        HddsProtos.NodeState.HEALTHY);
    assertEquals(0, decomNodes.size());
  }

  private String getDNHostAndPort(DatanodeDetails dn) {
    return dn.getHostName()+":"+dn.getPorts().get(0).getValue();
  }

}
