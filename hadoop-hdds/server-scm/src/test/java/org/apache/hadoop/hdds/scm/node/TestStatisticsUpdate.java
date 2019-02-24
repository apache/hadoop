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

package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .NodeReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.security.authentication.client
    .AuthenticationException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.UUID;

/**
 * Verifies the statics in NodeManager.
 */
public class TestStatisticsUpdate {

  private NodeManager nodeManager;
  private NodeReportHandler nodeReportHandler;

  @Before
  public void setup() throws IOException, AuthenticationException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final String storageDir = GenericTestUtils.getTempPath(
        TestDeadNodeHandler.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir);
    conf.set(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, "100ms");
    conf.set(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, "50ms");
    conf.set(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL, "1s");
    conf.set(ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL, "2s");
    final EventQueue eventQueue = new EventQueue();
    final StorageContainerManager scm = HddsTestUtils.getScm(conf);
    nodeManager = scm.getScmNodeManager();
    final DeadNodeHandler deadNodeHandler = new DeadNodeHandler(
        nodeManager, scm.getContainerManager());
    eventQueue.addHandler(SCMEvents.DEAD_NODE, deadNodeHandler);
    nodeReportHandler = new NodeReportHandler(nodeManager);
  }

  @Test
  public void testStatisticsUpdate() throws Exception {
    //GIVEN
    DatanodeDetails datanode1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails datanode2 = TestUtils.randomDatanodeDetails();

    String storagePath1 = GenericTestUtils.getRandomizedTempPath()
        .concat("/" + datanode1.getUuidString());
    String storagePath2 = GenericTestUtils.getRandomizedTempPath()
        .concat("/" + datanode2.getUuidString());

    StorageReportProto storageOne = TestUtils.createStorageReport(
        datanode1.getUuid(), storagePath1, 100, 10, 90, null);
    StorageReportProto storageTwo = TestUtils.createStorageReport(
        datanode2.getUuid(), storagePath2, 200, 20, 180, null);

    nodeManager.register(datanode1,
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(datanode2,
        TestUtils.createNodeReport(storageTwo), null);

    NodeReportProto nodeReportProto1 = TestUtils.createNodeReport(storageOne);
    NodeReportProto nodeReportProto2 = TestUtils.createNodeReport(storageTwo);

    nodeReportHandler.onMessage(
        new NodeReportFromDatanode(datanode1, nodeReportProto1),
        Mockito.mock(EventPublisher.class));
    nodeReportHandler.onMessage(
        new NodeReportFromDatanode(datanode2, nodeReportProto2),
        Mockito.mock(EventPublisher.class));

    SCMNodeStat stat = nodeManager.getStats();
    Assert.assertEquals(300L, stat.getCapacity().get().longValue());
    Assert.assertEquals(270L, stat.getRemaining().get().longValue());
    Assert.assertEquals(30L, stat.getScmUsed().get().longValue());

    SCMNodeMetric nodeStat = nodeManager.getNodeStat(datanode1);
    Assert.assertEquals(100L, nodeStat.get().getCapacity().get().longValue());
    Assert.assertEquals(90L, nodeStat.get().getRemaining().get().longValue());
    Assert.assertEquals(10L, nodeStat.get().getScmUsed().get().longValue());

    //TODO: Support logic to mark a node as dead in NodeManager.

    nodeManager.processHeartbeat(datanode2);
    Thread.sleep(1000);
    nodeManager.processHeartbeat(datanode2);
    Thread.sleep(1000);
    nodeManager.processHeartbeat(datanode2);
    Thread.sleep(1000);
    nodeManager.processHeartbeat(datanode2);
    //THEN statistics in SCM should changed.
    stat = nodeManager.getStats();
    Assert.assertEquals(200L, stat.getCapacity().get().longValue());
    Assert.assertEquals(180L,
        stat.getRemaining().get().longValue());
    Assert.assertEquals(20L, stat.getScmUsed().get().longValue());
  }

}
