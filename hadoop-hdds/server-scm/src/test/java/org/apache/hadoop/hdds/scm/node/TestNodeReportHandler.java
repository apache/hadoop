/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.node;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.NodeReportFromDatanode;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the Node Report Handler.
 */
public class TestNodeReportHandler implements EventPublisher {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestNodeReportHandler.class);
  private NodeReportHandler nodeReportHandler;
  private SCMNodeManager nodeManager;
  private String storagePath = GenericTestUtils.getRandomizedTempPath()
      .concat("/" + UUID.randomUUID().toString());

  @Before
  public void resetEventCollector() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    nodeManager = new SCMNodeManager(conf, "cluster1", null, new EventQueue());
    nodeReportHandler = new NodeReportHandler(nodeManager);
  }

  @Test
  public void testNodeReport() throws IOException {
    DatanodeDetails dn = TestUtils.randomDatanodeDetails();
    StorageReportProto storageOne = TestUtils
        .createStorageReport(dn.getUuid(), storagePath, 100, 10, 90, null);

    SCMNodeMetric nodeMetric = nodeManager.getNodeStat(dn);
    Assert.assertNull(nodeMetric);

    nodeManager.register(dn, getNodeReport(dn, storageOne).getReport(), null);
    nodeMetric = nodeManager.getNodeStat(dn);

    Assert.assertTrue(nodeMetric.get().getCapacity().get() == 100);
    Assert.assertTrue(nodeMetric.get().getRemaining().get() == 90);
    Assert.assertTrue(nodeMetric.get().getScmUsed().get() == 10);

    StorageReportProto storageTwo = TestUtils
        .createStorageReport(dn.getUuid(), storagePath, 100, 10, 90, null);
    nodeReportHandler.onMessage(
        getNodeReport(dn, storageOne, storageTwo), this);
    nodeMetric = nodeManager.getNodeStat(dn);

    Assert.assertTrue(nodeMetric.get().getCapacity().get() == 200);
    Assert.assertTrue(nodeMetric.get().getRemaining().get() == 180);
    Assert.assertTrue(nodeMetric.get().getScmUsed().get() == 20);

  }

  private NodeReportFromDatanode getNodeReport(DatanodeDetails dn,
      StorageReportProto... reports) {
    NodeReportProto nodeReportProto = TestUtils.createNodeReport(reports);
    return new NodeReportFromDatanode(dn, nodeReportProto);
  }

  @Override
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void fireEvent(
      EVENT_TYPE event, PAYLOAD payload) {
    LOG.info("Event is published: {}", payload);
  }
}
