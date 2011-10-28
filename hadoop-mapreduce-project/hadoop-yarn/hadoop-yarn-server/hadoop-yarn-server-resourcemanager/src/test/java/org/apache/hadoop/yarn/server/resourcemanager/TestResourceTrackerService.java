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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Test;

public class TestResourceTrackerService {

  private final static File TEMP_DIR = new File(System.getProperty(
      "test.build.data", "/tmp"), "decommision");
  private File hostFile = new File(TEMP_DIR + File.separator + "hostFile.txt");
  private MockRM rm;
  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  /**
   * decommissioning using a include hosts file
   */
  @Test
  public void testDecommissionWithIncludeHosts() throws Exception {

    writeToHostsFile("host1", "host2");
    Configuration conf = new Configuration();
    conf.set("yarn.resourcemanager.nodes.include-path", hostFile
        .getAbsolutePath());

    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    int initialMetricCount = ClusterMetrics.getMetrics()
        .getNumDecommisionedNMs();

    HeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    writeToHostsFile("host1");

    rm.getNodesListManager().refreshNodes();

    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    Assert
        .assertEquals(0, ClusterMetrics.getMetrics().getNumDecommisionedNMs());

    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue("Node is not decommisioned.", NodeAction.SHUTDOWN
        .equals(nodeHeartbeat.getNodeAction()));

    checkDecommissionedNMCount(rm, ++initialMetricCount);
  }

  /**
   * decommissioning using a exclude hosts file
   */
  @Test
  public void testDecommissionWithExcludeHosts() throws Exception {
    Configuration conf = new Configuration();
    conf.set("yarn.resourcemanager.nodes.exclude-path", hostFile
        .getAbsolutePath());

    writeToHostsFile("");
    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);

    int initialMetricCount = ClusterMetrics.getMetrics()
        .getNumDecommisionedNMs();

    HeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    writeToHostsFile("host2");

    rm.getNodesListManager().refreshNodes();

    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue("The decommisioned metrics are not updated",
        NodeAction.SHUTDOWN.equals(nodeHeartbeat.getNodeAction()));
    checkDecommissionedNMCount(rm, ++initialMetricCount);
  }
  
  @Test
  public void testNodeRegistrationFailure() throws Exception {
    writeToHostsFile("host1");
    Configuration conf = new Configuration();
    conf.set("yarn.resourcemanager.nodes.include-path", hostFile
        .getAbsolutePath());
    rm = new MockRM(conf);
    rm.start();
    
    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = Records.newRecord(NodeId.class);
    nodeId.setHost("host2");
    nodeId.setPort(1234);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    // trying to register a invalid node.
    RegisterNodeManagerResponse response = resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response.getRegistrationResponse().getNodeAction());
  }

  @Test
  public void testReboot() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = new MockNM("host2:1234", 2048, rm.getResourceTrackerService());

    int initialMetricCount = ClusterMetrics.getMetrics().getNumRebootedNMs();
    HeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    nodeHeartbeat = nm2.nodeHeartbeat(
        new HashMap<ApplicationId, List<ContainerStatus>>(), true,
        recordFactory.newRecordInstance(NodeId.class));
    Assert.assertTrue(NodeAction.REBOOT.equals(nodeHeartbeat.getNodeAction()));
    checkRebootedNMCount(rm, ++initialMetricCount);
  }

  private void checkRebootedNMCount(MockRM rm2, int count)
      throws InterruptedException {
    
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumRebootedNMs() != count
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals("The rebooted metrics are not updated", count,
        ClusterMetrics.getMetrics().getNumRebootedNMs());
  }

  @Test
  public void testUnhealthyNodeStatus() throws Exception {
    Configuration conf = new Configuration();
    conf.set("yarn.resourcemanager.nodes.exclude-path", hostFile
        .getAbsolutePath());

    MockRM rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    Assert.assertEquals(0, ClusterMetrics.getMetrics().getUnhealthyNMs());
    // node healthy
    nm1.nodeHeartbeat(true);

    // node unhealthy
    nm1.nodeHeartbeat(false);
    checkUnealthyNMCount(rm, nm1, true, 1);

    // node healthy again
    nm1.nodeHeartbeat(true);
    checkUnealthyNMCount(rm, nm1, false, 0);
  }
  
  private void checkUnealthyNMCount(MockRM rm, MockNM nm1, boolean health,
      int count) throws Exception {
    
    int waitCount = 0;
    while(rm.getRMContext().getRMNodes().get(nm1.getNodeId())
        .getNodeHealthStatus().getIsNodeHealthy() == health
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertFalse(rm.getRMContext().getRMNodes().get(nm1.getNodeId())
        .getNodeHealthStatus().getIsNodeHealthy() == health);
    Assert.assertEquals("Unhealthy metrics not incremented", count,
        ClusterMetrics.getMetrics().getUnhealthyNMs());
  }

  private void writeToHostsFile(String... hosts) throws IOException {
    if (!hostFile.exists()) {
      TEMP_DIR.mkdirs();
      hostFile.createNewFile();
    }
    FileOutputStream fStream = null;
    try {
      fStream = new FileOutputStream(hostFile);
      for (int i = 0; i < hosts.length; i++) {
        fStream.write(hosts[i].getBytes());
        fStream.write("\n".getBytes());
      }
    } finally {
      if (fStream != null) {
        IOUtils.closeStream(fStream);
        fStream = null;
      }
    }
  }

  private void checkDecommissionedNMCount(MockRM rm, int count)
      throws InterruptedException {
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumDecommisionedNMs() != count
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals(count, ClusterMetrics.getMetrics()
        .getNumDecommisionedNMs());
    Assert.assertEquals("The decommisioned metrics are not updated", count,
        ClusterMetrics.getMetrics().getNumDecommisionedNMs());
  }

  @After
  public void tearDown() {
    if (hostFile != null && hostFile.exists()) {
      hostFile.delete();
    }
    if (rm != null) {
      rm.stop();
    }
  }
}
