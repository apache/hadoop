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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.QueuedContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class TestTopKNodeSelector {

  static class FakeNodeId extends NodeId {
    final String host;
    final int port;

    public FakeNodeId(String host, int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public String getHost() {
      return host;
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    protected void setHost(String host) {}
    @Override
    protected void setPort(int port) {}
    @Override
    protected void build() {}

    @Override
    public String toString() {
      return host + ":" + port;
    }
  }

  @Test
  public void testQueueTimeSort() {
    TopKNodeSelector selector = new TopKNodeSelector(5,
        TopKNodeSelector.TopKComparator.WAIT_TIME);
    selector.nodeUpdate(createRMNode("h1", 1, 15, 10));
    selector.nodeUpdate(createRMNode("h2", 2, 5, 10));
    selector.nodeUpdate(createRMNode("h3", 3, 10, 10));
    selector.computeTask.run();
    List<NodeId> nodeIds = selector.selectNodes();
    System.out.println("1-> " + nodeIds);
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h3:3", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());

    // Now update node3
    selector.nodeUpdate(createRMNode("h3", 3, 2, 10));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    System.out.println("2-> "+ nodeIds);
    Assert.assertEquals("h3:3", nodeIds.get(0).toString());
    Assert.assertEquals("h2:2", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());

    // Now send update with -1 wait time
    selector.nodeUpdate(createRMNode("h4", 4, -1, 10));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    System.out.println("3-> "+ nodeIds);
    // No change
    Assert.assertEquals("h3:3", nodeIds.get(0).toString());
    Assert.assertEquals("h2:2", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());
  }

  @Test
  public void testQueueLengthSort() {
    TopKNodeSelector selector = new TopKNodeSelector(5,
        TopKNodeSelector.TopKComparator.QUEUE_LENGTH);
    selector.nodeUpdate(createRMNode("h1", 1, -1, 15));
    selector.nodeUpdate(createRMNode("h2", 2, -1, 5));
    selector.nodeUpdate(createRMNode("h3", 3, -1, 10));
    selector.computeTask.run();
    List<NodeId> nodeIds = selector.selectNodes();
    System.out.println("1-> " + nodeIds);
    Assert.assertEquals("h2:2", nodeIds.get(0).toString());
    Assert.assertEquals("h3:3", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());

    // Now update node3
    selector.nodeUpdate(createRMNode("h3", 3, -1, 2));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    System.out.println("2-> "+ nodeIds);
    Assert.assertEquals("h3:3", nodeIds.get(0).toString());
    Assert.assertEquals("h2:2", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());

    // Now send update with -1 wait time but valid length
    selector.nodeUpdate(createRMNode("h4", 4, -1, 20));
    selector.computeTask.run();
    nodeIds = selector.selectNodes();
    System.out.println("3-> "+ nodeIds);
    // No change
    Assert.assertEquals("h3:3", nodeIds.get(0).toString());
    Assert.assertEquals("h2:2", nodeIds.get(1).toString());
    Assert.assertEquals("h1:1", nodeIds.get(2).toString());
    Assert.assertEquals("h4:4", nodeIds.get(3).toString());
  }

  private RMNode createRMNode(String host, int port,
      int waitTime, int queueLength) {
    RMNode node1 = Mockito.mock(RMNode.class);
    NodeId nID1 = new FakeNodeId(host, port);
    Mockito.when(node1.getNodeID()).thenReturn(nID1);
    QueuedContainersStatus status1 =
        Mockito.mock(QueuedContainersStatus.class);
    Mockito.when(status1.getEstimatedQueueWaitTime())
        .thenReturn(waitTime);
    Mockito.when(status1.getWaitQueueLength())
        .thenReturn(queueLength);
    Mockito.when(node1.getQueuedContainersStatus()).thenReturn(status1);
    return node1;
  }
}
