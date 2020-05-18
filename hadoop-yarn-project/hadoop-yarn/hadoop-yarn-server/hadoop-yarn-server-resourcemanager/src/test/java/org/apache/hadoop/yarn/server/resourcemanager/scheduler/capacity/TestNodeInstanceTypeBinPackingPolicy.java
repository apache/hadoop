package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSorter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.Iterator;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNodeInstanceTypeBinPackingPolicy {
    private static final Logger LOG =
        LoggerFactory.getLogger(TestNodeInstanceTypeBinPackingPolicy.class);
    private static final String POLICY_CLASS_NAME =
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement."
        + "NodeInstanceTypeBinPackingPolicy";
    private CapacitySchedulerConfiguration conf;

    @Before
    public void setUp() {
      CapacitySchedulerConfiguration config =
          new CapacitySchedulerConfiguration();
      config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
          DominantResourceCalculator.class.getName());
      conf = new CapacitySchedulerConfiguration(config);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES,
          "instancetype-based");
      conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
          "instancetype-based");
      String policyName =
          CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
          + ".instancetype-based" + ".class";
      conf.set(policyName, POLICY_CLASS_NAME);
      conf.setBoolean(CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED,
          true);
      conf.set(CapacitySchedulerConfiguration
          .MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, "1");
      conf.setInt("yarn.scheduler.minimum-allocation-mb", 512);
      conf.setInt("yarn.scheduler.minimum-allocation-vcores", 1);
      conf.setInt("yarn.scheduler.maximum-allocation-mb", 102400);
    }

    /**
     * Test the nodes ordering for AM and Task Schedule.
     * */
    @Test
    public void testNodeInstanceTypeOrdering() throws Exception {
        MockRM rm = new MockRM(conf);
        rm.start();
        MockNM w1 = rm.registerNode("worker1:1234", 10 * GB, 10);
        MockNM w2 = rm.registerNode("worker2:1234", 20 * GB, 10);
        MockNM w3 = rm.registerNode("worker3:1234", 30 * GB, 10);
        MockNM c1 = rm.registerNode("compute1:1234", 10 * GB, 10);
        MockNM c2 = rm.registerNode("compute2:1234", 20 * GB, 10);
        MockNM c3 = rm.registerNode("compute3:1234", 30 * GB, 10);
        MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
                .getMultiNodeSortingManager();
        MultiNodeSorter<SchedulerNode> sorter = mns
                .getMultiNodePolicy(POLICY_CLASS_NAME);
        sorter.reSortClusterNodes();
        MockRMAppSubmissionData data =
            MockRMAppSubmissionData.Builder.createWithMemory(30 * GB, rm)
                .withAppName("app-1")
                .withUser("user1")
                .withAcls(null)
                .withQueue("default")
                .build();
        RMApp app1 = MockRMAppSubmitter.submit(rm, data);
        MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, w3);
        am1.allocateAndWaitForContainers("compute3", 1, 30 * GB, c3);

        sorter.reSortClusterNodes();
        MockRMAppSubmissionData data2 =
            MockRMAppSubmissionData.Builder.createWithMemory(15 * GB, rm)
                .withAppName("app-2")
                .withUser("user2")
                .withAcls(null)
                .withQueue("default")
                .build();
        RMApp app2 = MockRMAppSubmitter.submit(rm, data2);
        MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, w2);
        am2.allocateAndWaitForContainers("compute2", 1, 15 * GB, c2);
        sorter.reSortClusterNodes();

        NodeId[] amNodesOrder = {w2.getNodeId(), w1.getNodeId(), w3.getNodeId(),
                c2.getNodeId(), c1.getNodeId(), c3.getNodeId()};
        NodeId[] taskNodesOrder = {c2.getNodeId(), c1.getNodeId(), c3.getNodeId(),
                w2.getNodeId(), w1.getNodeId(), w3.getNodeId()};
        validateNodesOrder(sorter, amNodesOrder, true);
        validateNodesOrder(sorter, taskNodesOrder, false);
    }

    private void validateNodesOrder(MultiNodeSorter<SchedulerNode> sorter,
        NodeId[] nodesOrder, boolean isAMContainer) {
      SchedulerApplicationAttempt mockAppAttempt = mock(
          SchedulerApplicationAttempt.class);
      when(mockAppAttempt.isWaitingForAMContainer()).
          thenReturn(isAMContainer);
      Set<SchedulerNode> nodeList = sorter.getMultiNodeLookupPolicy()
          .getNodesPerPartition("");
      Iterator<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
          .getPreferredNodeIterator(nodeList, "", mockAppAttempt);
      int i=0;
      while(nodes.hasNext()) {
        Assert.assertEquals("Nodes ordering does not match",
            nodes.next().getNodeID(), nodesOrder[i++]);
      }
      Assert.assertEquals("Nodes size does not match", i, nodesOrder.length);
    }

    private void heartbeat(MockRM rm, MockNM nm) {
      RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
      // Send a heartbeat to kick the tires on the Scheduler
      NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
      rm.getResourceScheduler().handle(nodeUpdate);
    }
}


