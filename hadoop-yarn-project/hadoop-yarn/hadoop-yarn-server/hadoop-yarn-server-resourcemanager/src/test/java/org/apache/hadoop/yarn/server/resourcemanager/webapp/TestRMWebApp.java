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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.server.resourcemanager.MockNodes.newResource;
import static org.apache.hadoop.yarn.webapp.Params.TITLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.MockAsm;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;

public class TestRMWebApp {
  static final int GiB = 1024; // MiB

  @Test
  public void testControllerIndex() {
    Injector injector = WebAppTests.createMockInjector(TestRMWebApp.class,
        this, new Module() {

          @Override
          public void configure(Binder binder) {
            binder.bind(ApplicationACLsManager.class).toInstance(
                new ApplicationACLsManager(new Configuration()));
          }
        });
    RmController c = injector.getInstance(RmController.class);
    c.index();
    assertEquals("Applications", c.get(TITLE, "unknown"));
  }

  @Test public void testView() {
    Injector injector = WebAppTests.createMockInjector(RMContext.class,
        mockRMContext(15, 1, 2, 8*GiB),
        new Module() {
      @Override
      public void configure(Binder binder) {
        try {
          ResourceManager mockRm = mockRm(3, 1, 2, 8*GiB);
          binder.bind(ResourceManager.class).toInstance(mockRm);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    });
    RmView rmViewInstance = injector.getInstance(RmView.class);
    rmViewInstance.set(YarnWebParams.APP_STATE,
        YarnApplicationState.RUNNING.toString());
    rmViewInstance.render();
    WebAppTests.flushOutput(injector);

    rmViewInstance.set(YarnWebParams.APP_STATE, StringHelper.cjoin(
        YarnApplicationState.ACCEPTED.toString(),
        YarnApplicationState.RUNNING.toString()));
    rmViewInstance.render();
    WebAppTests.flushOutput(injector);
    Map<String, String> moreParams =
        rmViewInstance.context().requestContext().moreParams();
    String appsTableColumnsMeta = moreParams.get("ui.dataTables.apps.init");
    Assert.assertTrue(appsTableColumnsMeta.indexOf("natural") != -1);
  }

  @Test public void testNodesPage() {
    // 10 nodes. Two of each type.
    final RMContext rmContext = mockRMContext(3, 2, 12, 8*GiB);
    Injector injector = WebAppTests.createMockInjector(RMContext.class,
        rmContext,
        new Module() {
      @Override
      public void configure(Binder binder) {
        try {
          binder.bind(ResourceManager.class).toInstance(mockRm(rmContext));
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    });

    // All nodes
    NodesPage instance = injector.getInstance(NodesPage.class);
    instance.render();
    WebAppTests.flushOutput(injector);

    // Unhealthy nodes
    instance.moreParams().put(YarnWebParams.NODE_STATE,
      NodeState.UNHEALTHY.toString());
    instance.render();
    WebAppTests.flushOutput(injector);

    // Lost nodes
    instance.moreParams().put(YarnWebParams.NODE_STATE,
      NodeState.LOST.toString());
    instance.render();
    WebAppTests.flushOutput(injector);

  }

  @Test
  public void testRMAppColumnIndices() {

    // Find the columns to check
    List<Integer> colsId = new LinkedList<Integer>();
    List<Integer> colsTime = new LinkedList<Integer>();
    List<Integer> colsProgress = new LinkedList<Integer>();
    for (int i = 0; i < RMAppsBlock.COLUMNS.length; i++) {
      ColumnHeader col = RMAppsBlock.COLUMNS[i];
      if (col.getCData().contains("ID")) {
        colsId.add(i);
      } else if (col.getCData().contains("Time")) {
        colsTime.add(i);
      } else if (col.getCData().contains("Progress")) {
        colsProgress.add(i);
      }
    }

    // Verify that the table JS header matches the columns
    String tableInit = WebPageUtils.appsTableInit(true);
    for (String tableLine : tableInit.split("\\n")) {
      if (tableLine.contains("parseHadoopID")) {
        assertTrue(tableLine + " should have id " + colsId,
            tableLine.contains(colsId.toString()));
      } else if (tableLine.contains("renderHadoopDate")) {
        assertTrue(tableLine + " should have dates " + colsTime,
            tableLine.contains(colsTime.toString()));
      } else if (tableLine.contains("parseHadoopProgress")) {
        assertTrue(tableLine + " should have progress " + colsProgress,
            tableLine.contains(colsProgress.toString()));
      }
    }
  }

  public static RMContext mockRMContext(int numApps, int racks, int numNodes,
      int mbsPerNode) {
    final List<RMApp> apps = MockAsm.newApplications(numApps);
    final ConcurrentMap<ApplicationId, RMApp> applicationsMaps = Maps
        .newConcurrentMap();
    for (RMApp app : apps) {
      applicationsMaps.put(app.getApplicationId(), app);
    }
    final List<RMNode> nodes = MockNodes.newNodes(racks, numNodes,
        newResource(mbsPerNode));
    final ConcurrentMap<NodeId, RMNode> nodesMap = Maps.newConcurrentMap();
    for (RMNode node : nodes) {
      nodesMap.put(node.getNodeID(), node);
    }
    
    final List<RMNode> deactivatedNodes =
        MockNodes.deactivatedNodes(racks, numNodes, newResource(mbsPerNode));
    final ConcurrentMap<NodeId, RMNode> deactivatedNodesMap =
        Maps.newConcurrentMap();
    for (RMNode node : deactivatedNodes) {
      deactivatedNodesMap.put(node.getNodeID(), node);
    }

    RMContextImpl rmContext = new RMContextImpl(null, null, null, null,
        null, null, null, null, null, null) {
       @Override
       public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
         return applicationsMaps;
       }
       @Override
       public ConcurrentMap<NodeId, RMNode> getInactiveRMNodes() {
         return deactivatedNodesMap;
       }
       @Override
       public ConcurrentMap<NodeId, RMNode> getRMNodes() {
         return nodesMap;
       }
     }; 
    rmContext.setNodeLabelManager(new NullRMNodeLabelsManager());
    rmContext.setYarnConfiguration(new YarnConfiguration());
    return rmContext;
  }

  public static ResourceManager mockRm(int apps, int racks, int nodes,
                                       int mbsPerNode) throws IOException {
    RMContext rmContext = mockRMContext(apps, racks, nodes,
      mbsPerNode);
    return mockRm(rmContext);
  }

  public static ResourceManager mockRm(RMContext rmContext) throws IOException {
    return mockRm(rmContext, false);
  }

  public static ResourceManager mockRm(RMContext rmContext,
      boolean useDRC) throws IOException {
    ResourceManager rm = mock(ResourceManager.class);
    ResourceScheduler rs = mockCapacityScheduler(useDRC);
    ApplicationACLsManager aclMgr = mockAppACLsManager();
    ClientRMService clientRMService = mockClientRMService(rmContext);
    when(rm.getResourceScheduler()).thenReturn(rs);
    when(rm.getRMContext()).thenReturn(rmContext);
    when(rm.getApplicationACLsManager()).thenReturn(aclMgr);
    when(rm.getClientRMService()).thenReturn(clientRMService);
    return rm;
  }

  public static CapacityScheduler mockCapacityScheduler() throws IOException {
    return mockCapacityScheduler(false);
  }

  public static CapacityScheduler mockCapacityScheduler(boolean useDRC)
      throws IOException {
    // stolen from TestCapacityScheduler
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf, useDRC);

    CapacityScheduler cs = new CapacityScheduler();
    YarnConfiguration yarnConf = new YarnConfiguration();
    cs.setConf(yarnConf);
    RMContext rmContext = new RMContextImpl(null, null, null, null, null,
        null, new RMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInRM(conf),
        new ClientToAMTokenSecretManagerInRM(), null);
    RMNodeLabelsManager labelManager = new NullRMNodeLabelsManager();
    labelManager.init(yarnConf);
    rmContext.setNodeLabelManager(labelManager);
    cs.setRMContext(rmContext);
    cs.init(conf);
    return cs;
  }

  public static ApplicationACLsManager mockAppACLsManager() {
    Configuration conf = new Configuration();
    return new ApplicationACLsManager(conf);
  }

  public static ClientRMService mockClientRMService(RMContext rmContext) {
    ClientRMService clientRMService = mock(ClientRMService.class);
    List<ApplicationReport> appReports = new ArrayList<ApplicationReport>();
    for (RMApp app : rmContext.getRMApps().values()) {
      ApplicationReport appReport =
          ApplicationReport.newInstance(
              app.getApplicationId(), (ApplicationAttemptId) null,
              app.getUser(), app.getQueue(),
              app.getName(), (String) null, 0, (Token) null,
              app.createApplicationState(),
              app.getDiagnostics().toString(), (String) null,
              app.getStartTime(), app.getLaunchTime(), app.getFinishTime(),
              app.getFinalApplicationStatus(),
              (ApplicationResourceUsageReport) null, app.getTrackingUrl(),
              app.getProgress(), app.getApplicationType(), (Token) null);
      appReports.add(appReport);
    }
    GetApplicationsResponse response = mock(GetApplicationsResponse.class);
    when(response.getApplicationList()).thenReturn(appReports);
    try {
      when(clientRMService.getApplications(any(GetApplicationsRequest.class)))
          .thenReturn(response);
    } catch (YarnException e) {
      Assert.fail("Exception is not expected.");
    }
    return clientRMService;
  }


  static void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    setupQueueConfiguration(conf, false);
  }

  static void setupQueueConfiguration(CapacitySchedulerConfiguration conf,
      boolean useDRC) {
    // Define top-level queues
    QueuePath root = new QueuePath(CapacitySchedulerConfiguration.ROOT);
    conf.setQueues(root, new String[] {"a", "b", "c"});

    final QueuePath a = root.createNewLeaf("a");
    conf.setCapacity(a, 10);

    final QueuePath b = root.createNewLeaf("b");
    conf.setCapacity(b, 20);

    final QueuePath c = root.createNewLeaf("c");
    conf.setCapacity(c, 70);

    // Define 2nd-level queues
    final QueuePath a1 = a.createNewLeaf("a1");
    final QueuePath a2 = a.createNewLeaf("a2");
    conf.setQueues(a, new String[] {"a1", "a2"});
    conf.setCapacity(a1, 30);
    conf.setCapacity(a2, 70);

    final QueuePath b1 = b.createNewLeaf("b1");
    final QueuePath b2 = b.createNewLeaf("b2");
    final QueuePath b3 = b.createNewLeaf("b3");
    conf.setQueues(b, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(b1, 50);
    conf.setCapacity(b2, 30);
    conf.setCapacity(b3, 20);

    final QueuePath c1 = c.createNewLeaf("c1");
    final QueuePath c2 = c.createNewLeaf("c2");
    final QueuePath c3 = c.createNewLeaf("c3");
    final QueuePath c4 = c.createNewLeaf("c4");
    conf.setQueues(c, new String[] {"c1", "c2", "c3", "c4"});
    conf.setCapacity(c1, 50);
    conf.setCapacity(c2, 10);
    conf.setCapacity(c3, 35);
    conf.setCapacity(c4, 5);

    // Define 3rd-level queues
    final QueuePath c11 = c1.createNewLeaf("c11");
    final QueuePath c12 = c1.createNewLeaf("c12");
    final QueuePath c13 = c1.createNewLeaf("c13");
    conf.setQueues(c1, new String[] {"c11", "c12", "c13"});
    conf.setCapacity(c11, 15);
    conf.setCapacity(c12, 45);
    conf.setCapacity(c13, 40);
    if (useDRC) {
      conf.set("yarn.scheduler.capacity.resource-calculator",
          "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator");
    }
  }

  public static ResourceManager mockFifoRm(int apps, int racks, int nodes,
                                       int mbsPerNode)
  throws Exception {
    ResourceManager rm = mock(ResourceManager.class);
    RMContext rmContext = mockRMContext(apps, racks, nodes,
        mbsPerNode);
    ResourceScheduler rs = mockFifoScheduler(rmContext);
    when(rm.getResourceScheduler()).thenReturn(rs);
    when(rm.getRMContext()).thenReturn(rmContext);
    return rm;
  }

  public static FifoScheduler mockFifoScheduler(RMContext rmContext)
      throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupFifoQueueConfiguration(conf);

    FifoScheduler fs = new FifoScheduler();
    fs.setConf(new YarnConfiguration());
    fs.setRMContext(rmContext);
    fs.init(conf);
    return fs;
  }

  static void setupFifoQueueConfiguration(CapacitySchedulerConfiguration conf) {
    // Define default queue
    conf.setQueues(new QueuePath("default"), new String[] {"default"});
    conf.setCapacity(new QueuePath("default"), 100);
  }

  public static void main(String[] args) throws Exception {
    // For manual testing
    WebApps.$for("yarn", new TestRMWebApp()).at(8888).inDevMode().
        start(new RMWebApp(mockRm(2500, 8, 8, 8*GiB))).joinThread();
    WebApps.$for("yarn", new TestRMWebApp()).at(8888).inDevMode().
        start(new RMWebApp(mockFifoRm(10, 1, 4, 8*GiB))).joinThread();
  }
}
