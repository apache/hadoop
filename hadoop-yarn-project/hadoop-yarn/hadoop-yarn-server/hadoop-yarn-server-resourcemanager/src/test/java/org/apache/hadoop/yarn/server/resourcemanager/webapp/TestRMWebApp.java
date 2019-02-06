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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;
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
    ResourceManager rm = mock(ResourceManager.class);
    ResourceScheduler rs = mockCapacityScheduler();
    ApplicationACLsManager aclMgr = mockAppACLsManager();
    ClientRMService clientRMService = mockClientRMService(rmContext);
    when(rm.getResourceScheduler()).thenReturn(rs);
    when(rm.getRMContext()).thenReturn(rmContext);
    when(rm.getApplicationACLsManager()).thenReturn(aclMgr);
    when(rm.getClientRMService()).thenReturn(clientRMService);
    return rm;
  }

  public static CapacityScheduler mockCapacityScheduler() throws IOException {
    // stolen from TestCapacityScheduler
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);

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
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);

    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);

    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, 30);
    conf.setCapacity(A2, 70);

    final String B1 = B + ".b1";
    final String B2 = B + ".b2";
    final String B3 = B + ".b3";
    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(B1, 50);
    conf.setCapacity(B2, 30);
    conf.setCapacity(B3, 20);

    final String C1 = C + ".c1";
    final String C2 = C + ".c2";
    final String C3 = C + ".c3";
    final String C4 = C + ".c4";
    conf.setQueues(C, new String[] {"c1", "c2", "c3", "c4"});
    conf.setCapacity(C1, 50);
    conf.setCapacity(C2, 10);
    conf.setCapacity(C3, 35);
    conf.setCapacity(C4, 5);

    // Define 3rd-level queues
    final String C11 = C1 + ".c11";
    final String C12 = C1 + ".c12";
    final String C13 = C1 + ".c13";
    conf.setQueues(C1, new String[] {"c11", "c12", "c13"});
    conf.setCapacity(C11, 15);
    conf.setCapacity(C12, 45);
    conf.setCapacity(C13, 40);
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
    conf.setQueues("default", new String[] {"default"});
    conf.setCapacity("default", 100);
  }

  public static void main(String[] args) throws Exception {
    // For manual testing
    WebApps.$for("yarn", new TestRMWebApp()).at(8888).inDevMode().
        start(new RMWebApp(mockRm(2500, 8, 8, 8*GiB))).joinThread();
    WebApps.$for("yarn", new TestRMWebApp()).at(8888).inDevMode().
        start(new RMWebApp(mockFifoRm(10, 1, 4, 8*GiB))).joinThread();
  }
}
