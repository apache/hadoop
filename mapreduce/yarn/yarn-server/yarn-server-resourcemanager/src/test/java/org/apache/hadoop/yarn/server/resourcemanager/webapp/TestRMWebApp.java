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

import com.google.inject.Injector;

import java.util.List;

import static org.apache.hadoop.test.MockitoMaker.*;
import static org.apache.hadoop.yarn.server.resourcemanager.MockNodes.*;
import static org.apache.hadoop.yarn.webapp.Params.*;

import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationsManager;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.MockAsm;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.ClusterTracker;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestRMWebApp {
  static final int GiB = 1024; // MiB

  @Test public void testControllerIndex() {
    Injector injector = WebAppTests.createMockInjector(this);
    RmController c = injector.getInstance(RmController.class);
    c.index();
    assertEquals("Applications", c.get(TITLE, "unknown"));
  }

  @Test public void testView() {
    Injector injector = WebAppTests.createMockInjector(ApplicationsManager.class,
                                                       MockAsm.create(3));
    injector.getInstance(RmView.class).render();
    WebAppTests.flushOutput(injector);
  }

  @Test public void testNodesPage() {
    WebAppTests.testPage(NodesPage.class, ClusterTracker.class,
                         mockResource(1, 2, 8*GiB));
  }

  public static RMResourceTrackerImpl mockResource(int racks, int nodes,
                                                   int mbsPerNode) {
    final List<NodeInfo> list =
        MockNodes.newNodes(racks, nodes, newResource(mbsPerNode));
    return make(stub(RMResourceTrackerImpl.class).returning(list).from.
        getAllNodeInfo());
  }

  public static ResourceManager mockRm(int apps, int racks, int nodes,
                                       int mbsPerNode)
  throws Exception {
    ResourceManager rm = mock(ResourceManager.class);
    ApplicationsManager asm = MockAsm.create(apps);
    RMResourceTrackerImpl rt = mockResource(racks, nodes, mbsPerNode);
    ResourceScheduler rs = mockCapacityScheduler();
    when(rm.getApplicationsManager()).thenReturn(asm);
    when(rm.getResourceTracker()).thenReturn(rt);
    when(rm.getResourceScheduler()).thenReturn(rs);
    return rm;
  }

  public static CapacityScheduler mockCapacityScheduler() throws Exception {
    // stolen from TestCapacityScheduler
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);

    CapacityScheduler cs = new CapacityScheduler();
    cs.reinitialize(conf, null, null);
    return cs;
  }

  static void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(CapacityScheduler.ROOT, new String[] {"a", "b", "c"});
    conf.setCapacity(CapacityScheduler.ROOT, 100);

    final String A = CapacityScheduler.ROOT + ".a";
    conf.setCapacity(A, 10);

    final String B = CapacityScheduler.ROOT + ".b";
    conf.setCapacity(B, 20);

    final String C = CapacityScheduler.ROOT + ".c";
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

  public static void main(String[] args) throws Exception {
    // For manual testing
    WebApps.$for("yarn", new TestRMWebApp()).at(8888).inDevMode().
        start(new RMWebApp(mockRm(88, 8, 8, 8*GiB))).joinThread();
  }
}
