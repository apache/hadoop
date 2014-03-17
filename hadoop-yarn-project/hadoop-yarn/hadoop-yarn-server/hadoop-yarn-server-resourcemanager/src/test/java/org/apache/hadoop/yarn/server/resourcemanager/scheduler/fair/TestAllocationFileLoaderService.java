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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;

public class TestAllocationFileLoaderService {
  
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "/tmp")).getAbsolutePath();

  final static String ALLOC_FILE = new File(TEST_DIR,
      "test-queues").getAbsolutePath();
  
  private class MockClock implements Clock {
    private long time = 0;
    @Override
    public long getTime() {
      return time;
    }

    public void tick(long ms) {
      time += ms;
    }
  }
  
  @Test
  public void testGetAllocationFileFromClasspath() {
    Configuration conf = new Configuration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        "test-fair-scheduler.xml");
    AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
    File allocationFile = allocLoader.getAllocationFile(conf);
    assertEquals("test-fair-scheduler.xml", allocationFile.getName());
    assertTrue(allocationFile.exists());
  }
  
  @Test (timeout = 10000)
  public void testReload() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("  <queue name=\"queueA\">");
    out.println("    <maxRunningApps>1</maxRunningApps>");
    out.println("  </queue>");
    out.println("  <queue name=\"queueB\" />");
    out.println("  <queuePlacementPolicy>");
    out.println("    <rule name='default' />");
    out.println("  </queuePlacementPolicy>");
    out.println("</allocations>");
    out.close();
    
    MockClock clock = new MockClock();
    Configuration conf = new Configuration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileLoaderService allocLoader = new AllocationFileLoaderService(
        clock);
    allocLoader.reloadIntervalMs = 5;
    allocLoader.init(conf);
    ReloadListener confHolder = new ReloadListener();
    allocLoader.setReloadListener(confHolder);
    allocLoader.reloadAllocations();
    AllocationConfiguration allocConf = confHolder.allocConf;
    
    // Verify conf
    QueuePlacementPolicy policy = allocConf.getPlacementPolicy();
    List<QueuePlacementRule> rules = policy.getRules();
    assertEquals(1, rules.size());
    assertEquals(QueuePlacementRule.Default.class, rules.get(0).getClass());
    assertEquals(1, allocConf.getQueueMaxApps("root.queueA"));
    assertEquals(2, allocConf.getQueueNames().size());
    assertTrue(allocConf.getQueueNames().contains("root.queueA"));
    assertTrue(allocConf.getQueueNames().contains("root.queueB"));
    
    confHolder.allocConf = null;
    
    // Modify file and advance the clock
    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("  <queue name=\"queueB\">");
    out.println("    <maxRunningApps>3</maxRunningApps>");
    out.println("  </queue>");
    out.println("  <queuePlacementPolicy>");
    out.println("    <rule name='specified' />");
    out.println("    <rule name='default' />");
    out.println("  </queuePlacementPolicy>");
    out.println("</allocations>");
    out.close();
    
    clock.tick(System.currentTimeMillis()
        + AllocationFileLoaderService.ALLOC_RELOAD_WAIT_MS + 10000);
    allocLoader.start();
    
    while (confHolder.allocConf == null) {
      Thread.sleep(20);
    }
    
    // Verify conf
    allocConf = confHolder.allocConf;
    policy = allocConf.getPlacementPolicy();
    rules = policy.getRules();
    assertEquals(2, rules.size());
    assertEquals(QueuePlacementRule.Specified.class, rules.get(0).getClass());
    assertEquals(QueuePlacementRule.Default.class, rules.get(1).getClass());
    assertEquals(3, allocConf.getQueueMaxApps("root.queueB"));
    assertEquals(1, allocConf.getQueueNames().size());
    assertTrue(allocConf.getQueueNames().contains("root.queueB"));
  }
  
  @Test
  public void testAllocationFileParsing() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give queue A a minimum of 1024 M
    out.println("<queue name=\"queueA\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    // Give queue B a minimum of 2048 M
    out.println("<queue name=\"queueB\">");
    out.println("<minResources>2048mb,0vcores</minResources>");
    out.println("<aclAdministerApps>alice,bob admins</aclAdministerApps>");
    out.println("<schedulingPolicy>fair</schedulingPolicy>");
    out.println("</queue>");
    // Give queue C no minimum
    out.println("<queue name=\"queueC\">");
    out.println("<aclSubmitApps>alice,bob admins</aclSubmitApps>");
    out.println("</queue>");
    // Give queue D a limit of 3 running apps
    out.println("<queue name=\"queueD\">");
    out.println("<maxRunningApps>3</maxRunningApps>");
    out.println("</queue>");
    // Give queue E a preemption timeout of one minute
    out.println("<queue name=\"queueE\">");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</queue>");
    // Set default limit of apps per queue to 15
    out.println("<queueMaxAppsDefault>15</queueMaxAppsDefault>");
    // Set default limit of apps per user to 5
    out.println("<userMaxAppsDefault>5</userMaxAppsDefault>");
    // Give user1 a limit of 10 jobs
    out.println("<user name=\"user1\">");
    out.println("<maxRunningApps>10</maxRunningApps>");
    out.println("</user>");
    // Set default min share preemption timeout to 2 minutes
    out.println("<defaultMinSharePreemptionTimeout>120"
        + "</defaultMinSharePreemptionTimeout>");
    // Set fair share preemption timeout to 5 minutes
    out.println("<fairSharePreemptionTimeout>300</fairSharePreemptionTimeout>");
    // Set default scheduling policy to DRF
    out.println("<defaultQueueSchedulingPolicy>drf</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();
    
    allocLoader.init(conf);
    ReloadListener confHolder = new ReloadListener();
    allocLoader.setReloadListener(confHolder);
    allocLoader.reloadAllocations();
    AllocationConfiguration queueConf = confHolder.allocConf;
    
    assertEquals(5, queueConf.getQueueNames().size());
    assertEquals(Resources.createResource(0),
        queueConf.getMinResources("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(Resources.createResource(0),
        queueConf.getMinResources("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));

    assertEquals(Resources.createResource(1024, 0),
        queueConf.getMinResources("root.queueA"));
    assertEquals(Resources.createResource(2048, 0),
        queueConf.getMinResources("root.queueB"));
    assertEquals(Resources.createResource(0),
        queueConf.getMinResources("root.queueC"));
    assertEquals(Resources.createResource(0),
        queueConf.getMinResources("root.queueD"));
    assertEquals(Resources.createResource(0),
        queueConf.getMinResources("root.queueE"));

    assertEquals(15, queueConf.getQueueMaxApps("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(15, queueConf.getQueueMaxApps("root.queueA"));
    assertEquals(15, queueConf.getQueueMaxApps("root.queueB"));
    assertEquals(15, queueConf.getQueueMaxApps("root.queueC"));
    assertEquals(3, queueConf.getQueueMaxApps("root.queueD"));
    assertEquals(15, queueConf.getQueueMaxApps("root.queueE"));
    assertEquals(10, queueConf.getUserMaxApps("user1"));
    assertEquals(5, queueConf.getUserMaxApps("user2"));

    // Root should get * ACL
    assertEquals("*", queueConf.getQueueAcl("root",
        QueueACL.ADMINISTER_QUEUE).getAclString());
    assertEquals("*", queueConf.getQueueAcl("root",
        QueueACL.SUBMIT_APPLICATIONS).getAclString());

    // Unspecified queues should get default ACL
    assertEquals(" ", queueConf.getQueueAcl("root.queueA",
        QueueACL.ADMINISTER_QUEUE).getAclString());
    assertEquals(" ", queueConf.getQueueAcl("root.queueA",
        QueueACL.SUBMIT_APPLICATIONS).getAclString());

    // Queue B ACL
    assertEquals("alice,bob admins", queueConf.getQueueAcl("root.queueB",
        QueueACL.ADMINISTER_QUEUE).getAclString());

    // Queue C ACL
    assertEquals("alice,bob admins", queueConf.getQueueAcl("root.queueC",
        QueueACL.SUBMIT_APPLICATIONS).getAclString());

    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root." + 
        YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root.queueA"));
    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root.queueB"));
    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root.queueC"));
    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root.queueD"));
    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root.queueA"));
    assertEquals(60000, queueConf.getMinSharePreemptionTimeout("root.queueE"));
    assertEquals(300000, queueConf.getFairSharePreemptionTimeout());
    
    // Verify existing queues have default scheduling policy
    assertEquals(DominantResourceFairnessPolicy.NAME,
        queueConf.getSchedulingPolicy("root").getName());
    assertEquals(DominantResourceFairnessPolicy.NAME,
        queueConf.getSchedulingPolicy("root.queueA").getName());
    // Verify default is overriden if specified explicitly
    assertEquals(FairSharePolicy.NAME,
        queueConf.getSchedulingPolicy("root.queueB").getName());
    // Verify new queue gets default scheduling policy
    assertEquals(DominantResourceFairnessPolicy.NAME,
        queueConf.getSchedulingPolicy("root.newqueue").getName());
  }
  
  @Test
  public void testBackwardsCompatibleAllocationFileParsing() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give queue A a minimum of 1024 M
    out.println("<pool name=\"queueA\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</pool>");
    // Give queue B a minimum of 2048 M
    out.println("<pool name=\"queueB\">");
    out.println("<minResources>2048mb,0vcores</minResources>");
    out.println("<aclAdministerApps>alice,bob admins</aclAdministerApps>");
    out.println("</pool>");
    // Give queue C no minimum
    out.println("<pool name=\"queueC\">");
    out.println("<aclSubmitApps>alice,bob admins</aclSubmitApps>");
    out.println("</pool>");
    // Give queue D a limit of 3 running apps
    out.println("<pool name=\"queueD\">");
    out.println("<maxRunningApps>3</maxRunningApps>");
    out.println("</pool>");
    // Give queue E a preemption timeout of one minute
    out.println("<pool name=\"queueE\">");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    // Set default limit of apps per queue to 15
    out.println("<queueMaxAppsDefault>15</queueMaxAppsDefault>");
    // Set default limit of apps per user to 5
    out.println("<userMaxAppsDefault>5</userMaxAppsDefault>");
    // Give user1 a limit of 10 jobs
    out.println("<user name=\"user1\">");
    out.println("<maxRunningApps>10</maxRunningApps>");
    out.println("</user>");
    // Set default min share preemption timeout to 2 minutes
    out.println("<defaultMinSharePreemptionTimeout>120"
        + "</defaultMinSharePreemptionTimeout>");
    // Set fair share preemption timeout to 5 minutes
    out.println("<fairSharePreemptionTimeout>300</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    
    allocLoader.init(conf);
    ReloadListener confHolder = new ReloadListener();
    allocLoader.setReloadListener(confHolder);
    allocLoader.reloadAllocations();
    AllocationConfiguration queueConf = confHolder.allocConf;

    assertEquals(5, queueConf.getQueueNames().size());
    assertEquals(Resources.createResource(0),
        queueConf.getMinResources("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(Resources.createResource(0),
        queueConf.getMinResources("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));

    assertEquals(Resources.createResource(1024, 0),
        queueConf.getMinResources("root.queueA"));
    assertEquals(Resources.createResource(2048, 0),
        queueConf.getMinResources("root.queueB"));
    assertEquals(Resources.createResource(0),
        queueConf.getMinResources("root.queueC"));
    assertEquals(Resources.createResource(0),
        queueConf.getMinResources("root.queueD"));
    assertEquals(Resources.createResource(0),
        queueConf.getMinResources("root.queueE"));

    assertEquals(15, queueConf.getQueueMaxApps("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(15, queueConf.getQueueMaxApps("root.queueA"));
    assertEquals(15, queueConf.getQueueMaxApps("root.queueB"));
    assertEquals(15, queueConf.getQueueMaxApps("root.queueC"));
    assertEquals(3, queueConf.getQueueMaxApps("root.queueD"));
    assertEquals(15, queueConf.getQueueMaxApps("root.queueE"));
    assertEquals(10, queueConf.getUserMaxApps("user1"));
    assertEquals(5, queueConf.getUserMaxApps("user2"));

    // Unspecified queues should get default ACL
    assertEquals(" ", queueConf.getQueueAcl("root.queueA",
        QueueACL.ADMINISTER_QUEUE).getAclString());
    assertEquals(" ", queueConf.getQueueAcl("root.queueA",
        QueueACL.SUBMIT_APPLICATIONS).getAclString());

    // Queue B ACL
    assertEquals("alice,bob admins", queueConf.getQueueAcl("root.queueB",
        QueueACL.ADMINISTER_QUEUE).getAclString());

    // Queue C ACL
    assertEquals("alice,bob admins", queueConf.getQueueAcl("root.queueC",
        QueueACL.SUBMIT_APPLICATIONS).getAclString());


    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root." +
        YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root.queueA"));
    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root.queueB"));
    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root.queueC"));
    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root.queueD"));
    assertEquals(120000, queueConf.getMinSharePreemptionTimeout("root.queueA"));
    assertEquals(60000, queueConf.getMinSharePreemptionTimeout("root.queueE"));
    assertEquals(300000, queueConf.getFairSharePreemptionTimeout());
  }
  
  @Test
  public void testSimplePlacementPolicyFromConf() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.setBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS, false);
    conf.setBoolean(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, false);
    
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("</allocations>");
    out.close();
    
    AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
    allocLoader.init(conf);
    ReloadListener confHolder = new ReloadListener();
    allocLoader.setReloadListener(confHolder);
    allocLoader.reloadAllocations();
    AllocationConfiguration allocConf = confHolder.allocConf;
    
    QueuePlacementPolicy placementPolicy = allocConf.getPlacementPolicy();
    List<QueuePlacementRule> rules = placementPolicy.getRules();
    assertEquals(2, rules.size());
    assertEquals(QueuePlacementRule.Specified.class, rules.get(0).getClass());
    assertEquals(false, rules.get(0).create);
    assertEquals(QueuePlacementRule.Default.class, rules.get(1).getClass());
  }
  
  /**
   * Verify that you can't place queues at the same level as the root queue in
   * the allocations file.
   */
  @Test (expected = AllocationConfigurationException.class)
  public void testQueueAlongsideRoot() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("</queue>");
    out.println("<queue name=\"other\">");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();
    
    AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
    allocLoader.init(conf);
    ReloadListener confHolder = new ReloadListener();
    allocLoader.setReloadListener(confHolder);
    allocLoader.reloadAllocations();
  }
  
  private class ReloadListener implements AllocationFileLoaderService.Listener {
    public AllocationConfiguration allocConf;
    
    @Override
    public void onReload(AllocationConfiguration info) {
      allocConf = info;
    }
  }
}
