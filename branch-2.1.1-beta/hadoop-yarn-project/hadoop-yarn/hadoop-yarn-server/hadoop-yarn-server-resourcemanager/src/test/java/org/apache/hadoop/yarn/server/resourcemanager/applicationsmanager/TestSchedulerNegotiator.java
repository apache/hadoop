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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;


public class TestSchedulerNegotiator {
//  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
//  private SchedulerNegotiator schedulerNegotiator;
//  private DummyScheduler scheduler;
//  private final int testNum = 99999;
//
//  private final RMContext context = new RMContextImpl(new MemStore());
//  AppAttemptImpl masterInfo;
//  private EventHandler handler;
//  private Configuration conf = new Configuration();
//  private class DummyScheduler implements ResourceScheduler {
//    @Override
//    public Allocation allocate(ApplicationId applicationId,
//        List<ResourceRequest> ask, List<Container> release) throws IOException {
//      ArrayList<Container> containers = new ArrayList<Container>();
//      Container container = recordFactory.newRecordInstance(Container.class);
//      container.setId(recordFactory.newRecordInstance(ContainerId.class));
//      container.getId().setAppId(applicationId);
//      container.getId().setId(testNum);
//      containers.add(container);
//      return new Allocation(containers, Resources.none());
//    }
//
//
//    @Override
//    public void nodeUpdate(RMNode nodeInfo,
//        Map<String, List<Container>> containers) {
//    }
//
//    @Override
//    public void removeNode(RMNode node) {
//    }
//
//    @Override
//    public void handle(ASMEvent<ApplicationTrackerEventType> event) {
//    }
//
//    @Override
//    public QueueInfo getQueueInfo(String queueName,
//        boolean includeChildQueues,
//        boolean recursive) throws IOException {
//      return null;
//    }
//    @Override
//    public List<QueueUserACLInfo> getQueueUserAclInfo() {
//      return null;
//    }
//    @Override
//    public void addApplication(ApplicationId applicationId,
//        ApplicationMaster master, String user, String queue, Priority priority,
//        ApplicationStore store)
//        throws IOException {
//    }
//
//
//    @Override
//    public void addNode(RMNode nodeInfo) {
//    }
//
//
//    @Override
//    public void recover(RMState state) throws Exception {
//    }
//
//
//    @Override
//    public void reinitialize(Configuration conf,
//        ContainerTokenSecretManager secretManager, RMContext rmContext)
//        throws IOException {
//    }
//
//
//    @Override
//    public Resource getMaximumResourceCapability() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//
//    @Override
//    public Resource getMinimumResourceCapability() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//  }
//
//  @Before
//  public void setUp() {
//    scheduler = new DummyScheduler();
//    schedulerNegotiator = new SchedulerNegotiator(context, scheduler);
//    schedulerNegotiator.init(conf);
//    schedulerNegotiator.start();
//    handler = context.getDispatcher().getEventHandler();
//    context.getDispatcher().init(conf);
//    context.getDispatcher().start();
//  }
//
//  @After
//  public void tearDown() {
//    schedulerNegotiator.stop();
//  }
//
//  public void waitForState(ApplicationState state, AppAttemptImpl info) {
//    int count = 0;
//    while (info.getState() != state && count < 100) {
//      try {
//        Thread.sleep(50);
//      } catch (InterruptedException e) {
//       e.printStackTrace();
//      }
//      count++;
//    }
//    Assert.assertEquals(state, info.getState());
//  }
//
//  private class DummyEventHandler implements EventHandler<ASMEvent<AMLauncherEventType>> {
//    @Override
//    public void handle(ASMEvent<AMLauncherEventType> event) {
//    }
//  }
//
//  @Test
//  public void testSchedulerNegotiator() throws Exception {
//    ApplicationSubmissionContext submissionContext = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
//    submissionContext.setApplicationId(recordFactory.newRecordInstance(ApplicationId.class));
//    submissionContext.getApplicationId().setClusterTimestamp(System.currentTimeMillis());
//    submissionContext.getApplicationId().setId(1);
//
//    masterInfo = new AppAttemptImpl(this.context, this.conf, "dummy",
//        submissionContext, "dummyClientToken", StoreFactory
//            .createVoidAppStore(), new AMLivelinessMonitor(context
//            .getDispatcher().getEventHandler()));
//    context.getDispatcher().register(ApplicationEventType.class, masterInfo);
//    context.getDispatcher().register(ApplicationTrackerEventType.class, scheduler);
//    context.getDispatcher().register(AMLauncherEventType.class,
//        new DummyEventHandler());
//    handler.handle(new ApplicationEvent(
//        ApplicationEventType.ALLOCATE, submissionContext.getApplicationId()));
//    waitForState(ApplicationState.LAUNCHING, masterInfo); // LAUNCHING because ALLOCATED automatically movesto LAUNCHING for now.
//    Container container = masterInfo.getMasterContainer();
//    Assert.assertTrue(container.getId().getId() == testNum);
//  }
}
