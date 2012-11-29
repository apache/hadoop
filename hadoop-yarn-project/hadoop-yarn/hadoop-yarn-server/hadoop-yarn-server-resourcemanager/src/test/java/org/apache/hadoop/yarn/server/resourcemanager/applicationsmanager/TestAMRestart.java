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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to restart the AM on failure.
 *
 */
public class TestAMRestart {
//  private static final Log LOG = LogFactory.getLog(TestAMRestart.class);
//  ApplicationsManagerImpl appImpl;
//  RMContext asmContext = new RMContextImpl(new MemStore());
//  ApplicationTokenSecretManager appTokenSecretManager =
//    new ApplicationTokenSecretManager();
//  DummyResourceScheduler scheduler;
//  private ClientRMService clientRMService;
//  int count = 0;
//  ApplicationId appID;
//  final int maxFailures = 3;
//  AtomicInteger launchNotify = new AtomicInteger();
//  AtomicInteger schedulerNotify = new AtomicInteger();
//  volatile boolean stop = false;
//  int schedulerAddApplication = 0;
//  int schedulerRemoveApplication = 0;
//  int launcherLaunchCalled = 0;
//  int launcherCleanupCalled = 0;
//  private final static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
//
//  private class ExtApplicationsManagerImpl extends ApplicationsManagerImpl {
//    public ExtApplicationsManagerImpl(
//        ApplicationTokenSecretManager applicationTokenSecretManager,
//        YarnScheduler scheduler, RMContext asmContext) {
//      super(applicationTokenSecretManager, scheduler, asmContext);
//    }
//
//    @Override
//    public EventHandler<ASMEvent<AMLauncherEventType>> createNewApplicationMasterLauncher(
//        ApplicationTokenSecretManager tokenSecretManager) {
//      return new DummyAMLauncher();
//    }
//  }
//
//  private class DummyAMLauncher implements EventHandler<ASMEvent<AMLauncherEventType>> {
//
//    public DummyAMLauncher() {
//      asmContext.getDispatcher().register(AMLauncherEventType.class, this);
//      new Thread() {
//        public void run() {
//          while (!stop) {
//            LOG.info("DEBUG -- waiting for launch");
//            synchronized(launchNotify) {
//              while (launchNotify.get() == 0) {
//                try {
//                  launchNotify.wait();
//                } catch (InterruptedException e) {
//                }
//              }
//              asmContext.getDispatcher().getEventHandler().handle(
//                  new ApplicationEvent(
//                      ApplicationEventType.LAUNCHED, appID));
//              launchNotify.addAndGet(-1);
//            }
//          }
//        }
//      }.start();
//    }
//
//    @Override
//    public void handle(ASMEvent<AMLauncherEventType> event) {
//      switch (event.getType()) {
//      case CLEANUP:
//        launcherCleanupCalled++;
//        break;
//      case LAUNCH:
//        LOG.info("DEBUG -- launching");
//        launcherLaunchCalled++;
//        synchronized (launchNotify) {
//          launchNotify.addAndGet(1);
//          launchNotify.notify();
//        }
//        break;
//      default:
//        break;
//      }
//    }
//  }
//
//  private class DummyResourceScheduler implements ResourceScheduler {
//
//    @Override
//    public void removeNode(RMNode node) {
//    }
//
//    @Override
//    public Allocation allocate(ApplicationId applicationId,
//        List<ResourceRequest> ask, List<Container> release) throws IOException {
//      Container container = recordFactory.newRecordInstance(Container.class);
//      container.setContainerToken(recordFactory.newRecordInstance(ContainerToken.class));
//      container.setNodeId(recordFactory.newRecordInstance(NodeId.class));
//      container.setContainerManagerAddress("localhost");
//      container.setNodeHttpAddress("localhost:8042");
//      container.setId(recordFactory.newRecordInstance(ContainerId.class));
//      container.getId().setAppId(appID);
//      container.getId().setId(count);
//      count++;
//      return new Allocation(Arrays.asList(container), Resources.none());
//    }
//
//    @Override
//    public void handle(ASMEvent<ApplicationTrackerEventType> event) {
//      switch (event.getType()) {
//      case ADD:
//        schedulerAddApplication++;
//        break;
//      case EXPIRE:
//        schedulerRemoveApplication++;
//        LOG.info("REMOVING app : " + schedulerRemoveApplication);
//        if (schedulerRemoveApplication == maxFailures) {
//          synchronized (schedulerNotify) {
//            schedulerNotify.addAndGet(1);
//            schedulerNotify.notify();
//          }
//        }
//        break;
//      default:
//        break;
//      }
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
//    @Override
//    public void addNode(RMNode nodeInfo) {
//    }
//    @Override
//    public void recover(RMState state) throws Exception {
//    }
//    @Override
//    public void reinitialize(Configuration conf,
//        ContainerTokenSecretManager secretManager, RMContext rmContext)
//        throws IOException {
//    }
//
//    @Override
//    public void nodeUpdate(RMNode nodeInfo,
//        Map<String, List<Container>> containers) {
//    }
//
//    @Override
//    public Resource getMaximumResourceCapability() {
//      // TODO Auto-generated method stub
//      return null;
//    }
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
//
//    asmContext.getDispatcher().register(ApplicationEventType.class,
//        new ResourceManager.ApplicationEventDispatcher(asmContext));
//
//    appID = recordFactory.newRecordInstance(ApplicationId.class);
//    appID.setClusterTimestamp(System.currentTimeMillis());
//    appID.setId(1);
//    Configuration conf = new Configuration();
//    scheduler = new DummyResourceScheduler();
//    asmContext.getDispatcher().init(conf);
//    asmContext.getDispatcher().start();
//    asmContext.getDispatcher().register(ApplicationTrackerEventType.class, scheduler);
//    appImpl = new ExtApplicationsManagerImpl(appTokenSecretManager, scheduler, asmContext);
//
//    conf.setLong(YarnConfiguration.AM_EXPIRY_INTERVAL, 1000L);
//    conf.setInt(RMConfig.AM_MAX_RETRIES, maxFailures);
//    appImpl.init(conf);
//    appImpl.start();
//
//    this.clientRMService = new ClientRMService(asmContext, appImpl
//        .getAmLivelinessMonitor(), appImpl.getClientToAMSecretManager(),
//        scheduler);
//    this.clientRMService.init(conf);
//  }
//
//  @After
//  public void tearDown() {
//  }
//
//  private void waitForFailed(AppAttempt application, ApplicationState
//      finalState) throws Exception {
//    int count = 0;
//    while(application.getState() != finalState && count < 10) {
//      Thread.sleep(500);
//      count++;
//    }
//    Assert.assertEquals(finalState, application.getState());
//  }
//
//  @Test
//  public void testAMRestart() throws Exception {
//    ApplicationSubmissionContext subContext = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
//    subContext.setApplicationId(appID);
//    subContext.setApplicationName("dummyApp");
////    subContext.command = new ArrayList<String>();
////    subContext.environment = new HashMap<String, String>();
////    subContext.fsTokens = new ArrayList<String>();
//    subContext.setFsTokensTodo(ByteBuffer.wrap(new byte[0]));
//    SubmitApplicationRequest request = recordFactory
//        .newRecordInstance(SubmitApplicationRequest.class);
//    request.setApplicationSubmissionContext(subContext);
//    clientRMService.submitApplication(request);
//    AppAttempt application = asmContext.getApplications().get(appID);
//    synchronized (schedulerNotify) {
//      while(schedulerNotify.get() == 0) {
//        schedulerNotify.wait();
//      }
//    }
//    Assert.assertEquals(maxFailures, launcherCleanupCalled);
//    Assert.assertEquals(maxFailures, launcherLaunchCalled);
//    Assert.assertEquals(maxFailures, schedulerAddApplication);
//    Assert.assertEquals(maxFailures, schedulerRemoveApplication);
//    Assert.assertEquals(maxFailures, application.getFailedCount());
//    waitForFailed(application, ApplicationState.FAILED);
//    stop = true;
//  }
}
