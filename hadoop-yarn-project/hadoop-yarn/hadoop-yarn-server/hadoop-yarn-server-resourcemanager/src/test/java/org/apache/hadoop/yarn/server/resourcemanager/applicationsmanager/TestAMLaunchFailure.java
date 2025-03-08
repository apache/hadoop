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

/*
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
*/

/* a test case that tests the launch failure of a AM */
public class TestAMLaunchFailure {
//  private static final Logger LOG =
//      LoggerFactory.getLogger(TestAMLaunchFailure.class);
//  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
//  ApplicationsManagerImpl asmImpl;
//  YarnScheduler scheduler = new DummyYarnScheduler();
//  ApplicationTokenSecretManager applicationTokenSecretManager =
//    new ApplicationTokenSecretManager();
//  private ClientRMService clientService;
//
//  private RMContext context;
//
//  private static class DummyYarnScheduler implements YarnScheduler {
//    private Container container = recordFactory.newRecordInstance(Container.class);
//
//    @Override
//    public Allocation allocate(ApplicationId applicationId,
//        List<ResourceRequest> ask, List<Container> release) throws IOException {
//      return new Allocation(Arrays.asList(container), Resources.none());
//    }
//
//    @Override
//    public QueueInfo getQueueInfo(String queueName,
//        boolean includeChildQueues,
//        boolean recursive) throws IOException {
//      return null;
//    }
//
//    @Override
//    public List<QueueUserACLInfo> getQueueUserAclInfo() {
//      return null;
//    }
//
//    @Override
//    public void addApplicationIfAbsent(ApplicationId applicationId,
//        ApplicationMaster master, String user, String queue, Priority priority
//        , ApplicationStore appStore)
//        throws IOException {
//      // TODO Auto-generated method stub
//
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
//  private class DummyApplicationTracker implements EventHandler<ASMEvent<ApplicationTrackerEventType>> {
//    public DummyApplicationTracker() {
//      context.getDispatcher().register(ApplicationTrackerEventType.class, this);
//    }
//    @Override
//    public void handle(ASMEvent<ApplicationTrackerEventType> event) {
//    }
//  }
//
//  public class ExtApplicationsManagerImpl extends ApplicationsManagerImpl {
//
//    private  class DummyApplicationMasterLauncher implements EventHandler<ASMEvent<AMLauncherEventType>> {
//      private AtomicInteger notify = new AtomicInteger();
//      private AppAttempt app;
//
//      public DummyApplicationMasterLauncher(RMContext context) {
//        context.getDispatcher().register(AMLauncherEventType.class, this);
//        new TestThread().start();
//      }
//      @Override
//      public void handle(ASMEvent<AMLauncherEventType> appEvent) {
//        switch(appEvent.getType()) {
//        case LAUNCH:
//          LOG.info("LAUNCH called ");
//          app = appEvent.getApplication();
//          synchronized (notify) {
//            notify.addAndGet(1);
//            notify.notify();
//          }
//          break;
//        }
//      }
//
//      private class TestThread extends Thread {
//        public void run() {
//          synchronized(notify) {
//            try {
//              while (notify.get() == 0) {
//                notify.wait();
//              }
//            } catch (InterruptedException e) {
//              e.printStackTrace();
//            }
//            context.getDispatcher().getEventHandler().handle(
//                new ApplicationEvent(ApplicationEventType.LAUNCHED,
//                    app.getApplicationID()));
//          }
//        }
//      }
//    }
//
//    public ExtApplicationsManagerImpl(
//        ApplicationTokenSecretManager applicationTokenSecretManager,
//        YarnScheduler scheduler) {
//      super(applicationTokenSecretManager, scheduler, context);
//    }
//
//    @Override
//    protected EventHandler<ASMEvent<AMLauncherEventType>> createNewApplicationMasterLauncher(
//        ApplicationTokenSecretManager tokenSecretManager) {
//      return new DummyApplicationMasterLauncher(context);
//    }
//  }
//
//
//  @Before
//  public void setUp() {
//    context = new RMContextImpl(new MemStore());
//    Configuration conf = new Configuration();
//
//    context.getDispatcher().register(ApplicationEventType.class,
//        new ResourceManager.ApplicationEventDispatcher(context));
//
//    context.getDispatcher().init(conf);
//    context.getDispatcher().start();
//
//    asmImpl = new ExtApplicationsManagerImpl(applicationTokenSecretManager, scheduler);
//    clientService = new ClientRMService(context, asmImpl
//        .getAmLivelinessMonitor(), asmImpl.getClientToAMSecretManager(),
//        scheduler);
//    clientService.init(conf);
//    new DummyApplicationTracker();
//    conf.setLong(YarnConfiguration.AM_EXPIRY_INTERVAL, 3000L);
//    conf.setInt(RMConfig.AM_MAX_RETRIES, 1);
//    asmImpl.init(conf);
//    asmImpl.start();
//  }
//
//  @After
//  public void tearDown() {
//    asmImpl.stop();
//  }
//
//  private ApplicationSubmissionContext createDummyAppContext(ApplicationId appID) {
//    ApplicationSubmissionContext context = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
//    context.setApplicationId(appID);
//    return context;
//  }
//
//  @Test
//  public void testAMLaunchFailure() throws Exception {
//    ApplicationId appID = clientService.getNewApplicationId();
//    ApplicationSubmissionContext submissionContext = createDummyAppContext(appID);
//    SubmitApplicationRequest request = recordFactory
//        .newRecordInstance(SubmitApplicationRequest.class);
//    request.setApplicationSubmissionContext(submissionContext);
//    clientService.submitApplication(request);
//    AppAttempt application = context.getApplications().get(appID);
//
//    while (application.getState() != ApplicationState.FAILED) {
//      LOG.info("Waiting for application to go to FAILED state."
//          + " Current state is " + application.getState());
//      Thread.sleep(200);
//      application = context.getApplications().get(appID);
//    }
//    Assert.assertEquals(ApplicationState.FAILED, application.getState());
//  }
}
