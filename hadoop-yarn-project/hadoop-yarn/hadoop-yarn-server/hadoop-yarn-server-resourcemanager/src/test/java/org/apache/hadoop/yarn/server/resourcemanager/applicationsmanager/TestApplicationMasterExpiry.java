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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.junit.Before;
import org.junit.Test;

/**
 * A test case that tests the expiry of the application master.
 * More tests can be added to this.
 */
public class TestApplicationMasterExpiry {
//  private static final Log LOG = LogFactory.getLog(TestApplicationMasterExpiry.class);
//  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
//
//  private final RMContext context = new RMContextImpl(new MemStore());
//  private AMLivelinessMonitor amLivelinessMonitor;
//
//  @Before
//  public void setUp() {
//    new DummyApplicationTracker();
//    new DummySN();
//    new DummyLauncher();
//    new ApplicationEventTypeListener();
//    Configuration conf = new Configuration();
//    context.getDispatcher().register(ApplicationEventType.class,
//        new ResourceManager.ApplicationEventDispatcher(context));
//    context.getDispatcher().init(conf);
//    context.getDispatcher().start();
//    conf.setLong(YarnConfiguration.AM_EXPIRY_INTERVAL, 1000L);
//    amLivelinessMonitor = new AMLivelinessMonitor(this.context
//        .getDispatcher().getEventHandler());
//    amLivelinessMonitor.init(conf);
//    amLivelinessMonitor.start();
//  }
//
//  private class DummyApplicationTracker implements EventHandler<ASMEvent<ApplicationTrackerEventType>> {
//    DummyApplicationTracker() {
//      context.getDispatcher().register(ApplicationTrackerEventType.class, this);
//    }
//    @Override
//    public void handle(ASMEvent<ApplicationTrackerEventType> event) {
//    }
//  }
//
//  private AtomicInteger expiry = new AtomicInteger();
//  private boolean expired = false;
//
//  private class ApplicationEventTypeListener implements
//      EventHandler<ApplicationEvent> {
//    ApplicationEventTypeListener() {
//      context.getDispatcher().register(ApplicationEventType.class, this);
//    }
//    @Override
//    public void handle(ApplicationEvent event) {
//      switch(event.getType()) {
//      case EXPIRE:
//        expired = true;
//        LOG.info("Received expiry from application " + event.getApplicationId());
//        synchronized(expiry) {
//          expiry.addAndGet(1);
//        }
//      }
//    }
//  }
//
//  private class DummySN implements EventHandler<ASMEvent<SNEventType>> {
//    DummySN() {
//      context.getDispatcher().register(SNEventType.class, this);
//    }
//    @Override
//    public void handle(ASMEvent<SNEventType> event) {
//    }
//  }
//
//  private class DummyLauncher implements EventHandler<ASMEvent<AMLauncherEventType>> {
//    DummyLauncher() {
//      context.getDispatcher().register(AMLauncherEventType.class, this);
//    }
//    @Override
//    public void handle(ASMEvent<AMLauncherEventType> event) {
//    }
//  }
//
//  private void waitForState(AppAttempt application, ApplicationState
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
//  public void testAMExpiry() throws Exception {
//    ApplicationSubmissionContext submissionContext = recordFactory
//        .newRecordInstance(ApplicationSubmissionContext.class);
//    submissionContext.setApplicationId(recordFactory
//        .newRecordInstance(ApplicationId.class));
//    submissionContext.getApplicationId().setClusterTimestamp(
//        System.currentTimeMillis());
//    submissionContext.getApplicationId().setId(1);
//
//    ApplicationStore appStore = context.getApplicationsStore()
//    .createApplicationStore(submissionContext.getApplicationId(),
//        submissionContext);
//    AppAttempt application = new AppAttemptImpl(context,
//        new Configuration(), "dummy", submissionContext, "dummytoken", appStore,
//        amLivelinessMonitor);
//    context.getApplications()
//        .put(application.getApplicationID(), application);
//
//    this.context.getDispatcher().getSyncHandler().handle(
//        new ApplicationEvent(ApplicationEventType.ALLOCATE, submissionContext
//            .getApplicationId()));
//
//    waitForState(application, ApplicationState.ALLOCATING);
//
//    this.context.getDispatcher().getEventHandler().handle(
//        new AMAllocatedEvent(application.getApplicationID(),
//            application.getMasterContainer()));
//
//    waitForState(application, ApplicationState.LAUNCHING);
//
//    this.context.getDispatcher().getEventHandler().handle(
//        new ApplicationEvent(ApplicationEventType.LAUNCHED,
//            application.getApplicationID()));
//    synchronized(expiry) {
//      while (expiry.get() == 0) {
//        expiry.wait(1000);
//      }
//    }
//    Assert.assertTrue(expired);
//  }
}
