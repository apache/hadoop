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

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationFinishEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationTrackerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.SNEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestASMStateMachine extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestASMStateMachine.class);
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  RMContext context = new ResourceManager.RMContextImpl(new MemStore());
  EventHandler handler;
  private boolean snreceivedCleanUp = false;
  private boolean snAllocateReceived = false;
  private boolean launchCalled = false;
  private boolean addedApplication = false;
  private boolean removedApplication = false;
  private boolean launchCleanupCalled = false;
  private AtomicInteger waitForState = new AtomicInteger();

  @Before
  public void setUp() {
    context.getDispatcher().init(new Configuration());
    context.getDispatcher().start();
    handler = context.getDispatcher().getEventHandler();
    new DummyAMLaunchEventHandler();
    new DummySNEventHandler();
    new ApplicationTracker();
    new MockAppplicationMasterInfo();
  }

  @After
  public void tearDown() {

  }

  private class DummyAMLaunchEventHandler implements EventHandler<ASMEvent<AMLauncherEventType>> {
    AppContext appcontext;
    AtomicInteger amsync = new AtomicInteger(0);

    public DummyAMLaunchEventHandler() {
      context.getDispatcher().register(AMLauncherEventType.class, this);
    }

    @Override
    public void handle(ASMEvent<AMLauncherEventType> event) {
      switch(event.getType()) {
      case LAUNCH:
        launchCalled = true;
        appcontext = event.getAppContext();
        context.getDispatcher().getEventHandler().handle(
            new ASMEvent<ApplicationEventType>(ApplicationEventType.LAUNCHED,
                appcontext));
        break;
      case CLEANUP:
        launchCleanupCalled = true;
        break;
      }
    }
  }

  private class DummySNEventHandler implements EventHandler<ASMEvent<SNEventType>> {
    AppContext appContext;
    AtomicInteger snsync = new AtomicInteger(0);

    public DummySNEventHandler() {
      context.getDispatcher().register(SNEventType.class, this);
    }

    @Override
    public void handle(ASMEvent<SNEventType> event) {
      switch(event.getType()) {
      case CLEANUP:
        snreceivedCleanUp = true;
        break;
      case SCHEDULE:
        snAllocateReceived = true;
        appContext = event.getAppContext();
        context.getDispatcher().getEventHandler().handle(
            new ASMEvent<ApplicationEventType>(ApplicationEventType.ALLOCATED,
                appContext));
        break;
      }
    }

  }

  private static class StatusContext implements AppContext {
    @Override
    public ApplicationSubmissionContext getSubmissionContext() {
      return null;
    }
    @Override
    public Resource getResource() {
      return null;
    }
    @Override
    public ApplicationId getApplicationID() {
      return null;
    }
    @Override
    public ApplicationStatus getStatus() {
      ApplicationStatus status = recordFactory.newRecordInstance(ApplicationStatus.class);
      status.setLastSeen(-99);
      return status;
    }
    @Override
    public ApplicationMaster getMaster() {
      return null;
    }
    @Override
    public Container getMasterContainer() {
      return null;
    }
    @Override
    public String getUser() {
      return null;
    }
    @Override
    public long getLastSeen() {
      return 0;
    }
    @Override
    public String getName() {
      return null;
    }
    @Override
    public String getQueue() {
      return null;
    }
    @Override
    public int getFailedCount() {
      return 0;
    }
    @Override
    public ApplicationStore getStore() {
      return StoreFactory.createVoidAppStore();
    }
  }

  private class ApplicationTracker implements EventHandler<ASMEvent<ApplicationTrackerEventType>> {
    public ApplicationTracker() {
      context.getDispatcher().register(ApplicationTrackerEventType.class, this);
    }

    @Override
    public void handle(ASMEvent<ApplicationTrackerEventType> event) {
      switch (event.getType()) {
      case ADD:
        addedApplication = true;
        break;
      case REMOVE:
        removedApplication = true;
        break;
      }
    }
  }

  private class MockAppplicationMasterInfo implements EventHandler<ASMEvent<ApplicationEventType>> {

    MockAppplicationMasterInfo() {
      context.getDispatcher().register(ApplicationEventType.class, this);
    }
    @Override
    public void handle(ASMEvent<ApplicationEventType> event) {
      LOG.info("The event type is " + event.getType());
    }
  }

  private void waitForState( ApplicationState 
      finalState, ApplicationMasterInfo masterInfo) throws Exception {
    int count = 0;
    while(masterInfo.getState() != finalState && count < 10) {
      Thread.sleep(500);
      count++;
    }
    assertTrue(masterInfo.getState() == finalState);
  } 
  
  /* Test the state machine. 
   * 
   */
  @Test
  public void testStateMachine() throws Exception {
    ApplicationSubmissionContext submissioncontext = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    submissioncontext.setApplicationId(recordFactory.newRecordInstance(ApplicationId.class));
    submissioncontext.getApplicationId().setId(1);
    submissioncontext.getApplicationId().setClusterTimestamp(System.currentTimeMillis());

    ApplicationMasterInfo masterInfo 
    = new ApplicationMasterInfo(context, "dummyuser", submissioncontext, "dummyToken"
        , StoreFactory.createVoidAppStore());

    context.getDispatcher().register(ApplicationEventType.class, masterInfo);
    handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.
        ALLOCATE, masterInfo));

    waitForState(ApplicationState.ALLOCATED, masterInfo);
    handler.handle(new ASMEvent<ApplicationEventType>(
        ApplicationEventType.LAUNCH, masterInfo));

    waitForState(ApplicationState.LAUNCHED, masterInfo);
    Assert.assertTrue(snAllocateReceived);
    Assert.assertTrue(launchCalled);
    Assert.assertTrue(addedApplication);
    handler.handle(new ASMEvent<ApplicationEventType>(
        ApplicationEventType.REGISTERED, masterInfo));
    waitForState(ApplicationState.RUNNING, masterInfo);
    Assert.assertEquals(ApplicationState.RUNNING, masterInfo.getState());
    handler.handle(new ASMEvent<ApplicationEventType>(
        ApplicationEventType.STATUSUPDATE, new StatusContext()));

    /* check if the state is still RUNNING */

    Assert.assertEquals(ApplicationState.RUNNING, masterInfo.getState());

    handler.handle(new ApplicationFinishEvent(masterInfo,
        ApplicationState.COMPLETED));
    waitForState(ApplicationState.COMPLETED, masterInfo);
    Assert.assertEquals(ApplicationState.COMPLETED, masterInfo.getState());
    /* check if clean up is called for everyone */
    Assert.assertTrue(launchCleanupCalled);
    Assert.assertTrue(snreceivedCleanUp);
    Assert.assertTrue(removedApplication);

    /* check if expiry doesnt make it failed */
    handler.handle(
        new ASMEvent<ApplicationEventType>(ApplicationEventType.EXPIRE, masterInfo));
    Assert.assertEquals(ApplicationState.COMPLETED, masterInfo.getState());   
  }
}
