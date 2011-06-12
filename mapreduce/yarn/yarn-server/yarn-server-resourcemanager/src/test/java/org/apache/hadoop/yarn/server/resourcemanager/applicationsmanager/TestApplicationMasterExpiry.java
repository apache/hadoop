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
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationTrackerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.SNEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A test case that tests the expiry of the application master.
 * More tests can be added to this. 
 */
public class TestApplicationMasterExpiry extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestApplicationMasterExpiry.class);
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  AMTracker tracker;
  
  private final RMContext context = new ResourceManager.RMContextImpl(new MemStore());
  
  @Before
  public void setUp() {
    new DummyApplicationTracker();
    new DummySN();
    new DummyLauncher();
    new ApplicationEventTypeListener();
    tracker = new AMTracker(context); 
    Configuration conf = new Configuration();
    context.getDispatcher().init(conf);
    context.getDispatcher().start();
    conf.setLong(YarnConfiguration.AM_EXPIRY_INTERVAL, 1000L);
    tracker.init(conf);
    tracker.start();
  }
  
  @After 
  public void tearDown() {
    tracker.stop();
  }
  
  private class DummyApplicationTracker implements EventHandler<ASMEvent<ApplicationTrackerEventType>> {
    DummyApplicationTracker() {
      context.getDispatcher().register(ApplicationTrackerEventType.class, this);
    }
    @Override
    public void handle(ASMEvent<ApplicationTrackerEventType> event) {
    }
  }
  
  private AtomicInteger expiry = new AtomicInteger();
  private boolean expired = false;
  
  private class ApplicationEventTypeListener implements EventHandler<ASMEvent<ApplicationEventType>> {
    ApplicationEventTypeListener() {
      context.getDispatcher().register(ApplicationEventType.class, this);
    }
    @Override
    public void handle(ASMEvent<ApplicationEventType> event) {
      switch(event.getType()) {
      case EXPIRE:
        expired = true;
        LOG.info("Received expiry from application " + event.getAppContext().getApplicationID());
        synchronized(expiry) {
          expiry.addAndGet(1);
        }
      }
    }
  }
 
  private class DummySN implements EventHandler<ASMEvent<SNEventType>> {
    DummySN() {
      context.getDispatcher().register(SNEventType.class, this);
    }
    @Override
    public void handle(ASMEvent<SNEventType> event) {
    }
  }
  
  private class DummyLauncher implements EventHandler<ASMEvent<AMLauncherEventType>> {
    DummyLauncher() {
      context.getDispatcher().register(AMLauncherEventType.class, this);
    }
    @Override
    public void handle(ASMEvent<AMLauncherEventType> event) {
    }
  }
  
  private void waitForState(ApplicationMasterInfo masterInfo, ApplicationState 
      finalState) throws Exception {
    int count = 0;
    while(masterInfo.getState() != finalState && count < 10) {
      Thread.sleep(500);
      count++;
    }
    assertTrue(masterInfo.getState() == finalState);
  }

  @Test
  public void testAMExpiry() throws Exception {
    ApplicationSubmissionContext context = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    context.setApplicationId(recordFactory.newRecordInstance(ApplicationId.class));
    context.getApplicationId().setClusterTimestamp(System.currentTimeMillis());
    context.getApplicationId().setId(1);
    
    tracker.addMaster(
        "dummy", 
        context, "dummytoken");
    ApplicationMasterInfo masterInfo = tracker.get(context.getApplicationId());
    tracker.runApplication(context.getApplicationId());
    this.context.getDispatcher().getEventHandler().handle(
        new ASMEvent<ApplicationEventType>(ApplicationEventType.ALLOCATED, masterInfo));
    waitForState(masterInfo, ApplicationState.LAUNCHING);
    this.context.getDispatcher().getEventHandler().handle(
    new ASMEvent<ApplicationEventType>(ApplicationEventType.LAUNCHED, masterInfo));
    synchronized(expiry) {
      while (expiry.get() == 0) {
        expiry.wait(1000);
      }
    }
    Assert.assertTrue(expired);
  }
}
