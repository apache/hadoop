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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing the applications manager launcher.
 *
 */
public class TestApplicationMasterLauncher {
//  private static final Log LOG = LogFactory.getLog(TestApplicationMasterLauncher.class);
//  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
//  private ApplicationMasterLauncher amLauncher;
//  private DummyEventHandler asmHandle;
//  private final ApplicationTokenSecretManager applicationTokenSecretManager =
//    new ApplicationTokenSecretManager();
//  private final ClientToAMSecretManager clientToAMSecretManager = 
//    new ClientToAMSecretManager();
//
//  Object doneLaunching = new Object();
//  AtomicInteger launched = new AtomicInteger();
//  AtomicInteger cleanedUp = new AtomicInteger();
//  private RMContext context = new RMContextImpl(new MemStore(), null, null,
//      null);
//
//  private Configuration conf = new Configuration();
//  
//  private class DummyEventHandler implements EventHandler<ApplicationEvent> {
//    @Override
//    public void handle(ApplicationEvent appEvent) {
//      ApplicationEventType event = appEvent.getType();
//      switch (event) {
//      case FINISH:
//        synchronized(doneLaunching) {
//          doneLaunching.notify();
//        }
//        break;
//
//      default:
//        break;
//      }
//    }
//  }
//
//  private class DummyLaunch implements Runnable {
//    public void run() {
//      launched.incrementAndGet();
//    }
//  }
//
//  private class DummyCleanUp implements Runnable {
//    private EventHandler eventHandler;
//    
//    public DummyCleanUp(EventHandler eventHandler) {
//      this.eventHandler = eventHandler;
//    }
//    public void run() {
//      cleanedUp.incrementAndGet();
//      eventHandler.handle(new AMFinishEvent(null,
//          ApplicationState.COMPLETED, "", ""));
//    }
//  }
//
//  private class DummyApplicationMasterLauncher extends
//      ApplicationMasterLauncher {
//    private EventHandler eventHandler;
//
//    public DummyApplicationMasterLauncher(
//        ApplicationTokenSecretManager applicationTokenSecretManager,
//        ClientToAMSecretManager clientToAMSecretManager,
//        EventHandler eventHandler) {
//      super(applicationTokenSecretManager, clientToAMSecretManager, context);
//      this.eventHandler = eventHandler;
//    }
//
//    @Override
//    protected Runnable createRunnableLauncher(RMAppAttempt application,
//        AMLauncherEventType event) {
//      Runnable r = null;
//      switch (event) {
//      case LAUNCH:
//        r = new DummyLaunch();
//        break;
//      case CLEANUP:
//        r = new DummyCleanUp(eventHandler);
//      default:
//        break;
//      }
//      return r;
//    }
//  }
//
//  @Before
//  public void setUp() {
//    asmHandle = new DummyEventHandler();
//    amLauncher = new DummyApplicationMasterLauncher(applicationTokenSecretManager,
//        clientToAMSecretManager, asmHandle);
//    context.getDispatcher().init(conf);
//    amLauncher.init(conf);
//    context.getDispatcher().start();
//    amLauncher.start();
//    
//  }
//
//  @After
//  public void tearDown() {
//    amLauncher.stop();
//  }
//
//  @Test
//  public void testAMLauncher() throws Exception {
//
//    // Creat AppId
//    ApplicationId appId = recordFactory
//        .newRecordInstance(ApplicationId.class);
//    appId.setClusterTimestamp(System.currentTimeMillis());
//    appId.setId(1);
//
//    ApplicationAttemptId appAttemptId = Records
//        .newRecord(ApplicationAttemptId.class);
//    appAttemptId.setApplicationId(appId);
//    appAttemptId.setAttemptId(1);
//
//    // Create submissionContext
//    ApplicationSubmissionContext submissionContext = recordFactory
//        .newRecordInstance(ApplicationSubmissionContext.class);
//    submissionContext.setApplicationId(appId);
//    submissionContext.setUser("dummyuser");
//
//    RMAppAttempt appAttempt = new RMAppAttemptImpl(appAttemptId,
//        "dummyclienttoken", context, null, submissionContext);
//
//    // Tell AMLauncher to launch the appAttempt
//    amLauncher.handle(new AMLauncherEvent(AMLauncherEventType.LAUNCH,
//        appAttempt));
//
//    // Tell AMLauncher to cleanup the appAttempt
//    amLauncher.handle(new AMLauncherEvent(AMLauncherEventType.CLEANUP,
//        appAttempt));
//
//    synchronized (doneLaunching) {
//      doneLaunching.wait(10000);
//    }
//    Assert.assertEquals(1, launched.get());
//    Assert.assertEquals(1, cleanedUp.get());
//  }
}