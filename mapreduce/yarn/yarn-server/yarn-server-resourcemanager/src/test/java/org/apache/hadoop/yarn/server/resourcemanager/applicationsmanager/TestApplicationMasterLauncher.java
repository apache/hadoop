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

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationFinishEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing the applications manager launcher.
 *
 */
public class TestApplicationMasterLauncher extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestApplicationMasterLauncher.class);
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private ApplicationMasterLauncher amLauncher;
  private DummyASM asmHandle;
  private final ApplicationTokenSecretManager applicationTokenSecretManager =
    new ApplicationTokenSecretManager();
  private final ClientToAMSecretManager clientToAMSecretManager = 
    new ClientToAMSecretManager();

  Object doneLaunching = new Object();
  AtomicInteger launched = new AtomicInteger();
  AtomicInteger cleanedUp = new AtomicInteger();
  private RMContext context = new ResourceManager.RMContextImpl(new MemStore());
  
  private class DummyASM implements EventHandler<ASMEvent<ApplicationEventType>> {
    @Override
    public void handle(ASMEvent<ApplicationEventType> appEvent) {
      ApplicationEventType event = appEvent.getType();
      switch (event) {
      case FINISH:
        synchronized(doneLaunching) {
          doneLaunching.notify();
        }
        break;

      default:
        break;
      }
    }
  }

  private class DummyLaunch implements Runnable {
    public void run() {
      launched.incrementAndGet();
    }
  }

  private class DummyCleanUp implements Runnable {
    private EventHandler asmHandle;
    
    public DummyCleanUp(EventHandler asmHandle) {
      this.asmHandle = asmHandle;
    }
    public void run() {
      cleanedUp.incrementAndGet();
      asmHandle.handle(new ApplicationFinishEvent(null,
          ApplicationState.COMPLETED));
    }
  }

  private  class DummyApplicationMasterLauncher extends ApplicationMasterLauncher {
    private EventHandler asmHandle;
    
    public DummyApplicationMasterLauncher(ApplicationTokenSecretManager 
        applicationTokenSecretManager, ClientToAMSecretManager clientToAMSecretManager, 
        EventHandler handler) {
      super(applicationTokenSecretManager, clientToAMSecretManager, context);
      this.asmHandle = handler;
    }

    @Override
    protected Runnable createRunnableLauncher(AppContext masterInfo, 
        AMLauncherEventType event) {
      Runnable r = null;
      switch (event) {
      case LAUNCH:
        r = new DummyLaunch();
        break;
      case CLEANUP:
        r = new DummyCleanUp(asmHandle);
      default:
        break;
      }
      return r;
    }
  }

  @Before
  public void setUp() {
    asmHandle = new DummyASM();
    amLauncher = new DummyApplicationMasterLauncher(applicationTokenSecretManager,
        clientToAMSecretManager, asmHandle);
    Configuration conf = new Configuration();
    context.getDispatcher().init(conf);
    amLauncher.init(conf);
    context.getDispatcher().start();
    amLauncher.start();
    
  }

  @After
  public void tearDown() {
    amLauncher.stop();
  }

  @Test
  public void testAMLauncher() throws Exception {
    ApplicationSubmissionContext context = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    context.setApplicationId(recordFactory.newRecordInstance(ApplicationId.class));
    context.getApplicationId().setClusterTimestamp(System.currentTimeMillis());
    context.getApplicationId().setId(1);
    context.setUser("dummyuser");
    ApplicationMasterInfo masterInfo = new ApplicationMasterInfo(this.context,
        "dummyuser", context,
        "dummyclienttoken", StoreFactory.createVoidAppStore());
    amLauncher.handle(new ASMEvent<AMLauncherEventType>(AMLauncherEventType.LAUNCH, 
      masterInfo));
    amLauncher.handle(new ASMEvent<AMLauncherEventType>(AMLauncherEventType.CLEANUP,  
      masterInfo));
    synchronized (doneLaunching) {
      doneLaunching.wait(10000);
    }
    assertTrue(launched.get() == 1);
    assertTrue(cleanedUp.get() == 1);
  }
}