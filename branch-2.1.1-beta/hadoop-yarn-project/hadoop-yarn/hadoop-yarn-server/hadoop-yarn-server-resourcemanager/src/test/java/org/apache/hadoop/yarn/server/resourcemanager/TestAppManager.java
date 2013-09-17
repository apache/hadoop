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

package org.apache.hadoop.yarn.server.resourcemanager;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Testing applications being retired from RM.
 *
 */

public class TestAppManager{
  private static RMAppEventType appEventType = RMAppEventType.KILL; 

  public synchronized RMAppEventType getAppEventType() {
    return appEventType;
  } 
  public synchronized void setAppEventType(RMAppEventType newType) {
    appEventType = newType;
  } 


  public static List<RMApp> newRMApps(int n, long time, RMAppState state) {
    List<RMApp> list = Lists.newArrayList();
    for (int i = 0; i < n; ++i) {
      list.add(new MockRMApp(i, time, state));
    }
    return list;
  }

  public static RMContext mockRMContext(int n, long time) {
    final List<RMApp> apps = newRMApps(n, time, RMAppState.FINISHED);
    final ConcurrentMap<ApplicationId, RMApp> map = Maps.newConcurrentMap();
    for (RMApp app : apps) {
      map.put(app.getApplicationId(), app);
    }
    Dispatcher rmDispatcher = new AsyncDispatcher();
    ContainerAllocationExpirer containerAllocationExpirer = new ContainerAllocationExpirer(
        rmDispatcher);
    AMLivelinessMonitor amLivelinessMonitor = new AMLivelinessMonitor(
        rmDispatcher);
    AMLivelinessMonitor amFinishingMonitor = new AMLivelinessMonitor(
        rmDispatcher);
    return new RMContextImpl(rmDispatcher,
        containerAllocationExpirer, amLivelinessMonitor, amFinishingMonitor,
        null, null, null, null, null) {
      @Override
      public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
        return map;
      }
    };
  }

  public class TestAppManagerDispatcher implements
      EventHandler<RMAppManagerEvent> {


    public TestAppManagerDispatcher() {
    }

    @Override
    public void handle(RMAppManagerEvent event) {
       // do nothing
    }   
  }   

  public class TestDispatcher implements
      EventHandler<RMAppEvent> {

    public TestDispatcher() {
    }

    @Override
    public void handle(RMAppEvent event) {
      //RMApp rmApp = this.rmContext.getRMApps().get(appID);
      setAppEventType(event.getType());
      System.out.println("in handle routine " + getAppEventType().toString());
    }   
  }   
  

  // Extend and make the functions we want to test public
  public class TestRMAppManager extends RMAppManager {

    public TestRMAppManager(RMContext context, Configuration conf) {
      super(context, null, null, new ApplicationACLsManager(conf), conf);
      setCompletedAppsMax(YarnConfiguration.DEFAULT_RM_MAX_COMPLETED_APPLICATIONS);
    }

    public TestRMAppManager(RMContext context,
        ClientToAMTokenSecretManagerInRM clientToAMSecretManager,
        YarnScheduler scheduler, ApplicationMasterService masterService,
        ApplicationACLsManager applicationACLsManager, Configuration conf) {
      super(context, scheduler, masterService, applicationACLsManager, conf);
      setCompletedAppsMax(YarnConfiguration.DEFAULT_RM_MAX_COMPLETED_APPLICATIONS);
    }

    public void checkAppNumCompletedLimit() {
      super.checkAppNumCompletedLimit();
    }

    public void finishApplication(ApplicationId appId) {
      super.finishApplication(appId);
    }

    public int getCompletedAppsListSize() {
      return super.getCompletedAppsListSize();
    }

    public void setCompletedAppsMax(int max) {
      super.setCompletedAppsMax(max);
    }
    public void submitApplication(
        ApplicationSubmissionContext submissionContext, String user)
            throws YarnException {
      super.submitApplication(submissionContext, System.currentTimeMillis(),
          false, user);
    }
  }

  protected void addToCompletedApps(TestRMAppManager appMonitor, RMContext rmContext) {
    for (RMApp app : rmContext.getRMApps().values()) {
      if (app.getState() == RMAppState.FINISHED
          || app.getState() == RMAppState.KILLED 
          || app.getState() == RMAppState.FAILED) {
        appMonitor.finishApplication(app.getApplicationId());
      }
    }
  }

  private RMContext rmContext;
  private TestRMAppManager appMonitor;
  private ApplicationSubmissionContext asContext;
  private ApplicationId appId;

  @Before
  public void setUp() {
    long now = System.currentTimeMillis();

    rmContext = mockRMContext(1, now - 10);
    ResourceScheduler scheduler = mockResourceScheduler();
    Configuration conf = new Configuration();
    ApplicationMasterService masterService =
        new ApplicationMasterService(rmContext, scheduler);
    appMonitor = new TestRMAppManager(rmContext,
        new ClientToAMTokenSecretManagerInRM(), scheduler, masterService,
        new ApplicationACLsManager(conf), conf);

    appId = MockApps.newAppID(1);
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    asContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    asContext.setApplicationId(appId);
    asContext.setAMContainerSpec(mockContainerLaunchContext(recordFactory));
    asContext.setResource(mockResource());
    setupDispatcher(rmContext, conf);
  }

  @After
  public void tearDown() {
    setAppEventType(RMAppEventType.KILL);
    ((Service)rmContext.getDispatcher()).stop();
  }

  @Test
  public void testRMAppRetireNone() throws Exception {
    long now = System.currentTimeMillis();

    // Create such that none of the applications will retire since
    // haven't hit max #
    RMContext rmContext = mockRMContext(10, now - 10);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, new Configuration());

    appMonitor.setCompletedAppsMax(10);

    Assert.assertEquals("Number of apps incorrect before checkAppTimeLimit",
        10, rmContext.getRMApps().size());

    // add them to completed apps list
    addToCompletedApps(appMonitor, rmContext);

    // shouldn't  have to many apps
    appMonitor.checkAppNumCompletedLimit();
    Assert.assertEquals("Number of apps incorrect after # completed check", 10,
        rmContext.getRMApps().size());
    Assert.assertEquals("Number of completed apps incorrect after check", 10,
        appMonitor.getCompletedAppsListSize());
  }

  @Test
  public void testRMAppRetireSome() throws Exception {
    long now = System.currentTimeMillis();

    RMContext rmContext = mockRMContext(10, now - 20000);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, new Configuration());

    appMonitor.setCompletedAppsMax(3);

    Assert.assertEquals("Number of apps incorrect before", 10, rmContext
        .getRMApps().size());

    // add them to completed apps list
    addToCompletedApps(appMonitor, rmContext);

    // shouldn't  have to many apps
    appMonitor.checkAppNumCompletedLimit();
    Assert.assertEquals("Number of apps incorrect after # completed check", 3,
        rmContext.getRMApps().size());
    Assert.assertEquals("Number of completed apps incorrect after check", 3,
        appMonitor.getCompletedAppsListSize());
  }

  @Test
  public void testRMAppRetireSomeDifferentStates() throws Exception {
    long now = System.currentTimeMillis();

    // these parameters don't matter, override applications below
    RMContext rmContext = mockRMContext(10, now - 20000);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, new Configuration());

    appMonitor.setCompletedAppsMax(2);

    // clear out applications map
    rmContext.getRMApps().clear();
    Assert.assertEquals("map isn't empty", 0, rmContext.getRMApps().size());

    // / set with various finished states
    RMApp app = new MockRMApp(0, now - 20000, RMAppState.KILLED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(1, now - 200000, RMAppState.FAILED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(2, now - 30000, RMAppState.FINISHED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(3, now - 20000, RMAppState.RUNNING);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(4, now - 20000, RMAppState.NEW);
    rmContext.getRMApps().put(app.getApplicationId(), app);

    // make sure it doesn't expire these since still running
    app = new MockRMApp(5, now - 10001, RMAppState.KILLED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(6, now - 30000, RMAppState.ACCEPTED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(7, now - 20000, RMAppState.SUBMITTED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(8, now - 10001, RMAppState.FAILED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(9, now - 20000, RMAppState.FAILED);
    rmContext.getRMApps().put(app.getApplicationId(), app);

    Assert.assertEquals("Number of apps incorrect before", 10, rmContext
        .getRMApps().size());

    // add them to completed apps list
    addToCompletedApps(appMonitor, rmContext);

    // shouldn't  have to many apps
    appMonitor.checkAppNumCompletedLimit();
    Assert.assertEquals("Number of apps incorrect after # completed check", 6,
        rmContext.getRMApps().size());
    Assert.assertEquals("Number of completed apps incorrect after check", 2,
        appMonitor.getCompletedAppsListSize());

  }

  @Test
  public void testRMAppRetireNullApp() throws Exception {
    long now = System.currentTimeMillis();

    RMContext rmContext = mockRMContext(10, now - 20000);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, new Configuration());

    Assert.assertEquals("Number of apps incorrect before", 10, rmContext
        .getRMApps().size());

    appMonitor.finishApplication(null);

    Assert.assertEquals("Number of completed apps incorrect after check", 0,
        appMonitor.getCompletedAppsListSize());
  }

  @Test
  public void testRMAppRetireZeroSetting() throws Exception {
    long now = System.currentTimeMillis();

    RMContext rmContext = mockRMContext(10, now - 20000);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, new Configuration());

    Assert.assertEquals("Number of apps incorrect before", 10, rmContext
        .getRMApps().size());

    // test with 0
    appMonitor.setCompletedAppsMax(0);

    addToCompletedApps(appMonitor, rmContext);
    Assert.assertEquals("Number of completed apps incorrect", 10,
        appMonitor.getCompletedAppsListSize());

    appMonitor.checkAppNumCompletedLimit();

    Assert.assertEquals("Number of apps incorrect after # completed check", 0,
        rmContext.getRMApps().size());
    Assert.assertEquals("Number of completed apps incorrect after check", 0,
        appMonitor.getCompletedAppsListSize());
  }

  protected void setupDispatcher(RMContext rmContext, Configuration conf) {
    TestDispatcher testDispatcher = new TestDispatcher();
    TestAppManagerDispatcher testAppManagerDispatcher = 
        new TestAppManagerDispatcher();
    rmContext.getDispatcher().register(RMAppEventType.class, testDispatcher);
    rmContext.getDispatcher().register(RMAppManagerEventType.class, testAppManagerDispatcher);
    ((Service)rmContext.getDispatcher()).init(conf);
    ((Service)rmContext.getDispatcher()).start();
    Assert.assertEquals("app event type is wrong before", RMAppEventType.KILL, appEventType);
  }

  @Test
  public void testRMAppSubmit() throws Exception {
    appMonitor.submitApplication(asContext, "test");
    RMApp app = rmContext.getRMApps().get(appId);
    Assert.assertNotNull("app is null", app);
    Assert.assertEquals("app id doesn't match", appId, app.getApplicationId());
    Assert.assertEquals("app state doesn't match", RMAppState.NEW, app.getState());

    // wait for event to be processed
    int timeoutSecs = 0;
    while ((getAppEventType() == RMAppEventType.KILL) && 
        timeoutSecs++ < 20) {
      Thread.sleep(1000);
    }
    Assert.assertEquals("app event type sent is wrong", RMAppEventType.START,
        getAppEventType());
  }

  @Test (timeout = 30000)
  public void testRMAppSubmitMaxAppAttempts() throws Exception {
    int[] globalMaxAppAttempts = new int[] { 10, 1 };
    int[][] individualMaxAppAttempts = new int[][]{
        new int[]{ 9, 10, 11, 0 },
        new int[]{ 1, 10, 0, -1 }};
    int[][] expectedNums = new int[][]{
        new int[]{ 9, 10, 10, 10 },
        new int[]{ 1, 1, 1, 1 }};
    for (int i = 0; i < globalMaxAppAttempts.length; ++i) {
      for (int j = 0; j < individualMaxAppAttempts.length; ++j) {
        ResourceScheduler scheduler = mockResourceScheduler();
        Configuration conf = new Configuration();
        conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, globalMaxAppAttempts[i]);
        ApplicationMasterService masterService =
            new ApplicationMasterService(rmContext, scheduler);
        TestRMAppManager appMonitor = new TestRMAppManager(rmContext,
            new ClientToAMTokenSecretManagerInRM(), scheduler, masterService,
            new ApplicationACLsManager(conf), conf);

        ApplicationId appID = MockApps.newAppID(i * 4 + j + 1);
        asContext.setApplicationId(appID);
        if (individualMaxAppAttempts[i][j] != 0) {
          asContext.setMaxAppAttempts(individualMaxAppAttempts[i][j]);
        }
        appMonitor.submitApplication(asContext, "test");
        RMApp app = rmContext.getRMApps().get(appID);
        Assert.assertEquals("max application attempts doesn't match",
            expectedNums[i][j], app.getMaxAppAttempts());

        // wait for event to be processed
        int timeoutSecs = 0;
        while ((getAppEventType() == RMAppEventType.KILL) &&
            timeoutSecs++ < 20) {
          Thread.sleep(1000);
        }
        setAppEventType(RMAppEventType.KILL);
      }
    }
  }

  @Test (timeout = 30000)
  public void testRMAppSubmitDuplicateApplicationId() throws Exception {
    ApplicationId appId = MockApps.newAppID(0);
    asContext.setApplicationId(appId);
    RMApp appOrig = rmContext.getRMApps().get(appId);
    Assert.assertTrue("app name matches but shouldn't", "testApp1" != appOrig.getName());

    // our testApp1 should be rejected and original app with same id should be left in place
    try {
      appMonitor.submitApplication(asContext, "test");
      Assert.fail("Exception is expected when applicationId is duplicate.");
    } catch (YarnException e) {
      Assert.assertTrue("The thrown exception is not the expectd one.",
          e.getMessage().contains("Cannot add a duplicate!"));
    }

    // make sure original app didn't get removed
    RMApp app = rmContext.getRMApps().get(appId);
    Assert.assertNotNull("app is null", app);
    Assert.assertEquals("app id doesn't match", appId, app.getApplicationId());
    Assert.assertEquals("app state doesn't match", RMAppState.FINISHED, app.getState());
  }

  @Test (timeout = 30000)
  public void testRMAppSubmitInvalidResourceRequest() throws Exception {
    asContext.setResource(Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB + 1));

    // submit an app
    try {
      appMonitor.submitApplication(asContext, "test");
      Assert.fail("Application submission should fail because resource" +
          " request is invalid.");
    } catch (YarnException e) {
      // Exception is expected
      // TODO Change this to assert the expected exception type - post YARN-142
      // sub-task related to specialized exceptions.
      Assert.assertTrue("The thrown exception is not" +
          " InvalidResourceRequestException",
          e.getMessage().contains("Invalid resource request"));
    }
  }

  private static ResourceScheduler mockResourceScheduler() {
    ResourceScheduler scheduler = mock(ResourceScheduler.class);
    when(scheduler.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
    when(scheduler.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
    return scheduler;
  }

  private static ContainerLaunchContext mockContainerLaunchContext(
      RecordFactory recordFactory) {
    ContainerLaunchContext amContainer = recordFactory.newRecordInstance(
        ContainerLaunchContext.class);
    amContainer.setApplicationACLs(new HashMap<ApplicationAccessType, String>());;
    return amContainer;
  }

  private static Resource mockResource() {
    return Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
  }

}
