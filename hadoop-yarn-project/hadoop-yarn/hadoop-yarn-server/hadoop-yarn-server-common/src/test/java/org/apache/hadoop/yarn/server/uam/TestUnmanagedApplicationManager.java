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

package org.apache.hadoop.yarn.server.uam;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.AMHeartbeatRequestHandler;
import org.apache.hadoop.yarn.server.AMRMClientRelayer;
import org.apache.hadoop.yarn.server.MockResourceManagerFacade;
import org.apache.hadoop.yarn.util.AsyncCallback;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for UnmanagedApplicationManager.
 */
public class TestUnmanagedApplicationManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestUnmanagedApplicationManager.class);

  private TestableUnmanagedApplicationManager uam;
  private Configuration conf = new YarnConfiguration();
  private CountingCallback callback;

  private ApplicationAttemptId attemptId;

  private UnmanagedAMPoolManager uamPool;
  private ExecutorService threadpool;

  @Before
  public void setup() {
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "subclusterId");
    callback = new CountingCallback();

    attemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1);

    uam = new TestableUnmanagedApplicationManager(conf,
        attemptId.getApplicationId(), null, "submitter", "appNameSuffix", true,
        "rm", null);

    threadpool = Executors.newCachedThreadPool();
    uamPool = new TestableUnmanagedAMPoolManager(this.threadpool);
    uamPool.init(conf);
    uamPool.start();
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    if (uam != null) {
      uam.shutDownConnections();
      uam = null;
    }
    if (uamPool != null) {
      if (uamPool.isInState(Service.STATE.STARTED)) {
        uamPool.stop();
      }
      uamPool = null;
    }
    if (threadpool != null) {
      threadpool.shutdownNow();
      threadpool = null;
    }
  }

  protected void waitForCallBackCountAndCheckZeroPending(
      CountingCallback callBack, int expectCallBackCount) {
    synchronized (callBack) {
      while (callBack.callBackCount != expectCallBackCount) {
        try {
          callBack.wait();
        } catch (InterruptedException e) {
        }
      }
      Assert.assertEquals(
          "Non zero pending requests when number of allocate callbacks reaches "
              + expectCallBackCount,
          0, callBack.requestQueueSize);
    }
  }

  @Test(timeout = 10000)
  public void testBasicUsage()
      throws YarnException, IOException, InterruptedException {

    launchUAM(attemptId);
    registerApplicationMaster(
        RegisterApplicationMasterRequest.newInstance(null, 0, null), attemptId);

    allocateAsync(AllocateRequest.newInstance(0, 0, null, null, null), callback,
        attemptId);

    // Wait for outstanding async allocate callback
    waitForCallBackCountAndCheckZeroPending(callback, 1);

    finishApplicationMaster(
        FinishApplicationMasterRequest.newInstance(null, null, null),
        attemptId);

    while (uam.isHeartbeatThreadAlive()) {
      LOG.info("waiting for heartbeat thread to finish");
      Thread.sleep(100);
    }
  }

  /*
   * Test re-attaching of an existing UAM. This is for HA of UAM client.
   */
  @Test(timeout = 5000)
  public void testUAMReAttach()
      throws YarnException, IOException, InterruptedException {

    launchUAM(attemptId);
    registerApplicationMaster(
        RegisterApplicationMasterRequest.newInstance(null, 0, null), attemptId);

    allocateAsync(AllocateRequest.newInstance(0, 0, null, null, null), callback,
        attemptId);
    // Wait for outstanding async allocate callback
    waitForCallBackCountAndCheckZeroPending(callback, 1);

    MockResourceManagerFacade rmProxy = uam.getRMProxy();
    uam = new TestableUnmanagedApplicationManager(conf,
        attemptId.getApplicationId(), null, "submitter", "appNameSuffix", true,
        "rm");
    uam.setRMProxy(rmProxy);

    reAttachUAM(null, attemptId);
    registerApplicationMaster(
        RegisterApplicationMasterRequest.newInstance(null, 0, null), attemptId);

    allocateAsync(AllocateRequest.newInstance(0, 0, null, null, null), callback,
        attemptId);

    // Wait for outstanding async allocate callback
    waitForCallBackCountAndCheckZeroPending(callback, 2);

    finishApplicationMaster(
        FinishApplicationMasterRequest.newInstance(null, null, null),
        attemptId);
  }

  @Test(timeout = 5000)
  public void testReRegister()
      throws YarnException, IOException, InterruptedException {

    launchUAM(attemptId);
    registerApplicationMaster(
        RegisterApplicationMasterRequest.newInstance(null, 0, null), attemptId);

    uam.setShouldReRegisterNext();

    allocateAsync(AllocateRequest.newInstance(0, 0, null, null, null), callback,
        attemptId);

    // Wait for outstanding async allocate callback
    waitForCallBackCountAndCheckZeroPending(callback, 1);

    uam.setShouldReRegisterNext();

    finishApplicationMaster(
        FinishApplicationMasterRequest.newInstance(null, null, null),
        attemptId);
  }

  /**
   * If register is slow, async allocate requests in the meanwhile should not
   * throw or be dropped.
   */
  @Test(timeout = 5000)
  public void testSlowRegisterCall()
      throws YarnException, IOException, InterruptedException {

    // Register with wait() in RM in a separate thread
    Thread registerAMThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          launchUAM(attemptId);
          registerApplicationMaster(
              RegisterApplicationMasterRequest.newInstance(null, 1001, null),
              attemptId);
        } catch (Exception e) {
          LOG.info("Register thread exception", e);
        }
      }
    });

    // Sync obj from mock RM
    Object syncObj = MockResourceManagerFacade.getRegisterSyncObj();

    // Wait for register call in the thread get into RM and then wake us
    synchronized (syncObj) {
      LOG.info("Starting register thread");
      registerAMThread.start();
      try {
        LOG.info("Test main starts waiting");
        syncObj.wait();
        LOG.info("Test main wait finished");
      } catch (Exception e) {
        LOG.info("Test main wait interrupted", e);
      }
    }

    // First allocate before register succeeds
    allocateAsync(AllocateRequest.newInstance(0, 0, null, null, null), callback,
        attemptId);

    // Notify the register thread
    synchronized (syncObj) {
      syncObj.notifyAll();
    }

    LOG.info("Test main wait for register thread to finish");
    registerAMThread.join();
    LOG.info("Register thread finished");

    // Second allocate, normal case
    allocateAsync(AllocateRequest.newInstance(0, 0, null, null, null), callback,
        attemptId);

    // Both allocate before should respond
    waitForCallBackCountAndCheckZeroPending(callback, 2);

    finishApplicationMaster(
        FinishApplicationMasterRequest.newInstance(null, null, null),
        attemptId);

    // Allocates after finishAM should be ignored
    allocateAsync(AllocateRequest.newInstance(0, 0, null, null, null), callback,
        attemptId);
    allocateAsync(AllocateRequest.newInstance(0, 0, null, null, null), callback,
        attemptId);

    Assert.assertEquals(0, callback.requestQueueSize);

    // A short wait just in case the allocates get executed
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }

    Assert.assertEquals(2, callback.callBackCount);
  }

  @Test(expected = Exception.class)
  public void testAllocateWithoutRegister()
      throws YarnException, IOException, InterruptedException {
    allocateAsync(AllocateRequest.newInstance(0, 0, null, null, null), callback,
        attemptId);
  }

  @Test(expected = Exception.class)
  public void testFinishWithoutRegister()
      throws YarnException, IOException, InterruptedException {
    finishApplicationMaster(
        FinishApplicationMasterRequest.newInstance(null, null, null),
        attemptId);
  }

  @Test(timeout = 10000)
  public void testForceKill()
      throws YarnException, IOException, InterruptedException {
    launchUAM(attemptId);
    registerApplicationMaster(
        RegisterApplicationMasterRequest.newInstance(null, 0, null), attemptId);
    uam.forceKillApplication();

    while (uam.isHeartbeatThreadAlive()) {
      LOG.info("waiting for heartbeat thread to finish");
      Thread.sleep(100);
    }

    try {
      uam.forceKillApplication();
      Assert.fail("Should fail because application is already killed");
    } catch (YarnException t) {
    }
  }

  @Test(timeout = 10000)
  public void testShutDownConnections()
      throws YarnException, IOException, InterruptedException {
    launchUAM(attemptId);
    registerApplicationMaster(
        RegisterApplicationMasterRequest.newInstance(null, 0, null), attemptId);
    uam.shutDownConnections();
    while (uam.isHeartbeatThreadAlive()) {
      LOG.info("waiting for heartbeat thread to finish");
      Thread.sleep(100);
    }
  }

  protected UserGroupInformation getUGIWithToken(
      ApplicationAttemptId appAttemptId) {
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(appAttemptId.toString());
    AMRMTokenIdentifier token = new AMRMTokenIdentifier(appAttemptId, 1);
    ugi.addTokenIdentifier(token);
    return ugi;
  }

  protected Token<AMRMTokenIdentifier> launchUAM(
      ApplicationAttemptId appAttemptId)
      throws IOException, InterruptedException {
    return getUGIWithToken(appAttemptId)
        .doAs((PrivilegedExceptionAction<Token<AMRMTokenIdentifier>>) () -> uam.launchUAM());
  }

  protected void reAttachUAM(final Token<AMRMTokenIdentifier> uamToken,
      ApplicationAttemptId appAttemptId)
      throws IOException, InterruptedException {
    getUGIWithToken(appAttemptId).doAs((PrivilegedExceptionAction<Object>) () -> {
      uam.reAttachUAM(uamToken);
      return null;
    });
  }

  protected RegisterApplicationMasterResponse registerApplicationMaster(
      final RegisterApplicationMasterRequest request,
      ApplicationAttemptId appAttemptId)
      throws YarnException, IOException, InterruptedException {
    return getUGIWithToken(appAttemptId).doAs(
        (PrivilegedExceptionAction<RegisterApplicationMasterResponse>)
        () -> uam.registerApplicationMaster(request));
  }

  protected void allocateAsync(final AllocateRequest request,
      final AsyncCallback<AllocateResponse> callBack, ApplicationAttemptId appAttemptId)
      throws YarnException, IOException, InterruptedException {
    getUGIWithToken(appAttemptId).doAs((PrivilegedExceptionAction<Object>) () -> {
      uam.allocateAsync(request, callBack);
      return null;
    });
  }

  protected FinishApplicationMasterResponse finishApplicationMaster(
      final FinishApplicationMasterRequest request,
      ApplicationAttemptId appAttemptId)
      throws YarnException, IOException, InterruptedException {
    return getUGIWithToken(appAttemptId).doAs(
        (PrivilegedExceptionAction<FinishApplicationMasterResponse>) () ->
        uam.finishApplicationMaster(request));
  }

  protected class CountingCallback implements AsyncCallback<AllocateResponse> {
    private int callBackCount;
    private int requestQueueSize;

    @Override
    public void callback(AllocateResponse response) {
      synchronized (this) {
        callBackCount++;
        requestQueueSize = uam.getRequestQueueSize();
        this.notifyAll();
      }
    }
  }

  /**
   * Testable UnmanagedApplicationManager that talks to a mock RM.
   */
  public class TestableUnmanagedApplicationManager
      extends UnmanagedApplicationManager {

    private MockResourceManagerFacade rmProxy;

    public TestableUnmanagedApplicationManager(Configuration conf,
        ApplicationId appId, String queueName, String submitter,
        String appNameSuffix, boolean keepContainersAcrossApplicationAttempts,
        String rmName) {
      this(conf, appId, queueName, submitter, appNameSuffix,
          keepContainersAcrossApplicationAttempts, rmName, null);
    }

    public TestableUnmanagedApplicationManager(Configuration conf,
        ApplicationId appId, String queueName, String submitter,
        String appNameSuffix, boolean keepContainersAcrossApplicationAttempts,
        String rmName, ApplicationSubmissionContext originalApplicationSubmissionContext) {
      super(conf, appId, queueName, submitter, appNameSuffix,
          keepContainersAcrossApplicationAttempts, rmName, originalApplicationSubmissionContext);
    }

    @Override
    protected AMHeartbeatRequestHandler createAMHeartbeatRequestHandler(
        Configuration config, ApplicationId appId,
        AMRMClientRelayer rmProxyRelayer) {
      return new TestableAMRequestHandlerThread(config, appId, rmProxyRelayer);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T> T createRMProxy(final Class<T> protocol, Configuration config,
        UserGroupInformation user, Token<AMRMTokenIdentifier> token) {
      if (rmProxy == null) {
        rmProxy = new MockResourceManagerFacade(config, 0);
      }
      return (T) rmProxy;
    }

    public void setShouldReRegisterNext() {
      if (rmProxy != null) {
        rmProxy.setShouldReRegisterNext();
      }
    }

    public MockResourceManagerFacade getRMProxy() {
      return rmProxy;
    }

    public void setRMProxy(MockResourceManagerFacade proxy) {
      this.rmProxy = proxy;
    }
  }

  /**
   * Wrap the handler thread so it calls from the same user.
   */
  public class TestableAMRequestHandlerThread
      extends AMHeartbeatRequestHandler {
    public TestableAMRequestHandlerThread(Configuration conf,
        ApplicationId applicationId, AMRMClientRelayer rmProxyRelayer) {
      super(conf, applicationId, rmProxyRelayer);
    }

    @Override
    public void run() {
      try {
        getUGIWithToken(attemptId).doAs((PrivilegedExceptionAction<Object>) () -> {
          TestableAMRequestHandlerThread.super.run();
          return null;
        });
      } catch (Exception e) {
        LOG.error("Exception running TestableAMRequestHandlerThread", e);
      }
    }
  }

  protected class TestableUnmanagedAMPoolManager extends UnmanagedAMPoolManager {
    public TestableUnmanagedAMPoolManager(ExecutorService threadpool) {
      super(threadpool);
    }

    @Override
    public UnmanagedApplicationManager createUAM(Configuration configuration,
        ApplicationId appId, String queueName, String submitter, String appNameSuffix,
        boolean keepContainersAcrossApplicationAttempts, String rmId,
        ApplicationSubmissionContext originalAppSubmissionContext) {
      return new TestableUnmanagedApplicationManager(configuration, appId, queueName, submitter,
          appNameSuffix, keepContainersAcrossApplicationAttempts, rmId,
          originalAppSubmissionContext);
    }
  }

  @Test
  public void testSeparateThreadWithoutBlockServiceStop() throws Exception {
    ApplicationAttemptId attemptId1 =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(Time.now(), 1), 1);
    Token<AMRMTokenIdentifier> token1 = uamPool.launchUAM("SC-1", this.conf,
        attemptId1.getApplicationId(), "default", "test-user", "SC-HOME", true, "SC-1", null);
    Assert.assertNotNull(token1);

    ApplicationAttemptId attemptId2 =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(Time.now(), 2), 1);
    Token<AMRMTokenIdentifier> token2 = uamPool.launchUAM("SC-2", this.conf,
        attemptId2.getApplicationId(), "default", "test-user", "SC-HOME", true, "SC-2", null);
    Assert.assertNotNull(token2);

    Map<String, UnmanagedApplicationManager> unmanagedAppMasterMap =
        uamPool.getUnmanagedAppMasterMap();
    Assert.assertNotNull(unmanagedAppMasterMap);
    Assert.assertEquals(2, unmanagedAppMasterMap.size());

    // try to stop uamPool
    uamPool.stop();
    Assert.assertTrue(uamPool.waitForServiceToStop(2000));
    // process force finish Application in a separate thread, not blocking the main thread
    Assert.assertEquals(Service.STATE.STOPPED, uamPool.getServiceState());

    // Wait for the thread to terminate, check if uamPool#unmanagedAppMasterMap is 0
    Thread finishApplicationThread = uamPool.getFinishApplicationThread();
    GenericTestUtils.waitFor(() -> !finishApplicationThread.isAlive(),
        100, 2000);
    Assert.assertEquals(0, unmanagedAppMasterMap.size());
  }

  @Test
  public void testApplicationAttributes()
      throws IOException, YarnException, InterruptedException, TimeoutException {
    long now = Time.now();
    ApplicationId applicationId = ApplicationId.newInstance(now, 10);
    ApplicationSubmissionContext appSubmissionContext = ApplicationSubmissionContext.newInstance(
        applicationId, "test", "default", Priority.newInstance(10), null, true, true, 2,
        Resource.newInstance(10, 2), "test");
    Set<String> tags = Collections.singleton("1");
    appSubmissionContext.setApplicationTags(tags);

    Token<AMRMTokenIdentifier> token1 = uamPool.launchUAM("SC-1", this.conf,
        applicationId, "default", "test-user", "SC-HOME", true, "SC-1", appSubmissionContext);
    Assert.assertNotNull(token1);

    Map<String, UnmanagedApplicationManager> unmanagedAppMasterMap =
        uamPool.getUnmanagedAppMasterMap();

    UnmanagedApplicationManager uamApplicationManager = unmanagedAppMasterMap.get("SC-1");
    Assert.assertNotNull(uamApplicationManager);

    ApplicationSubmissionContext appSubmissionContextByUam =
        uamApplicationManager.getApplicationSubmissionContext();

    Assert.assertNotNull(appSubmissionContext);
    Assert.assertEquals(10, appSubmissionContextByUam.getPriority().getPriority());
    Assert.assertEquals("test", appSubmissionContextByUam.getApplicationType());
    Assert.assertEquals(1, appSubmissionContextByUam.getApplicationTags().size());

    uamPool.stop();
    Thread finishApplicationThread = uamPool.getFinishApplicationThread();
    GenericTestUtils.waitFor(() -> !finishApplicationThread.isAlive(),
        100, 2000);
    Assert.assertEquals(0, unmanagedAppMasterMap.size());
  }
}