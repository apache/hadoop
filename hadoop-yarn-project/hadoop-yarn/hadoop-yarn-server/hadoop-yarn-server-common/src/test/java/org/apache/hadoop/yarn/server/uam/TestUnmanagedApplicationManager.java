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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.MockResourceManagerFacade;
import org.apache.hadoop.yarn.util.AsyncCallback;
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

  @Before
  public void setup() {
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "subclusterId");
    callback = new CountingCallback();

    attemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1);

    uam = new TestableUnmanagedApplicationManager(conf,
        attemptId.getApplicationId(), null, "submitter", "appNameSuffix", true);
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

  @Test(timeout = 5000)
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
        attemptId.getApplicationId(), null, "submitter", "appNameSuffix", true);
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
    Object syncObj = MockResourceManagerFacade.getSyncObj();

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

  @Test
  public void testForceKill()
      throws YarnException, IOException, InterruptedException {
    launchUAM(attemptId);
    registerApplicationMaster(
        RegisterApplicationMasterRequest.newInstance(null, 0, null), attemptId);
    uam.forceKillApplication();

    try {
      uam.forceKillApplication();
      Assert.fail("Should fail because application is already killed");
    } catch (YarnException t) {
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
        .doAs(new PrivilegedExceptionAction<Token<AMRMTokenIdentifier>>() {
          @Override
          public Token<AMRMTokenIdentifier> run() throws Exception {
            return uam.launchUAM();
          }
        });
  }

  protected void reAttachUAM(final Token<AMRMTokenIdentifier> uamToken,
      ApplicationAttemptId appAttemptId)
      throws IOException, InterruptedException {
    getUGIWithToken(appAttemptId).doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Token<AMRMTokenIdentifier> run() throws Exception {
        uam.reAttachUAM(uamToken);
        return null;
      }
    });
  }

  protected RegisterApplicationMasterResponse registerApplicationMaster(
      final RegisterApplicationMasterRequest request,
      ApplicationAttemptId appAttemptId)
      throws YarnException, IOException, InterruptedException {
    return getUGIWithToken(appAttemptId).doAs(
        new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {
          @Override
          public RegisterApplicationMasterResponse run()
              throws YarnException, IOException {
            return uam.registerApplicationMaster(request);
          }
        });
  }

  protected void allocateAsync(final AllocateRequest request,
      final AsyncCallback<AllocateResponse> callBack,
      ApplicationAttemptId appAttemptId)
      throws YarnException, IOException, InterruptedException {
    getUGIWithToken(appAttemptId).doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws YarnException {
        uam.allocateAsync(request, callBack);
        return null;
      }
    });
  }

  protected FinishApplicationMasterResponse finishApplicationMaster(
      final FinishApplicationMasterRequest request,
      ApplicationAttemptId appAttemptId)
      throws YarnException, IOException, InterruptedException {
    return getUGIWithToken(appAttemptId)
        .doAs(new PrivilegedExceptionAction<FinishApplicationMasterResponse>() {
          @Override
          public FinishApplicationMasterResponse run()
              throws YarnException, IOException {
            FinishApplicationMasterResponse response =
                uam.finishApplicationMaster(request);
            return response;
          }
        });
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
  public static class TestableUnmanagedApplicationManager
      extends UnmanagedApplicationManager {

    private MockResourceManagerFacade rmProxy;

    public TestableUnmanagedApplicationManager(Configuration conf,
        ApplicationId appId, String queueName, String submitter,
        String appNameSuffix, boolean keepContainersAcrossApplicationAttempts) {
      super(conf, appId, queueName, submitter, appNameSuffix,
          keepContainersAcrossApplicationAttempts, "TEST");
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

}