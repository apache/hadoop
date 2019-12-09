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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.CommitResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RestartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RollbackResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.NMNotYetReadyException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Supplier;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestApplicationMasterLauncher {

  private static final Log LOG = LogFactory
      .getLog(TestApplicationMasterLauncher.class);

  private static final class MyContainerManagerImpl implements
      ContainerManagementProtocol {

    boolean launched = false;
    boolean cleanedup = false;
    String attemptIdAtContainerManager = null;
    String containerIdAtContainerManager = null;
    String nmHostAtContainerManager = null;
    long submitTimeAtContainerManager;
    int maxAppAttempts;

    @Override
    public StartContainersResponse
        startContainers(StartContainersRequest requests)
            throws YarnException {
      StartContainerRequest request = requests.getStartContainerRequests().get(0);
      LOG.info("Container started by MyContainerManager: " + request);
      launched = true;
      Map<String, String> env =
          request.getContainerLaunchContext().getEnvironment();

      Token containerToken = request.getContainerToken();
      ContainerTokenIdentifier tokenId = null;

      try {
        tokenId = BuilderUtils.newContainerTokenIdentifier(containerToken);
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      }

      ContainerId containerId = tokenId.getContainerID();
      containerIdAtContainerManager = containerId.toString();
      attemptIdAtContainerManager =
          containerId.getApplicationAttemptId().toString();
      nmHostAtContainerManager = tokenId.getNmHostAddress();
      submitTimeAtContainerManager =
          Long.parseLong(env.get(ApplicationConstants.APP_SUBMIT_TIME_ENV));
      maxAppAttempts = YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS;
      return StartContainersResponse.newInstance(
        new HashMap<String, ByteBuffer>(), new ArrayList<ContainerId>(),
        new HashMap<ContainerId, SerializedException>());
    }

    @Override
    public StopContainersResponse stopContainers(StopContainersRequest request)
        throws YarnException {
      LOG.info("Container cleaned up by MyContainerManager");
      cleanedup = true;
      return null;
    }

    @Override
    public GetContainerStatusesResponse getContainerStatuses(
        GetContainerStatusesRequest request) throws YarnException {
      return null;
    }

    @Override
    @Deprecated
    public IncreaseContainersResourceResponse increaseContainersResource(
        IncreaseContainersResourceRequest request)
            throws YarnException {
      return null;
    }

    @Override
    public SignalContainerResponse signalToContainer(
        SignalContainerRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ResourceLocalizationResponse localize(
        ResourceLocalizationRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ReInitializeContainerResponse reInitializeContainer(
        ReInitializeContainerRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public RestartContainerResponse restartContainer(ContainerId containerId)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public RollbackResponse rollbackLastReInitialization(
        ContainerId containerId) throws YarnException, IOException {
      return null;
    }

    @Override
    public CommitResponse commitLastReInitialization(ContainerId containerId)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public ContainerUpdateResponse updateContainer(ContainerUpdateRequest
        request) throws YarnException, IOException {
      return null;
    }
  }

  @Test
  public void testAMLaunchAndCleanup() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MyContainerManagerImpl containerManager = new MyContainerManagerImpl();
    MockRMWithCustomAMLauncher rm = new MockRMWithCustomAMLauncher(
        containerManager);
    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5120);

    RMApp app = rm.submitApp(2000);

    // kick the scheduling
    nm1.nodeHeartbeat(true);

    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override public Boolean get() {
          return containerManager.launched;
        }
      }, 100, 200 * 100);
    } catch (TimeoutException e) {
      fail("timed out while waiting for AM Launch to happen.");
    }
    Assert.assertTrue(containerManager.launched);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId appAttemptId = attempt.getAppAttemptId();
    Assert.assertEquals(appAttemptId.toString(),
        containerManager.attemptIdAtContainerManager);
    Assert.assertEquals(app.getSubmitTime(),
        containerManager.submitTimeAtContainerManager);
    Assert.assertEquals(app.getRMAppAttempt(appAttemptId)
        .getMasterContainer().getId()
        .toString(), containerManager.containerIdAtContainerManager);
    Assert.assertEquals(nm1.getNodeId().toString(),
        containerManager.nmHostAtContainerManager);
    Assert.assertEquals(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS,
        containerManager.maxAppAttempts);

    MockAM am = new MockAM(rm.getRMContext(), rm
        .getApplicationMasterService(), appAttemptId);
    am.registerAppAttempt();
    am.unregisterAppAttempt();

    //complete the AM container to finish the app normally
    nm1.nodeHeartbeat(attempt.getAppAttemptId(), 1, ContainerState.COMPLETE);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);

    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override public Boolean get() {
          return containerManager.cleanedup;
        }
      }, 100, 200 * 100);
    } catch (TimeoutException e) {
      fail("timed out while waiting for AM cleanup to happen.");
    }
    Assert.assertTrue(containerManager.cleanedup);

    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);
    rm.stop();
  }

  @Test
  public void testAMCleanupBeforeLaunch() throws Exception {
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5120);
    RMApp app = rm.submitApp(2000);
    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt = app.getCurrentAppAttempt();

    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override public Boolean get() {
          return attempt.getMasterContainer() != null;
        }
      }, 10, 200 * 100);
    } catch (TimeoutException e) {
      fail("timed out while waiting for AM Launch to happen.");
    }

    //send kill before launch
    rm.killApp(app.getApplicationId());
    rm.waitForState(app.getApplicationId(), RMAppState.KILLED);
    //Launch after kill
    AMLauncher launcher = new AMLauncher(rm.getRMContext(),
            attempt, AMLauncherEventType.LAUNCH, rm.getConfig()) {
        @Override
        public void onAMLaunchFailed(ContainerId containerId, Exception e) {
          Assert.assertFalse("NullPointerException happens "
                 + " while launching " + containerId,
                   e instanceof NullPointerException);
        }
        @Override
        protected ContainerManagementProtocol getContainerMgrProxy(
            ContainerId containerId) {
          return new MyContainerManagerImpl();
        }
    };
    launcher.run();
    rm.stop();
  }

  @Test
  public void testRetriesOnFailures() throws Exception {
    final ContainerManagementProtocol mockProxy =
        mock(ContainerManagementProtocol.class);
    final StartContainersResponse mockResponse =
        mock(StartContainersResponse.class);
    when(mockProxy.startContainers(any(StartContainersRequest.class)))
        .thenThrow(new NMNotYetReadyException("foo")).thenReturn(mockResponse);
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    conf.setInt(YarnConfiguration.CLIENT_NM_CONNECT_RETRY_INTERVAL_MS, 1);
    MockRM rm = new MockRMWithCustomAMLauncher(conf, null) {
      @Override
      protected ApplicationMasterLauncher createAMLauncher() {
        return new ApplicationMasterLauncher(getRMContext()) {
          @Override
          protected Runnable createRunnableLauncher(RMAppAttempt application,
              AMLauncherEventType event) {
            return new AMLauncher(context, application, event, getConfig()) {
              @Override
              protected YarnRPC getYarnRPC() {
                YarnRPC mockRpc = mock(YarnRPC.class);

                when(mockRpc.getProxy(
                    any(Class.class),
                    any(InetSocketAddress.class),
                    any(Configuration.class)))
                    .thenReturn(mockProxy);
                return mockRpc;
              }
            };
          }
        };
      }
    };

    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5120);

    RMApp app = rm.submitApp(2000);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    rm.drainEvents();

    MockRM.waitForState(app.getCurrentAppAttempt(),
        RMAppAttemptState.LAUNCHED, 500);
  }



  @SuppressWarnings("unused")
  @Test(timeout = 100000)
  public void testallocateBeforeAMRegistration() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    boolean thrown = false;
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 5000);
    RMApp app = rm.submitApp(2000);
    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());

    // request for containers
    int request = 2;
    AllocateResponse ar = null;
    try {
      ar = am.allocate("h1", 1000, request, new ArrayList<ContainerId>());
      Assert.fail();
    } catch (ApplicationMasterNotRegisteredException e) {
    }

    // kick the scheduler
    nm1.nodeHeartbeat(true);

    AllocateResponse amrs = null;
    try {
      amrs = am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>());
      Assert.fail();
    } catch (ApplicationMasterNotRegisteredException e) {
    }

    am.registerAppAttempt();
    try {
      am.registerAppAttempt(false);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(AMRMClientUtils.APP_ALREADY_REGISTERED_MESSAGE
          + attempt.getAppAttemptId().getApplicationId(), e.getMessage());
    }

    // Simulate an AM that was disconnected and app attempt was removed
    // (responseMap does not contain attemptid)
    am.unregisterAppAttempt();
    nm1.nodeHeartbeat(attempt.getAppAttemptId(), 1,
        ContainerState.COMPLETE);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);

    try {
      amrs = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>());
      Assert.fail();
    } catch (ApplicationAttemptNotFoundException e) {
    }
  }

  @Test
  public void testSetupTokens() throws Exception {
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 5000);
    RMApp app = rm.submitApp(2000);
    /// kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MyAMLauncher launcher = new MyAMLauncher(rm.getRMContext(),
        attempt, AMLauncherEventType.LAUNCH, rm.getConfig());
    DataOutputBuffer dob = new DataOutputBuffer();
    Credentials ts = new Credentials();
    ts.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(),
        0, dob.getLength());
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(null, null,
            null, null, securityTokens, null);
    ContainerId containerId = ContainerId.newContainerId(
        attempt.getAppAttemptId(), 0L);

    try {
      launcher.setupTokens(amContainer, containerId);
    } catch (Exception e) {
      // ignore the first fake exception
    }
    try {
      launcher.setupTokens(amContainer, containerId);
    } catch (java.io.EOFException e) {
      Assert.fail("EOFException should not happen.");
    }
  }

  static class MyAMLauncher extends AMLauncher {
    int count;
    public MyAMLauncher(RMContext rmContext, RMAppAttempt application,
        AMLauncherEventType eventType, Configuration conf) {
      super(rmContext, application, eventType, conf);
      count = 0;
    }

    protected org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>
        createAndSetAMRMToken() {
      count++;
      if (count == 1) {
        throw new RuntimeException("createAndSetAMRMToken failure");
      }
      return null;
    }

    protected void setupTokens(ContainerLaunchContext container,
        ContainerId containerID) throws IOException {
      super.setupTokens(container, containerID);
    }
  }
}
