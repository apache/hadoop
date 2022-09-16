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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.*;

public class TestAMAuthorization {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAMAuthorization.class);

  private MockRM rm;

  // Note : Any test case in ResourceManager package that creates a proxy has
  // to be run with enabling hadoop.security.token.service.use_ip. And reset
  // to false at the end of test class. See YARN-5208
  @BeforeAll
  static void setUp() {
    Configuration conf = new Configuration();
    conf.setBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP, true);
    SecurityUtil.setConfiguration(conf);
  }

  @AfterAll
  static void resetConf() {
    Configuration conf = new Configuration();
    conf.setBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP, false);
    SecurityUtil.setConfiguration(conf);
  }

  public static Collection<Object[]> configs() {
    Configuration conf = new Configuration();
    Configuration confWithSecurity = new Configuration();
    confWithSecurity.set(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      UserGroupInformation.AuthenticationMethod.KERBEROS.toString());
    return Arrays.asList(new Object[][] {{ conf }, { confWithSecurity} });
  }

  @AfterEach
  public void tearDown() {
    if (rm != null) {
      rm.stop();
    }
  }

  public static final class MyContainerManager implements ContainerManagementProtocol {

    public ByteBuffer containerTokens;

    public MyContainerManager() {
    }

    @Override
    public StartContainersResponse
        startContainers(StartContainersRequest request)
            throws YarnException {
      containerTokens = request.getStartContainerRequests().get(0).getContainerLaunchContext().getTokens();
      return StartContainersResponse.newInstance(null, null, null);
    }

    @Override
    public StopContainersResponse stopContainers(StopContainersRequest request)
        throws YarnException {
      return StopContainersResponse.newInstance(null, null);
    }

    @Override
    public GetContainerStatusesResponse getContainerStatuses(
        GetContainerStatusesRequest request) throws YarnException {
      return GetContainerStatusesResponse.newInstance(null, null);
    }

    @Deprecated
    @Override
    public IncreaseContainersResourceResponse increaseContainersResource(IncreaseContainersResourceRequest request)
        throws YarnException {
      return IncreaseContainersResourceResponse.newInstance(null, null);
    }

    @Override
    public ContainerUpdateResponse updateContainer(ContainerUpdateRequest
        request) throws YarnException, IOException {
      return ContainerUpdateResponse.newInstance(null, null);
    }

    public Credentials getContainerCredentials() throws IOException {
      Credentials credentials = new Credentials();
      DataInputByteBuffer buf = new DataInputByteBuffer();
      containerTokens.rewind();
      buf.reset(containerTokens);
      credentials.readTokenStorageStream(buf);
      return credentials;
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
    public GetLocalizationStatusesResponse getLocalizationStatuses(
        GetLocalizationStatusesRequest request) throws YarnException,
        IOException {
      return null;
    }
  }

  public static class MockRMWithAMS extends MockRMWithCustomAMLauncher {

    public MockRMWithAMS(Configuration conf, ContainerManagementProtocol containerManager) {
      super(conf, containerManager);
    }

    @Override
    protected void doSecureLogin() throws IOException {
      // Skip the login.
    }

    @Override
    protected ApplicationMasterService createApplicationMasterService() {
      return new ApplicationMasterService(getRMContext(), this.scheduler);
    }

    @SuppressWarnings("unchecked")
    public static Token<? extends TokenIdentifier> setupAndReturnAMRMToken(
        InetSocketAddress rmBindAddress,
        Collection<Token<? extends TokenIdentifier>> allTokens) {
      for (Token<? extends TokenIdentifier> token : allTokens) {
        if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
          SecurityUtil.setTokenService(token, rmBindAddress);
          return (Token<AMRMTokenIdentifier>) token;
        }
      }
      return null;
    }
  }

  @ParameterizedTest
  @MethodSource("configs")
  void testAuthorizedAccess(final Configuration conf) throws Exception {
    UserGroupInformation.setConfiguration(conf);
    MyContainerManager containerManager = new MyContainerManager();
    rm =
        new MockRMWithAMS(conf, containerManager);
    rm.start();

    MockNM nm1 = rm.registerNode("localhost:1234", 5120);

    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>(2);
    acls.put(ApplicationAccessType.VIEW_APP, "*");
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
            .withAppName("appname")
            .withUser("appuser")
            .withAcls(acls)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);

    nm1.nodeHeartbeat(true);

    int waitCount = 0;
    while (containerManager.containerTokens == null && waitCount++ < 20) {
      LOG.info("Waiting for AM Launch to happen..");
      Thread.sleep(1000);
    }
    Assertions.assertNotNull(containerManager.containerTokens);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId applicationAttemptId = attempt.getAppAttemptId();
    waitForLaunchedState(attempt);

    // Create a client to the RM.
    final Configuration config = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(config);

    UserGroupInformation currentUser = UserGroupInformation
        .createRemoteUser(applicationAttemptId.toString());
    Credentials credentials = containerManager.getContainerCredentials();
    final InetSocketAddress rmBindAddress =
        rm.getApplicationMasterService().getBindAddress();
    Token<? extends TokenIdentifier> amRMToken =
        MockRMWithAMS.setupAndReturnAMRMToken(rmBindAddress,
          credentials.getAllTokens());
    currentUser.addToken(amRMToken);
    ApplicationMasterProtocol client = currentUser
        .doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
          @Override
          public ApplicationMasterProtocol run() {
            return (ApplicationMasterProtocol) rpc.getProxy(ApplicationMasterProtocol.class, rm
              .getApplicationMasterService().getBindAddress(), config);
          }
        });

    RegisterApplicationMasterRequest request = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    RegisterApplicationMasterResponse response =
        client.registerApplicationMaster(request);
    Assertions.assertNotNull(response.getClientToAMTokenMasterKey());
    if (UserGroupInformation.isSecurityEnabled()) {
      Assertions
        .assertTrue(response.getClientToAMTokenMasterKey().array().length > 0);
    }
    Assertions.assertEquals("*", response.getApplicationACLs().get(ApplicationAccessType.VIEW_APP),
        "Register response has bad ACLs");
  }

  @ParameterizedTest
  @MethodSource("configs")
  void testUnauthorizedAccess(final Configuration conf) throws Exception {
    UserGroupInformation.setConfiguration(conf);
    MyContainerManager containerManager = new MyContainerManager();
    rm = new MockRMWithAMS(conf, containerManager);
    rm.start();

    MockNM nm1 = rm.registerNode("localhost:1234", 5120);

    RMApp app = MockRMAppSubmitter.submitWithMemory(1024, rm);

    nm1.nodeHeartbeat(true);

    int waitCount = 0;
    while (containerManager.containerTokens == null && waitCount++ < 40) {
      LOG.info("Waiting for AM Launch to happen..");
      Thread.sleep(1000);
    }
    Assertions.assertNotNull(containerManager.containerTokens);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId applicationAttemptId = attempt.getAppAttemptId();
    waitForLaunchedState(attempt);

    final Configuration config = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(config);
    final InetSocketAddress serviceAddr = config.getSocketAddr(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    UserGroupInformation currentUser = UserGroupInformation
        .createRemoteUser(applicationAttemptId.toString());

    // First try contacting NM without tokens
    ApplicationMasterProtocol client = currentUser
        .doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
          @Override
          public ApplicationMasterProtocol run() {
            return (ApplicationMasterProtocol) rpc.getProxy(ApplicationMasterProtocol.class,
                serviceAddr, config);
          }
        });
    
    RegisterApplicationMasterRequest request = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    try {
      client.registerApplicationMaster(request);
      Assertions.fail("Should fail with authorization error");
    } catch (Exception e) {
      if (isCause(AccessControlException.class, e)) {
        // Because there are no tokens, the request should be rejected as the
        // server side will assume we are trying simple auth.
        String expectedMessage = "";
        if (UserGroupInformation.isSecurityEnabled()) {
          expectedMessage = "Client cannot authenticate via:[TOKEN]";
        } else {
          expectedMessage =
              "SIMPLE authentication is not enabled.  Available:[TOKEN]";
        }
        Assertions.assertTrue(e.getCause().getMessage().contains(expectedMessage)); 
      } else {
        throw e;
      }
    }

    // TODO: Add validation of invalid authorization when there's more data in
    // the AMRMToken
  }
  
  /**
   * Identify if an expected throwable included in an exception stack. We use
   * this because sometimes, an exception will be wrapped to another exception
   * before thrown. Like,
   * 
   * <pre>
   * {@code
   * void methodA() throws IOException {
   *   try {
   *     // something
   *   } catch (AccessControlException e) {
   *     // do process
   *     throw new IOException(e)
   *   }
   * }
   * </pre>
   * 
   * So we cannot simply catch AccessControlException by using
   * <pre>
   * {@code
   * try {
   *   methodA()
   * } catch (AccessControlException e) {
   *   // do something
   * }
   * </pre>
   * 
   * This method is useful in such cases.
   */
  private static boolean isCause(
      Class<? extends Throwable> expected,
      Throwable e
  ) {
    return (e != null)
        && (expected.isInstance(e) || isCause(expected, e.getCause()));
  }

  private void waitForLaunchedState(RMAppAttempt attempt)
      throws InterruptedException {
    int waitCount = 0;
    while (attempt.getAppAttemptState() != RMAppAttemptState.LAUNCHED
        && waitCount++ < 40) {
      LOG.info("Waiting for AppAttempt to reach LAUNCHED state. "
          + "Current state is " + attempt.getAppAttemptState());
      Thread.sleep(1000);
    }
    Assertions.assertEquals(attempt.getAppAttemptState(),
        RMAppAttemptState.LAUNCHED);
  }
}
