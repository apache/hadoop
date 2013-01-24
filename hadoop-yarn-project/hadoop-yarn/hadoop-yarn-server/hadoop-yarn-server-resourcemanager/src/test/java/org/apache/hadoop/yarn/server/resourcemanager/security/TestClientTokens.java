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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ClientToken;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.security.client.ClientTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientTokenSelector;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMWithCustomAMLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ProtoUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

public class TestClientTokens {

  private interface CustomProtocol {
    public static final long versionID = 1L;

    public void ping();
  }

  private static class CustomSecurityInfo extends SecurityInfo {

    @Override
    public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
      return new TokenInfo() {

        @Override
        public Class<? extends Annotation> annotationType() {
          return null;
        }

        @Override
        public Class<? extends TokenSelector<? extends TokenIdentifier>>
            value() {
          return ClientTokenSelector.class;
        }
      };
    }

    @Override
    public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
      return null;
    }
  };

  private static class CustomAM extends AbstractService implements
      CustomProtocol {

    private final ApplicationAttemptId appAttemptId;
    private final String secretKey;
    private InetSocketAddress address;
    private boolean pinged = false;

    public CustomAM(ApplicationAttemptId appId, String secretKeyStr) {
      super("CustomAM");
      this.appAttemptId = appId;
      this.secretKey = secretKeyStr;
    }

    @Override
    public void ping() {
      this.pinged = true;
    }

    @Override
    public synchronized void start() {
      Configuration conf = getConfig();

      ClientToAMTokenSecretManager secretManager = null;
      byte[] bytes = Base64.decodeBase64(this.secretKey);
      secretManager = new ClientToAMTokenSecretManager(this.appAttemptId, bytes);
      Server server;
      try {
        server =
            new RPC.Builder(conf).setProtocol(CustomProtocol.class)
              .setNumHandlers(1).setSecretManager(secretManager)
              .setInstance(this).build();
      } catch (Exception e) {
        throw new YarnException(e);
      }
      server.start();
      this.address = NetUtils.getConnectAddress(server);
      super.start();
    }
  }

  private static class CustomNM implements ContainerManager {

    public String clientTokensSecret;

    @Override
    public StartContainerResponse startContainer(StartContainerRequest request)
        throws YarnRemoteException {
      this.clientTokensSecret =
          request.getContainerLaunchContext().getEnvironment()
            .get(ApplicationConstants.APPLICATION_CLIENT_SECRET_ENV_NAME);
      return null;
    }

    @Override
    public StopContainerResponse stopContainer(StopContainerRequest request)
        throws YarnRemoteException {
      return null;
    }

    @Override
    public GetContainerStatusResponse getContainerStatus(
        GetContainerStatusRequest request) throws YarnRemoteException {
      return null;
    }

  }

  @Test
  public void testClientTokens() throws Exception {

    final Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    UserGroupInformation.setConfiguration(conf);

    CustomNM containerManager = new CustomNM();
    final DrainDispatcher dispatcher = new DrainDispatcher();

    MockRM rm = new MockRMWithCustomAMLauncher(conf, containerManager) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
          this.rmAppManager, this.applicationACLsManager,
          this.rmDTSecretManager);
      };

      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }

      @Override
      protected void doSecureLogin() throws IOException {
      }
    };
    rm.start();

    // Submit an app
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    // Set up a node.
    MockNM nm1 = rm.registerNode("localhost:1234", 3072);
    nm1.nodeHeartbeat(true);
    dispatcher.await();

    // Get the app-report.
    GetApplicationReportRequest request =
        Records.newRecord(GetApplicationReportRequest.class);
    request.setApplicationId(app.getApplicationId());
    GetApplicationReportResponse reportResponse =
        rm.getClientRMService().getApplicationReport(request);
    ApplicationReport appReport = reportResponse.getApplicationReport();
    ClientToken clientToken = appReport.getClientToken();

    // Wait till AM is 'launched'
    int waitTime = 0;
    while (containerManager.clientTokensSecret == null && waitTime++ < 20) {
      Thread.sleep(1000);
    }
    Assert.assertNotNull(containerManager.clientTokensSecret);

    // Start the AM with the correct shared-secret.
    ApplicationAttemptId appAttemptId =
        app.getAppAttempts().keySet().iterator().next();
    Assert.assertNotNull(appAttemptId);
    final CustomAM am =
        new CustomAM(appAttemptId, containerManager.clientTokensSecret);
    am.init(conf);
    am.start();

    // Now the real test!
    // Set up clients to be able to pick up correct tokens.
    SecurityUtil.setSecurityInfoProviders(new CustomSecurityInfo());

    // Verify denial for unauthenticated user
    try {
      CustomProtocol client =
          (CustomProtocol) RPC.getProxy(CustomProtocol.class, 1L, am.address,
            conf);
      client.ping();
      fail("Access by unauthenticated user should fail!!");
    } catch (Exception e) {
      Assert.assertFalse(am.pinged);
    }

    // Verify denial for a malicious user
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("me");
    Token<ClientTokenIdentifier> token =
        ProtoUtils.convertFromProtoFormat(clientToken, am.address);

    // Malicious user, messes with appId
    ClientTokenIdentifier maliciousID =
        new ClientTokenIdentifier(BuilderUtils.newApplicationAttemptId(
          BuilderUtils.newApplicationId(app.getApplicationId()
            .getClusterTimestamp(), 42), 43));

    Token<ClientTokenIdentifier> maliciousToken =
        new Token<ClientTokenIdentifier>(maliciousID.getBytes(),
          token.getPassword(), token.getKind(),
          token.getService());
    ugi.addToken(maliciousToken);

    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          CustomProtocol client =
              (CustomProtocol) RPC.getProxy(CustomProtocol.class, 1L,
                am.address, conf);
          client.ping();
          fail("Connection initiation with illegally modified "
              + "tokens is expected to fail.");
          return null;
        }
      });
    } catch (YarnRemoteException e) {
      fail("Cannot get a YARN remote exception as "
          + "it will indicate RPC success");
    } catch (Exception e) {
      Assert
        .assertEquals(java.lang.reflect.UndeclaredThrowableException.class
          .getCanonicalName(), e.getClass().getCanonicalName());
      Assert.assertTrue(e
        .getCause()
        .getMessage()
        .contains(
          "DIGEST-MD5: digest response format violation. "
              + "Mismatched response."));
      Assert.assertFalse(am.pinged);
    }

    // Now for an authenticated user
    ugi = UserGroupInformation.createRemoteUser("me");
    ugi.addToken(token);

    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        CustomProtocol client =
            (CustomProtocol) RPC.getProxy(CustomProtocol.class, 1L, am.address,
              conf);
        client.ping();
        Assert.assertTrue(am.pinged);
        return null;
      }
    });
  }

}
