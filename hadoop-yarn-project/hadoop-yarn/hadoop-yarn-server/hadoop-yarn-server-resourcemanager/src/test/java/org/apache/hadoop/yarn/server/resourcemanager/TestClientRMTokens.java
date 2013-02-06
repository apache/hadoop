/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ProtoUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;


public class TestClientRMTokens {

  private static final Log LOG = LogFactory.getLog(TestClientRMTokens.class);
  
  @Before
  public void resetSecretManager() {
    RMDelegationTokenIdentifier.Renewer.setSecretManager(null, null);
  }
  
  @Test
  public void testDelegationToken() throws IOException, InterruptedException {
    
    final YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_PRINCIPAL, "testuser/localhost@apache.org");

    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    
    ResourceScheduler scheduler = createMockScheduler(conf);
    
    long initialInterval = 10000l;
    long maxLifetime= 20000l;
    long renewInterval = 10000l;

    RMDelegationTokenSecretManager rmDtSecretManager = createRMDelegationTokenSecretManager(
        initialInterval, maxLifetime, renewInterval);
    rmDtSecretManager.startThreads();
    LOG.info("Creating DelegationTokenSecretManager with initialInterval: "
        + initialInterval + ", maxLifetime: " + maxLifetime
        + ", renewInterval: " + renewInterval);

    final ClientRMService clientRMService = new ClientRMServiceForTest(conf,
        scheduler, rmDtSecretManager);
    clientRMService.init(conf);
    clientRMService.start();

    ClientRMProtocol clientRMWithDT = null;
    try {

      // Create a user for the renewr and fake the authentication-method
      UserGroupInformation loggedInUser = UserGroupInformation
          .createRemoteUser("testrenewer@APACHE.ORG");
      Assert.assertEquals("testrenewer", loggedInUser.getShortUserName());
      // Default realm is APACHE.ORG
      loggedInUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS);

      
      DelegationToken token =
          getDelegationToken(loggedInUser, clientRMService,
              loggedInUser.getShortUserName());
      long tokenFetchTime = System.currentTimeMillis();
      LOG.info("Got delegation token at: " + tokenFetchTime);
 
      // Now try talking to RMService using the delegation token
      clientRMWithDT = getClientRMProtocolWithDT(token,
          clientRMService.getBindAddress(), "loginuser1", conf);

      GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
      
      try {
        clientRMWithDT.getNewApplication(request);
      } catch (UndeclaredThrowableException e) {
        fail("Unexpected exception" + e);
      }
      
      // Renew after 50% of token age.
      while(System.currentTimeMillis() < tokenFetchTime + initialInterval / 2) {
        Thread.sleep(500l);
      }
      long nextExpTime = renewDelegationToken(loggedInUser, clientRMService, token);
      long renewalTime = System.currentTimeMillis();
      LOG.info("Renewed token at: " + renewalTime + ", NextExpiryTime: "
          + nextExpTime);

      // Wait for first expiry, but before renewed expiry.
      while (System.currentTimeMillis() > tokenFetchTime + initialInterval
          && System.currentTimeMillis() < nextExpTime) {
        Thread.sleep(500l);
      }
      Thread.sleep(50l);
      
      // Valid token because of renewal.
      try {
        clientRMWithDT.getNewApplication(request);
      } catch (UndeclaredThrowableException e) {
        fail("Unexpected exception" + e);
      }
      
      // Wait for expiry.
      while(System.currentTimeMillis() < renewalTime + renewInterval) {
        Thread.sleep(500l);
      }
      Thread.sleep(50l);
      LOG.info("At time: " + System.currentTimeMillis() + ", token should be invalid");
      // Token should have expired.      
      try {
        clientRMWithDT.getNewApplication(request);
        fail("Should not have succeeded with an expired token");
      } catch (UndeclaredThrowableException e) {
        assertTrue(e.getCause().getMessage().contains("is expired"));
      }

      // Test cancellation
      // Stop the existing proxy, start another.
      if (clientRMWithDT != null) {
        RPC.stopProxy(clientRMWithDT);
        clientRMWithDT = null;
      }
      token =
          getDelegationToken(loggedInUser, clientRMService,
              loggedInUser.getShortUserName());
      tokenFetchTime = System.currentTimeMillis();
      LOG.info("Got delegation token at: " + tokenFetchTime);
 
      // Now try talking to RMService using the delegation token
      clientRMWithDT = getClientRMProtocolWithDT(token,
          clientRMService.getBindAddress(), "loginuser2", conf);

      request = Records.newRecord(GetNewApplicationRequest.class);
      
      try {
        clientRMWithDT.getNewApplication(request);
      } catch (UndeclaredThrowableException e) {
        fail("Unexpected exception" + e);
      }
      cancelDelegationToken(loggedInUser, clientRMService, token);
      if (clientRMWithDT != null) {
        RPC.stopProxy(clientRMWithDT);
        clientRMWithDT = null;
      } 
      
      // Creating a new connection.
      clientRMWithDT = getClientRMProtocolWithDT(token,
          clientRMService.getBindAddress(), "loginuser2", conf);
      LOG.info("Cancelled delegation token at: " + System.currentTimeMillis());
      // Verify cancellation worked.
      try {
        clientRMWithDT.getNewApplication(request);
        fail("Should not have succeeded with a cancelled delegation token");
      } catch (UndeclaredThrowableException e) {
      }



    } finally {
      rmDtSecretManager.stopThreads();
      // TODO PRECOMMIT Close proxies.
      if (clientRMWithDT != null) {
        RPC.stopProxy(clientRMWithDT);
      }
    }
  }
  
  @Test
  public void testShortCircuitRenewCancel()
      throws IOException, InterruptedException {
    InetSocketAddress addr =
        new InetSocketAddress(InetAddress.getLocalHost(), 123);    
    checkShortCircuitRenewCancel(addr, addr, true);
  }

  @Test
  public void testShortCircuitRenewCancelWildcardAddress()
      throws IOException, InterruptedException {
    InetSocketAddress rmAddr = new InetSocketAddress(123);
    checkShortCircuitRenewCancel(
        rmAddr,
        new InetSocketAddress(InetAddress.getLocalHost(), rmAddr.getPort()),
        true);
  }

  @Test
  public void testShortCircuitRenewCancelSameHostDifferentPort()
      throws IOException, InterruptedException {
    InetSocketAddress rmAddr =
        new InetSocketAddress(InetAddress.getLocalHost(), 123);    
    checkShortCircuitRenewCancel(
        rmAddr,
        new InetSocketAddress(rmAddr.getAddress(), rmAddr.getPort()+1),
        false);
  }

  @Test
  public void testShortCircuitRenewCancelDifferentHostSamePort()
      throws IOException, InterruptedException {
    InetSocketAddress rmAddr =
        new InetSocketAddress(InetAddress.getLocalHost(), 123);    
    checkShortCircuitRenewCancel(
        rmAddr,
        new InetSocketAddress("1.1.1.1", rmAddr.getPort()),
        false);
  }

  @Test
  public void testShortCircuitRenewCancelDifferentHostDifferentPort()
      throws IOException, InterruptedException {
    InetSocketAddress rmAddr =
        new InetSocketAddress(InetAddress.getLocalHost(), 123);    
    checkShortCircuitRenewCancel(
        rmAddr,
        new InetSocketAddress("1.1.1.1", rmAddr.getPort()+1),
        false);
  }

  @SuppressWarnings("unchecked")
  private void checkShortCircuitRenewCancel(InetSocketAddress rmAddr,
                                            InetSocketAddress serviceAddr,
                                            boolean shouldShortCircuit
      ) throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.IPC_RPC_IMPL,
        YarnBadRPC.class, YarnRPC.class);
    
    RMDelegationTokenSecretManager secretManager =
        mock(RMDelegationTokenSecretManager.class);
    RMDelegationTokenIdentifier.Renewer.setSecretManager(secretManager, rmAddr);

    RMDelegationTokenIdentifier ident = new RMDelegationTokenIdentifier(
        new Text("owner"), new Text("renewer"), null);
    Token<RMDelegationTokenIdentifier> token =
        new Token<RMDelegationTokenIdentifier>(ident, secretManager);

    SecurityUtil.setTokenService(token, serviceAddr);
    if (shouldShortCircuit) {
      token.renew(conf);
      verify(secretManager).renewToken(eq(token), eq("renewer"));
      reset(secretManager);
      token.cancel(conf);
      verify(secretManager).cancelToken(eq(token), eq("renewer"));
    } else {      
      try { 
        token.renew(conf);
        fail();
      } catch (RuntimeException e) {
        assertEquals("getProxy", e.getMessage());
      }
      verify(secretManager, never()).renewToken(any(Token.class), anyString());
      try { 
        token.cancel(conf);
        fail();
      } catch (RuntimeException e) {
        assertEquals("getProxy", e.getMessage());
      }
      verify(secretManager, never()).cancelToken(any(Token.class), anyString());
    }
  }
  
  @SuppressWarnings("rawtypes")
  public static class YarnBadRPC extends YarnRPC {
    @Override
    public Object getProxy(Class protocol, InetSocketAddress addr,
        Configuration conf) {
      throw new RuntimeException("getProxy");
    }

    @Override
    public void stopProxy(Object proxy, Configuration conf) {
      throw new RuntimeException("stopProxy");
    }

    @Override
    public Server getServer(Class protocol, Object instance,
        InetSocketAddress addr, Configuration conf,
        SecretManager<? extends TokenIdentifier> secretManager,
        int numHandlers, String portRangeConfig) {
      throw new RuntimeException("getServer");
    }
  }
  
  // Get the delegation token directly as it is a little difficult to setup
  // the kerberos based rpc.
  private DelegationToken getDelegationToken(
      final UserGroupInformation loggedInUser,
      final ClientRMProtocol clientRMService, final String renewerString)
      throws IOException,
      InterruptedException {
    DelegationToken token = loggedInUser
        .doAs(new PrivilegedExceptionAction<DelegationToken>() {
          @Override
          public DelegationToken run() throws YarnRemoteException {
            GetDelegationTokenRequest request = Records
                .newRecord(GetDelegationTokenRequest.class);
            request.setRenewer(renewerString);
            return clientRMService.getDelegationToken(request)
                .getRMDelegationToken();
          }
        });
    return token;
  }
  
  private long renewDelegationToken(final UserGroupInformation loggedInUser,
      final ClientRMProtocol clientRMService, final DelegationToken dToken)
      throws IOException, InterruptedException {
    long nextExpTime = loggedInUser.doAs(new PrivilegedExceptionAction<Long>() {
      @Override
      public Long run() throws YarnRemoteException {
        RenewDelegationTokenRequest request = Records
            .newRecord(RenewDelegationTokenRequest.class);
        request.setDelegationToken(dToken);
        return clientRMService.renewDelegationToken(request)
            .getNextExpirationTime();
      }
    });
    return nextExpTime;
  }
  
  private void cancelDelegationToken(final UserGroupInformation loggedInUser,
      final ClientRMProtocol clientRMService, final DelegationToken dToken)
      throws IOException, InterruptedException {
    loggedInUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws YarnRemoteException {
        CancelDelegationTokenRequest request = Records
            .newRecord(CancelDelegationTokenRequest.class);
        request.setDelegationToken(dToken);
        clientRMService.cancelDelegationToken(request);
        return null;
      }
    });
  }
  
  private ClientRMProtocol getClientRMProtocolWithDT(DelegationToken token,
      final InetSocketAddress rmAddress, String user, final Configuration conf) {
    // Maybe consider converting to Hadoop token, serialize de-serialize etc
    // before trying to renew the token.

    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser(user);
    ugi.addToken(ProtoUtils.convertFromProtoFormat(token, rmAddress));

    final YarnRPC rpc = YarnRPC.create(conf);
    ClientRMProtocol clientRMWithDT = ugi
        .doAs(new PrivilegedAction<ClientRMProtocol>() {
          @Override
          public ClientRMProtocol run() {
            return (ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class,
                rmAddress, conf);
          }
        });
    return clientRMWithDT;
  }
  
  class ClientRMServiceForTest extends ClientRMService {
    
    public ClientRMServiceForTest(Configuration conf,
        ResourceScheduler scheduler,
        RMDelegationTokenSecretManager rmDTSecretManager) {
      super(mock(RMContext.class), scheduler, mock(RMAppManager.class),
          new ApplicationACLsManager(conf), rmDTSecretManager);
    }

    // Use a random port unless explicitly specified.
    @Override
    InetSocketAddress getBindAddress(Configuration conf) {
      return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADDRESS, 0);
    }

    @Override
    public void stop() {
      if (rmDTSecretManager != null) {
        rmDTSecretManager.stopThreads();
      }
      super.stop();
    }

    
  }

  private static ResourceScheduler createMockScheduler(Configuration conf) {
    ResourceScheduler mockSched = mock(ResourceScheduler.class);
    doReturn(BuilderUtils.newResource(512)).when(mockSched)
        .getMinimumResourceCapability();
    doReturn(BuilderUtils.newResource(5120)).when(mockSched)
        .getMaximumResourceCapability();
    return mockSched;
  }

  private static RMDelegationTokenSecretManager createRMDelegationTokenSecretManager(
      long secretKeyInterval, long tokenMaxLifetime, long tokenRenewInterval) {
    RMDelegationTokenSecretManager rmDtSecretManager = new RMDelegationTokenSecretManager(
        secretKeyInterval, tokenMaxLifetime, tokenRenewInterval, 3600000);
    return rmDtSecretManager;
  }
}
