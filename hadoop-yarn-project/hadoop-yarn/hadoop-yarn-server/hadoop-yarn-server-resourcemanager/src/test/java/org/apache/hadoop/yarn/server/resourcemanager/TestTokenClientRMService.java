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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTokenClientRMService {

  private final static String kerberosRule =
      "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\nDEFAULT";
  private static RMDelegationTokenSecretManager dtsm;
  static {
    KerberosName.setRules(kerberosRule);
  }

  private static final UserGroupInformation owner = UserGroupInformation
      .createRemoteUser("owner", AuthMethod.KERBEROS);
  private static final UserGroupInformation other = UserGroupInformation
      .createRemoteUser("other", AuthMethod.KERBEROS);
  private static final UserGroupInformation tester = UserGroupInformation
      .createRemoteUser("tester", AuthMethod.KERBEROS);
  private static final String testerPrincipal = "tester@EXAMPLE.COM";
  private static final String ownerPrincipal = "owner@EXAMPLE.COM";
  private static final String otherPrincipal = "other@EXAMPLE.COM";
  private static final UserGroupInformation testerKerb = UserGroupInformation
      .createRemoteUser(testerPrincipal, AuthMethod.KERBEROS);
  private static final UserGroupInformation ownerKerb = UserGroupInformation
      .createRemoteUser(ownerPrincipal, AuthMethod.KERBEROS);
  private static final UserGroupInformation otherKerb = UserGroupInformation
      .createRemoteUser(otherPrincipal, AuthMethod.KERBEROS);

  @BeforeClass
  public static void setupSecretManager() throws IOException {
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getStateStore()).thenReturn(new NullRMStateStore());
    dtsm =
        new RMDelegationTokenSecretManager(60000, 60000, 60000, 60000,
            rmContext);
    dtsm.startThreads();
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("hadoop.security.auth_to_local", kerberosRule);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.getLoginUser()
       .setAuthenticationMethod(AuthenticationMethod.KERBEROS);
  }

  @AfterClass
  public static void teardownSecretManager() {
    if (dtsm != null) {
      dtsm.stopThreads();
    }
  }

  @Test
  public void testTokenCancellationByOwner() throws Exception {
    // two tests required - one with a kerberos name
    // and with a short name
    RMContext rmContext = mock(RMContext.class);
    final ClientRMService rmService =
        new ClientRMService(rmContext, null, null, null, null, dtsm);
    testerKerb.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTokenCancellation(rmService, testerKerb, other);
        return null;
      }
    });
    owner.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTokenCancellation(owner, other);
        return null;
      }
    });
  }

  @Test
  public void testTokenRenewalWrongUser() throws Exception {
    try {
      owner.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try {
            checkTokenRenewal(owner, other);
            return null;
          } catch (YarnException ex) {
            Assert.assertTrue(ex.getMessage().contains(
                owner.getUserName() + " tries to renew a token"));
            Assert.assertTrue(ex.getMessage().contains(
                "with non-matching renewer " + other.getUserName()));
            throw ex;
          }
        }
      });
    } catch (Exception e) {
      return;
    }
    Assert.fail("renew should have failed");
  }

  @Test
  public void testTokenRenewalByLoginUser() throws Exception {
    UserGroupInformation.getLoginUser().doAs(
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            checkTokenRenewal(owner, owner);
            checkTokenRenewal(owner, other);
            return null;
          }
        });
  }

  private void checkTokenRenewal(UserGroupInformation owner,
      UserGroupInformation renewer) throws IOException, YarnException {
    RMDelegationTokenIdentifier tokenIdentifier =
        new RMDelegationTokenIdentifier(new Text(owner.getUserName()),
            new Text(renewer.getUserName()), null);
    Token<?> token =
        new Token<RMDelegationTokenIdentifier>(tokenIdentifier, dtsm);
    org.apache.hadoop.yarn.api.records.Token dToken =
        BuilderUtils.newDelegationToken(token.getIdentifier(), token.getKind()
            .toString(), token.getPassword(), token.getService().toString());
    RenewDelegationTokenRequest request =
        Records.newRecord(RenewDelegationTokenRequest.class);
    request.setDelegationToken(dToken);

    RMContext rmContext = mock(RMContext.class);
    ClientRMService rmService =
        new ClientRMService(rmContext, null, null, null, null, dtsm);
    rmService.renewDelegationToken(request);
  }

  @Test
  public void testTokenCancellationByRenewer() throws Exception {
    // two tests required - one with a kerberos name
    // and with a short name
    RMContext rmContext = mock(RMContext.class);
    final ClientRMService rmService =
        new ClientRMService(rmContext, null, null, null, null, dtsm);
    testerKerb.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTokenCancellation(rmService, owner, testerKerb);
        return null;
      }
    });
    other.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTokenCancellation(owner, other);
        return null;
      }
    });
  }

  @Test
  public void testTokenCancellationByWrongUser() {
    // two sets to test -
    // 1. try to cancel tokens of short and kerberos users as a kerberos UGI
    // 2. try to cancel tokens of short and kerberos users as a simple auth UGI

    RMContext rmContext = mock(RMContext.class);
    final ClientRMService rmService =
        new ClientRMService(rmContext, null, null, null, null, dtsm);
    UserGroupInformation[] kerbTestOwners =
        { owner, other, tester, ownerKerb, otherKerb };
    UserGroupInformation[] kerbTestRenewers =
        { owner, other, ownerKerb, otherKerb };
    for (final UserGroupInformation tokOwner : kerbTestOwners) {
      for (final UserGroupInformation tokRenewer : kerbTestRenewers) {
        try {
          testerKerb.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              try {
                checkTokenCancellation(rmService, tokOwner, tokRenewer);
                Assert.fail("We should not reach here; token owner = "
                    + tokOwner.getUserName() + ", renewer = "
                    + tokRenewer.getUserName());
                return null;
              } catch (YarnException e) {
                Assert.assertTrue(e.getMessage().contains(
                    testerKerb.getUserName()
                        + " is not authorized to cancel the token"));
                return null;
              }
            }
          });
        } catch (Exception e) {
          Assert.fail("Unexpected exception; " + e.getMessage());
        }
      }
    }

    UserGroupInformation[] simpleTestOwners =
        { owner, other, ownerKerb, otherKerb, testerKerb };
    UserGroupInformation[] simpleTestRenewers =
        { owner, other, ownerKerb, otherKerb };
    for (final UserGroupInformation tokOwner : simpleTestOwners) {
      for (final UserGroupInformation tokRenewer : simpleTestRenewers) {
        try {
          tester.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              try {
                checkTokenCancellation(tokOwner, tokRenewer);
                Assert.fail("We should not reach here; token owner = "
                    + tokOwner.getUserName() + ", renewer = "
                    + tokRenewer.getUserName());
                return null;
              } catch (YarnException ex) {
                Assert.assertTrue(ex.getMessage().contains(
                    tester.getUserName()
                        + " is not authorized to cancel the token"));
                return null;
              }
            }
          });
        } catch (Exception e) {
          Assert.fail("Unexpected exception; " + e.getMessage());
        }
      }
    }
  }

  private void checkTokenCancellation(UserGroupInformation owner,
      UserGroupInformation renewer) throws IOException, YarnException {
    RMContext rmContext = mock(RMContext.class);
    final ClientRMService rmService =
        new ClientRMService(rmContext, null, null, null, null, dtsm);
    checkTokenCancellation(rmService, owner, renewer);
  }

  private void checkTokenCancellation(ClientRMService rmService,
      UserGroupInformation owner, UserGroupInformation renewer)
      throws IOException, YarnException {
    RMDelegationTokenIdentifier tokenIdentifier =
        new RMDelegationTokenIdentifier(new Text(owner.getUserName()),
            new Text(renewer.getUserName()), null);
    Token<?> token =
        new Token<RMDelegationTokenIdentifier>(tokenIdentifier, dtsm);
    org.apache.hadoop.yarn.api.records.Token dToken =
        BuilderUtils.newDelegationToken(token.getIdentifier(), token.getKind()
            .toString(), token.getPassword(), token.getService().toString());
    CancelDelegationTokenRequest request =
        Records.newRecord(CancelDelegationTokenRequest.class);
    request.setDelegationToken(dToken);
    rmService.cancelDelegationToken(request);
  }

  @Test
  public void testTokenRenewalByOwner() throws Exception {
    owner.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTokenRenewal(owner, owner);
        return null;
      }
    });
  }

}
