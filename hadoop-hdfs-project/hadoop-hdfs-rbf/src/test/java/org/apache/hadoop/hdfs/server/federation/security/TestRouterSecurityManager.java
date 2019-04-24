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

package org.apache.hadoop.hdfs.server.federation.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.router.RouterHDFSContract;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.security.RouterSecurityManager;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.junit.rules.ExpectedException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.apache.hadoop.fs.contract.router.SecurityConfUtil.initSecurity;

import org.hamcrest.core.StringContains;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test functionality of {@link RouterSecurityManager}, which manages
 * delegation tokens for router.
 */
public class TestRouterSecurityManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterSecurityManager.class);

  private static RouterSecurityManager securityManager = null;

  @BeforeClass
  public static void createMockSecretManager() throws IOException {
    AbstractDelegationTokenSecretManager<DelegationTokenIdentifier>
        mockDelegationTokenSecretManager =
        new MockDelegationTokenSecretManager(100, 100, 100, 100);
    mockDelegationTokenSecretManager.startThreads();
    securityManager =
        new RouterSecurityManager(mockDelegationTokenSecretManager);
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testDelegationTokens() throws IOException {
    UserGroupInformation.reset();
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createUserForTesting("router", getUserGroupForTesting()));

    // Get a delegation token
    Token<DelegationTokenIdentifier> token =
        securityManager.getDelegationToken(new Text("some_renewer"));
    assertNotNull(token);
    // Renew the delegation token
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createUserForTesting("some_renewer", getUserGroupForTesting()));
    long updatedExpirationTime = securityManager.renewDelegationToken(token);
    assertTrue(updatedExpirationTime <= token.decodeIdentifier().getMaxDate());

    // Cancel the delegation token
    securityManager.cancelDelegationToken(token);

    String exceptionCause = "Renewal request for unknown token";
    exceptionRule.expect(SecretManager.InvalidToken.class);
    exceptionRule.expectMessage(exceptionCause);

    // This throws an exception as token has been cancelled.
    securityManager.renewDelegationToken(token);
  }

  @Test
  public void testVerifyToken() throws IOException {
    UserGroupInformation.reset();
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createUserForTesting("router", getUserGroupForTesting()));

    // Get a delegation token
    Token<DelegationTokenIdentifier> token =
        securityManager.getDelegationToken(new Text("some_renewer"));
    assertNotNull(token);

    // Verify the password in delegation token
    securityManager.verifyToken(token.decodeIdentifier(),
        token.getPassword());

    // Verify an invalid password
    String exceptionCause = "password doesn't match";
    exceptionRule.expect(SecretManager.InvalidToken.class);
    exceptionRule.expectMessage(
        StringContains.containsString(exceptionCause));

    securityManager.verifyToken(token.decodeIdentifier(), new byte[10]);
  }

  @Test
  public void testCreateCredentials() throws Exception {
    Configuration conf = initSecurity();

    // Start routers with only an RPC service
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();

    conf.addResource(routerConf);
    Router router = new Router();
    router.init(conf);
    router.start();

    UserGroupInformation ugi =
        UserGroupInformation.createUserForTesting(
        "router", getUserGroupForTesting());
    Credentials creds = RouterSecurityManager.createCredentials(
        router, ugi, "some_renewer");
    for (Token token : creds.getAllTokens()) {
      assertNotNull(token);
      // Verify properties of the token
      assertEquals("HDFS_DELEGATION_TOKEN", token.getKind().toString());
      DelegationTokenIdentifier identifier = (DelegationTokenIdentifier)
          token.decodeIdentifier();
      assertNotNull(identifier);
      String owner = identifier.getOwner().toString();
      // Windows will not reverse name lookup "127.0.0.1" to "localhost".
      String host = Path.WINDOWS ? "127.0.0.1" : "localhost";
      String expectedOwner = "router/"+ host + "@EXAMPLE.COM";
      assertEquals(expectedOwner, owner);
      assertEquals("some_renewer", identifier.getRenewer().toString());
    }
    RouterHDFSContract.destroyCluster();
  }


  private static String[] getUserGroupForTesting() {
    String[] groupsForTesting = {"router_group"};
    return groupsForTesting;
  }
}
