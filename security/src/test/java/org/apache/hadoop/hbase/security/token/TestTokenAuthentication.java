/*
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

package org.apache.hadoop.hbase.security.token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.SecureRpcEngine;
import org.apache.hadoop.hbase.ipc.SecureServer;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;

import org.apache.hadoop.security.token.Token;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for authentication token creation and usage
 */
public class TestTokenAuthentication {
  public static interface IdentityProtocol extends CoprocessorProtocol {
    public String whoami();
    public String getAuthMethod();
  }

  public static class IdentityCoprocessor extends BaseEndpointCoprocessor
      implements IdentityProtocol {
    public String whoami() {
      return RequestContext.getRequestUserName();
    }

    public String getAuthMethod() {
      UserGroupInformation ugi = null;
      User user = RequestContext.getRequestUser();
      if (user != null) {
        ugi = user.getUGI();
      }
      if (ugi != null) {
        return ugi.getAuthenticationMethod().toString();
      }
      return null;
    }
  }

  private static HBaseTestingUtility TEST_UTIL;
  private static AuthenticationTokenSecretManager secretManager;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HBaseRPC.RPC_ENGINE_PROP, SecureRpcEngine.class.getName());
    conf.set("hbase.coprocessor.region.classes",
        IdentityCoprocessor.class.getName());
    TEST_UTIL.startMiniCluster();
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    RpcServer server = rs.getRpcServer();
    assertTrue(server instanceof SecureServer);
    SecretManager mgr =
        ((SecureServer)server).getSecretManager();
    assertTrue(mgr instanceof AuthenticationTokenSecretManager);
    secretManager = (AuthenticationTokenSecretManager)mgr;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testTokenCreation() throws Exception {
    Token<AuthenticationTokenIdentifier> token =
        secretManager.generateToken("testuser");

    AuthenticationTokenIdentifier ident = new AuthenticationTokenIdentifier();
    Writables.getWritable(token.getIdentifier(), ident);
    assertEquals("Token username should match", "testuser",
        ident.getUsername());
    byte[] passwd = secretManager.retrievePassword(ident);
    assertTrue("Token password and password from secret manager should match",
        Bytes.equals(token.getPassword(), passwd));
  }

  // @Test - Disable due to kerberos requirement
  public void testTokenAuthentication() throws Exception {
    UserGroupInformation testuser =
        UserGroupInformation.createUserForTesting("testuser", new String[]{"testgroup"});

    testuser.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.TOKEN);
    final Configuration conf = TEST_UTIL.getConfiguration();
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("randomkey", UUID.randomUUID().toString());
    testuser.setConfiguration(conf);
    Token<AuthenticationTokenIdentifier> token =
        secretManager.generateToken("testuser");
    testuser.addToken(token);

    // verify the server authenticates us as this token user
    testuser.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws Exception {
        HTable table = new HTable(conf, ".META.");
        IdentityProtocol prot = table.coprocessorProxy(
            IdentityProtocol.class, HConstants.EMPTY_START_ROW);
        String myname = prot.whoami();
        assertEquals("testuser", myname);
        String authMethod = prot.getAuthMethod();
        assertEquals("TOKEN", authMethod);
        return null;
      }
    });
  }
}
