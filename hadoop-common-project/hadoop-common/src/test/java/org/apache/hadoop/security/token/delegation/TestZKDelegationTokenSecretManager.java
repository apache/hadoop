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

package org.apache.hadoop.security.token.delegation;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import static org.junit.Assert.fail;

import org.junit.Test;

public class TestZKDelegationTokenSecretManager {

  private static final long DAY_IN_SECS = 86400;

  private TestingServer zkServer;

  @Before
  public void setup() throws Exception {
    zkServer = new TestingServer();
    zkServer.start();
  }

  @After
  public void tearDown() throws Exception {
    if (zkServer != null) {
      zkServer.close();
    }
  }

  protected Configuration getSecretConf(String connectString) {
   Configuration conf = new Configuration();
   conf.setBoolean(DelegationTokenManager.ENABLE_ZK_KEY, true);
   conf.set(ZKDelegationTokenSecretManager.ZK_DTSM_ZK_CONNECTION_STRING, connectString);
   conf.set(ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH, "testPath");
   conf.set(ZKDelegationTokenSecretManager.ZK_DTSM_ZK_AUTH_TYPE, "none");
   conf.setLong(DelegationTokenManager.UPDATE_INTERVAL, DAY_IN_SECS);
   conf.setLong(DelegationTokenManager.MAX_LIFETIME, DAY_IN_SECS);
   conf.setLong(DelegationTokenManager.RENEW_INTERVAL, DAY_IN_SECS);
   conf.setLong(DelegationTokenManager.REMOVAL_SCAN_INTERVAL, DAY_IN_SECS);
   return conf;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMultiNodeOperations() throws Exception {
    DelegationTokenManager tm1, tm2 = null;
    String connectString = zkServer.getConnectString();
    Configuration conf = getSecretConf(connectString);
    tm1 = new DelegationTokenManager(conf, new Text("bla"));
    tm1.init();
    tm2 = new DelegationTokenManager(conf, new Text("bla"));
    tm2.init();

    Token<DelegationTokenIdentifier> token =
        (Token<DelegationTokenIdentifier>) tm1.createToken(
            UserGroupInformation.getCurrentUser(), "foo");
    Assert.assertNotNull(token);
    tm2.verifyToken(token);
    tm2.renewToken(token, "foo");
    tm1.verifyToken(token);
    tm1.cancelToken(token, "foo");
    try {
      tm2.verifyToken(token);
      fail("Expected InvalidToken");
    } catch (SecretManager.InvalidToken it) {
      // Ignore
    }

    token = (Token<DelegationTokenIdentifier>) tm2.createToken(
        UserGroupInformation.getCurrentUser(), "bar");
    Assert.assertNotNull(token);
    tm1.verifyToken(token);
    tm1.renewToken(token, "bar");
    tm2.verifyToken(token);
    tm2.cancelToken(token, "bar");
    try {
      tm1.verifyToken(token);
      fail("Expected InvalidToken");
    } catch (SecretManager.InvalidToken it) {
      // Ignore
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRenewTokenSingleManager() throws Exception {
    DelegationTokenManager tm1 = null;
    String connectString = zkServer.getConnectString();
    Configuration conf = getSecretConf(connectString);
    tm1 = new DelegationTokenManager(conf, new Text("foo"));
    tm1.init();

    Token<DelegationTokenIdentifier> token =
        (Token<DelegationTokenIdentifier>)
        tm1.createToken(UserGroupInformation.getCurrentUser(), "foo");
    Assert.assertNotNull(token);
    tm1.renewToken(token, "foo");
    tm1.verifyToken(token);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCancelTokenSingleManager() throws Exception {
    DelegationTokenManager tm1 = null;
    String connectString = zkServer.getConnectString();
    Configuration conf = getSecretConf(connectString);
    tm1 = new DelegationTokenManager(conf, new Text("foo"));
    tm1.init();

    Token<DelegationTokenIdentifier> token =
        (Token<DelegationTokenIdentifier>)
        tm1.createToken(UserGroupInformation.getCurrentUser(), "foo");
    Assert.assertNotNull(token);
    tm1.cancelToken(token, "foo");
    try {
      tm1.verifyToken(token);
      fail("Expected InvalidToken");
    } catch (SecretManager.InvalidToken it) {
      it.printStackTrace();
    }
  }
}
