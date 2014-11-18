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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

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

  private static final int TEST_RETRIES = 2;

  private static final int RETRY_COUNT = 5;

  private static final int RETRY_WAIT = 1000;

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
   conf.setLong(ZKDelegationTokenSecretManager.ZK_DTSM_ZK_SHUTDOWN_TIMEOUT, 100);
   conf.setLong(DelegationTokenManager.UPDATE_INTERVAL, DAY_IN_SECS);
   conf.setLong(DelegationTokenManager.MAX_LIFETIME, DAY_IN_SECS);
   conf.setLong(DelegationTokenManager.RENEW_INTERVAL, DAY_IN_SECS);
   conf.setLong(DelegationTokenManager.REMOVAL_SCAN_INTERVAL, DAY_IN_SECS);
   return conf;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMultiNodeOperations() throws Exception {
    for (int i = 0; i < TEST_RETRIES; i++) {
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
        verifyTokenFail(tm2, token);
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
        verifyTokenFail(tm1, token);
        fail("Expected InvalidToken");
      } catch (SecretManager.InvalidToken it) {
        // Ignore
      }
      verifyDestroy(tm1, conf);
      verifyDestroy(tm2, conf);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNodeUpAferAWhile() throws Exception {
    for (int i = 0; i < TEST_RETRIES; i++) {
      String connectString = zkServer.getConnectString();
      Configuration conf = getSecretConf(connectString);
      DelegationTokenManager tm1 = new DelegationTokenManager(conf, new Text("bla"));
      tm1.init();
      Token<DelegationTokenIdentifier> token1 =
          (Token<DelegationTokenIdentifier>) tm1.createToken(
              UserGroupInformation.getCurrentUser(), "foo");
      Assert.assertNotNull(token1);
      Token<DelegationTokenIdentifier> token2 =
          (Token<DelegationTokenIdentifier>) tm1.createToken(
              UserGroupInformation.getCurrentUser(), "bar");
      Assert.assertNotNull(token2);
      Token<DelegationTokenIdentifier> token3 =
          (Token<DelegationTokenIdentifier>) tm1.createToken(
              UserGroupInformation.getCurrentUser(), "boo");
      Assert.assertNotNull(token3);

      tm1.verifyToken(token1);
      tm1.verifyToken(token2);
      tm1.verifyToken(token3);

      // Cancel one token
      tm1.cancelToken(token1, "foo");

      // Start second node after some time..
      Thread.sleep(1000);
      DelegationTokenManager tm2 = new DelegationTokenManager(conf, new Text("bla"));
      tm2.init();

      tm2.verifyToken(token2);
      tm2.verifyToken(token3);
      try {
        verifyTokenFail(tm2, token1);
        fail("Expected InvalidToken");
      } catch (SecretManager.InvalidToken it) {
        // Ignore
      }

      // Create a new token thru the new ZKDTSM
      Token<DelegationTokenIdentifier> token4 =
          (Token<DelegationTokenIdentifier>) tm2.createToken(
              UserGroupInformation.getCurrentUser(), "xyz");
      Assert.assertNotNull(token4);
      tm2.verifyToken(token4);
      tm1.verifyToken(token4);

      // Bring down tm2
      verifyDestroy(tm2, conf);

      // Start third node after some time..
      Thread.sleep(1000);
      DelegationTokenManager tm3 = new DelegationTokenManager(conf, new Text("bla"));
      tm3.init();

      tm3.verifyToken(token2);
      tm3.verifyToken(token3);
      tm3.verifyToken(token4);
      try {
        verifyTokenFail(tm3, token1);
        fail("Expected InvalidToken");
      } catch (SecretManager.InvalidToken it) {
        // Ignore
      }

      verifyDestroy(tm3, conf);
      verifyDestroy(tm1, conf);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRenewTokenSingleManager() throws Exception {
    for (int i = 0; i < TEST_RETRIES; i++) {
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
      verifyDestroy(tm1, conf);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCancelTokenSingleManager() throws Exception {
    for (int i = 0; i < TEST_RETRIES; i++) {
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
        verifyTokenFail(tm1, token);
        fail("Expected InvalidToken");
      } catch (SecretManager.InvalidToken it) {
        it.printStackTrace();
      }
      verifyDestroy(tm1, conf);
    }
  }

  @SuppressWarnings("rawtypes")
  protected void verifyDestroy(DelegationTokenManager tm, Configuration conf)
      throws Exception {
    AbstractDelegationTokenSecretManager sm =
        tm.getDelegationTokenSecretManager();
    ZKDelegationTokenSecretManager zksm = (ZKDelegationTokenSecretManager) sm;
    ExecutorService es = zksm.getListenerThreadPool();
    tm.destroy();
    Assert.assertTrue(es.isShutdown());
    // wait for the pool to terminate
    long timeout =
        conf.getLong(
            ZKDelegationTokenSecretManager.ZK_DTSM_ZK_SHUTDOWN_TIMEOUT,
            ZKDelegationTokenSecretManager.ZK_DTSM_ZK_SHUTDOWN_TIMEOUT_DEFAULT);
    Thread.sleep(timeout * 3);
    Assert.assertTrue(es.isTerminated());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testStopThreads() throws Exception {
    DelegationTokenManager tm1 = null;
    String connectString = zkServer.getConnectString();

    // let's make the update interval short and the shutdown interval
    // comparatively longer, so if the update thread runs after shutdown,
    // it will cause an error.
    final long updateIntervalSeconds = 1;
    final long shutdownTimeoutMillis = updateIntervalSeconds * 1000 * 5;
    Configuration conf = getSecretConf(connectString);
    conf.setLong(DelegationTokenManager.UPDATE_INTERVAL, updateIntervalSeconds);
    conf.setLong(DelegationTokenManager.REMOVAL_SCAN_INTERVAL, updateIntervalSeconds);
    conf.setLong(DelegationTokenManager.RENEW_INTERVAL, updateIntervalSeconds);

    conf.setLong(ZKDelegationTokenSecretManager.ZK_DTSM_ZK_SHUTDOWN_TIMEOUT, shutdownTimeoutMillis);
    tm1 = new DelegationTokenManager(conf, new Text("foo"));
    tm1.init();

    Token<DelegationTokenIdentifier> token =
      (Token<DelegationTokenIdentifier>)
    tm1.createToken(UserGroupInformation.getCurrentUser(), "foo");
    Assert.assertNotNull(token);

    AbstractDelegationTokenSecretManager sm = tm1.getDelegationTokenSecretManager();
    ZKDelegationTokenSecretManager zksm = (ZKDelegationTokenSecretManager)sm;
    ExecutorService es = zksm.getListenerThreadPool();
    es.submit(new Callable<Void>() {
      public Void call() throws Exception {
        Thread.sleep(shutdownTimeoutMillis * 2); // force this to be shutdownNow
        return null;
      }
    });

    tm1.destroy();
  }

  // Since it is possible that there can be a delay for the cancel token message
  // initiated by one node to reach another node.. The second node can ofcourse
  // verify with ZK directly if the token that needs verification has been
  // cancelled but.. that would mean having to make an RPC call for every
  // verification request.
  // Thus, the eventual consistency tradef-off should be acceptable here...
  private void verifyTokenFail(DelegationTokenManager tm,
      Token<DelegationTokenIdentifier> token) throws IOException,
      InterruptedException {
    verifyTokenFailWithRetry(tm, token, RETRY_COUNT);
  }

  private void verifyTokenFailWithRetry(DelegationTokenManager tm,
      Token<DelegationTokenIdentifier> token, int retryCount)
      throws IOException, InterruptedException {
    try {
      tm.verifyToken(token);
    } catch (SecretManager.InvalidToken er) {
      throw er;
    }
    if (retryCount > 0) {
      Thread.sleep(RETRY_WAIT);
      verifyTokenFailWithRetry(tm, token, retryCount - 1);
    }
  }

}
