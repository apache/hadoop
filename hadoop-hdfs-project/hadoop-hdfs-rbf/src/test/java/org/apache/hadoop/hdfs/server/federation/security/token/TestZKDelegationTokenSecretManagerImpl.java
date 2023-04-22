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

package org.apache.hadoop.hdfs.server.federation.security.token;

import static org.apache.hadoop.hdfs.server.federation.router.security.token.ZKDelegationTokenSecretManagerImpl.ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL;
import static org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.ZK_DTSM_TOKEN_WATCHER_ENABLED;
import static org.apache.hadoop.security.token.delegation.web.DelegationTokenManager.REMOVAL_SCAN_INTERVAL;
import static org.apache.hadoop.security.token.delegation.web.DelegationTokenManager.RENEW_INTERVAL;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.security.token.ZKDelegationTokenSecretManagerImpl;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.TestZKDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestZKDelegationTokenSecretManagerImpl
    extends TestZKDelegationTokenSecretManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestZKDelegationTokenSecretManagerImpl.class);

  @SuppressWarnings("unchecked")
  @Test
  public void testMultiNodeOperationWithoutWatch() throws Exception {
    String connectString = zkServer.getConnectString();
    Configuration conf = getSecretConf(connectString);
    // disable watch
    conf.setBoolean(ZK_DTSM_TOKEN_WATCHER_ENABLED, false);
    conf.setInt(ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL, 3);

    for (int i = 0; i < TEST_RETRIES; i++) {
      ZKDelegationTokenSecretManagerImpl dtsm1 =
          new ZKDelegationTokenSecretManagerImpl(conf);
      ZKDelegationTokenSecretManagerImpl dtsm2 =
          new ZKDelegationTokenSecretManagerImpl(conf);
      DelegationTokenManager tm1, tm2;
      tm1 = new DelegationTokenManager(conf, new Text("bla"));
      tm1.setExternalDelegationTokenSecretManager(dtsm1);
      tm2 = new DelegationTokenManager(conf, new Text("bla"));
      tm2.setExternalDelegationTokenSecretManager(dtsm2);

      // common token operation without watchers should still be working
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

      dtsm1.stopThreads();
      dtsm2.stopThreads();
      verifyDestroy(tm1, conf);
      verifyDestroy(tm2, conf);
    }
  }

  @Test
  public void testMultiNodeTokenRemovalShortSyncWithoutWatch()
      throws Exception {
    String connectString = zkServer.getConnectString();
    Configuration conf = getSecretConf(connectString);
    // disable watch
    conf.setBoolean(ZK_DTSM_TOKEN_WATCHER_ENABLED, false);
    // make sync quick
    conf.setInt(ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL, 3);
    // set the renewal window and removal interval to be a
    // short time to trigger the background cleanup
    conf.setInt(RENEW_INTERVAL, 10);
    conf.setInt(REMOVAL_SCAN_INTERVAL, 10);

    for (int i = 0; i < TEST_RETRIES; i++) {
      ZKDelegationTokenSecretManagerImpl dtsm1 =
          new ZKDelegationTokenSecretManagerImpl(conf);
      ZKDelegationTokenSecretManagerImpl dtsm2 =
          new ZKDelegationTokenSecretManagerImpl(conf);
      DelegationTokenManager tm1, tm2;
      tm1 = new DelegationTokenManager(conf, new Text("bla"));
      tm1.setExternalDelegationTokenSecretManager(dtsm1);
      tm2 = new DelegationTokenManager(conf, new Text("bla"));
      tm2.setExternalDelegationTokenSecretManager(dtsm2);

      // time: X
      // token expiry time:
      //   tm1: X + 10
      //   tm2: X + 10
      Token<DelegationTokenIdentifier> token =
          (Token<DelegationTokenIdentifier>) tm1.createToken(
              UserGroupInformation.getCurrentUser(), "foo");
      Assert.assertNotNull(token);
      tm2.verifyToken(token);

      // time: X + 9
      // token expiry time:
      //   tm1: X + 10
      //   tm2: X + 19
      Thread.sleep(9 * 1000);
      tm2.renewToken(token, "foo");
      tm1.verifyToken(token);

      // time: X + 13
      // token expiry time: (sync happened)
      //   tm1: X + 19
      //   tm2: X + 19
      Thread.sleep(4 * 1000);
      tm1.verifyToken(token);
      tm2.verifyToken(token);

      dtsm1.stopThreads();
      dtsm2.stopThreads();
      verifyDestroy(tm1, conf);
      verifyDestroy(tm2, conf);
    }
  }

  // This is very unlikely to happen in real case, but worth putting
  // the case out
  @Test
  public void testMultiNodeTokenRemovalLongSyncWithoutWatch()
      throws Exception {
    String connectString = zkServer.getConnectString();
    Configuration conf = getSecretConf(connectString);
    // disable watch
    conf.setBoolean(ZK_DTSM_TOKEN_WATCHER_ENABLED, false);
    // make sync quick
    conf.setInt(ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL, 20);
    // set the renewal window and removal interval to be a
    // short time to trigger the background cleanup
    conf.setInt(RENEW_INTERVAL, 10);
    conf.setInt(REMOVAL_SCAN_INTERVAL, 10);

    for (int i = 0; i < TEST_RETRIES; i++) {
      ZKDelegationTokenSecretManagerImpl dtsm1 =
          new ZKDelegationTokenSecretManagerImpl(conf);
      ZKDelegationTokenSecretManagerImpl dtsm2 =
          new ZKDelegationTokenSecretManagerImpl(conf);
      ZKDelegationTokenSecretManagerImpl dtsm3 =
          new ZKDelegationTokenSecretManagerImpl(conf);
      DelegationTokenManager tm1, tm2, tm3;
      tm1 = new DelegationTokenManager(conf, new Text("bla"));
      tm1.setExternalDelegationTokenSecretManager(dtsm1);
      tm2 = new DelegationTokenManager(conf, new Text("bla"));
      tm2.setExternalDelegationTokenSecretManager(dtsm2);
      tm3 = new DelegationTokenManager(conf, new Text("bla"));
      tm3.setExternalDelegationTokenSecretManager(dtsm3);

      // time: X
      // token expiry time:
      //   tm1: X + 10
      //   tm2: X + 10
      //   tm3: No token due to no sync
      Token<DelegationTokenIdentifier> token =
          (Token<DelegationTokenIdentifier>) tm1.createToken(
              UserGroupInformation.getCurrentUser(), "foo");
      Assert.assertNotNull(token);
      tm2.verifyToken(token);

      // time: X + 9
      // token expiry time:
      //   tm1: X + 10
      //   tm2: X + 19
      //   tm3: No token due to no sync
      Thread.sleep(9 * 1000);
      long renewalTime = tm2.renewToken(token, "foo");
      LOG.info("Renew for token {} at current time {} renewal time {}",
          token.getIdentifier(), Time.formatTime(Time.now()),
          Time.formatTime(renewalTime));
      tm1.verifyToken(token);

      // time: X + 13
      // token expiry time: (sync din't happen)
      //   tm1: X + 10
      //   tm2: X + 19
      //   tm3: X + 19 due to fetch from zk
      Thread.sleep(4 * 1000);
      tm2.verifyToken(token);
      tm3.verifyToken(token);

      dtsm1.stopThreads();
      dtsm2.stopThreads();
      dtsm3.stopThreads();
      verifyDestroy(tm1, conf);
      verifyDestroy(tm2, conf);
      verifyDestroy(tm3, conf);
    }
  }

}
