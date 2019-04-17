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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreTestBase.TestDispatcher;
import org.apache.hadoop.util.ZKUtil;

import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestZKRMStateStoreZKClientConnections {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestZKRMStateStoreZKClientConnections.class);

  private static final int ZK_TIMEOUT_MS = 1000;
  private static final String DIGEST_USER_PASS="test-user:test-password";
  private static final String TEST_AUTH_GOOD = "digest:" + DIGEST_USER_PASS;
  private static final String DIGEST_USER_HASH;
  static {
    try {
      DIGEST_USER_HASH = DigestAuthenticationProvider.generateDigest(
          DIGEST_USER_PASS);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
  private static final String TEST_ACL = "digest:" + DIGEST_USER_HASH + ":rwcda";

  private TestingServer testingServer;

  @Before
  public void setupZKServer() throws Exception {
    testingServer = new TestingServer();
    testingServer.start();
  }

  @After
  public void cleanupZKServer() throws Exception {
    testingServer.stop();
  }

  class TestZKClient {

    ZKRMStateStore store;

    protected class TestZKRMStateStore extends ZKRMStateStore {

      public TestZKRMStateStore(Configuration conf, String workingZnode)
          throws Exception {
        setResourceManager(new ResourceManager());
        init(conf);
        start();
        assertTrue(znodeWorkingPath.equals(workingZnode));
      }
    }

    public RMStateStore getRMStateStore(Configuration conf) throws Exception {
      String workingZnode = "/Test";
      conf.set(CommonConfigurationKeys.ZK_ADDRESS,
          testingServer.getConnectString());
      conf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, workingZnode);
      this.store = new TestZKRMStateStore(conf, workingZnode);
      return this.store;
    }
  }

  @Test (timeout = 20000)
  public void testZKClientRetry() throws Exception {
    TestZKClient zkClientTester = new TestZKClient();
    final String path = "/test";
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(CommonConfigurationKeys.ZK_TIMEOUT_MS, ZK_TIMEOUT_MS);
    conf.setLong(CommonConfigurationKeys.ZK_RETRY_INTERVAL_MS, 100);
    final ZKRMStateStore store =
        (ZKRMStateStore) zkClientTester.getRMStateStore(conf);
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);
    final AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);

    testingServer.stop();
    Thread clientThread = new Thread() {
      @Override
      public void run() {
        try {
          store.getData(path);
        } catch (Exception e) {
          e.printStackTrace();
          assertionFailedInThread.set(true);
        }
      }
    };
    Thread.sleep(2000);
    testingServer.start();
    clientThread.join();
    Assert.assertFalse(assertionFailedInThread.get());
  }

  @Test(timeout = 20000)
  public void testSetZKAcl() {
    TestZKClient zkClientTester = new TestZKClient();
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(CommonConfigurationKeys.ZK_ACL, "world:anyone:rwca");
    try {
      zkClientTester.store.delete(zkClientTester.store
          .znodeWorkingPath);
      fail("Shouldn't be able to delete path");
    } catch (Exception e) {/* expected behavior */
    }
  }

  @Test(timeout = 20000)
  public void testInvalidZKAclConfiguration() {
    TestZKClient zkClientTester = new TestZKClient();
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(CommonConfigurationKeys.ZK_ACL, "randomstring&*");
    try {
      zkClientTester.getRMStateStore(conf);
      fail("ZKRMStateStore created with bad ACL");
    } catch (ZKUtil.BadAclFormatException bafe) {
      // expected behavior
    } catch (Exception e) {
      String error = "Incorrect exception on BadAclFormat";
      LOG.error(error, e);
      fail(error);
    }
  }

  @Test
  public void testZKAuths() throws Exception {
    TestZKClient zkClientTester = new TestZKClient();
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(CommonConfigurationKeys.ZK_NUM_RETRIES, 1);
    conf.setInt(CommonConfigurationKeys.ZK_TIMEOUT_MS, ZK_TIMEOUT_MS);
    conf.set(CommonConfigurationKeys.ZK_ACL, TEST_ACL);
    conf.set(CommonConfigurationKeys.ZK_AUTH, TEST_AUTH_GOOD);

    zkClientTester.getRMStateStore(conf);
  }
}
