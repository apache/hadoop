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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;

import java.io.IOException;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Test the behavior when disabling the Router quota.
 */
public class TestDisableRouterQuota {

  private static Router router;

  @BeforeClass
  public static void setUp() throws Exception {
    // Build and start a router
    router = new Router();
    Configuration routerConf = new RouterConfigBuilder()
        .quota(false) //set false to verify the quota disabled in Router
        .rpc()
        .build();
    routerConf.set(RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "0.0.0.0:0");
    router.init(routerConf);
    router.setRouterId("TestRouterId");
    router.start();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (router != null) {
      router.stop();
      router.close();
    }
  }

  @Before
  public void checkDisableQuota() {
    assertFalse(router.isQuotaEnabled());
  }

  @Test
  public void testSetQuota() throws Exception {
    long nsQuota = 1024;
    long ssQuota = 1024;

    try {
      Quota quotaModule = router.getRpcServer().getQuotaModule();
      quotaModule.setQuota("/test", nsQuota, ssQuota, null, false);
      fail("The setQuota call should fail.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "The quota system is disabled in Router.", ioe);
    }
  }

  @Test
  public void testGetQuotaUsage() throws Exception {
    try {
      Quota quotaModule = router.getRpcServer().getQuotaModule();
      quotaModule.getQuotaUsage("/test");
      fail("The getQuotaUsage call should fail.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "The quota system is disabled in Router.", ioe);
    }
  }

  @Test
  public void testGetGlobalQuota() throws Exception {
    LambdaTestUtils.intercept(IOException.class,
        "The quota system is disabled in Router.",
        "The getGlobalQuota call should fail.", () -> {
          Quota quotaModule = router.getRpcServer().getQuotaModule();
          quotaModule.getGlobalQuota("/test");
        });
  }
}
