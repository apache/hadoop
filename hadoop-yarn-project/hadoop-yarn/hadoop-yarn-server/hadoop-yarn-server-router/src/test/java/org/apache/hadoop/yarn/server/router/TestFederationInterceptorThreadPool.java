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

package org.apache.hadoop.yarn.server.router;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.router.clientrm.FederationClientInterceptor;
import org.apache.hadoop.yarn.server.router.rmadmin.FederationRMAdminInterceptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFederationInterceptorThreadPool {

  @ParameterizedTest
  @CsvSource({", 0", "0s, 0", "30s, 30000", "1m, 60000",
      "500ms, 500", "60, 60000"})
  public void testKeepAliveTime(String configuredValue, long expectedMillis) {
    Configuration conf = new Configuration(false);
    if (configuredValue != null) {
      conf.set(YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_KEEP_ALIVE_TIME,
          configuredValue);
    }
    assertKeepAliveTime(conf, expectedMillis);
  }

  @Test
  public void testDefaultKeepAliveTime() {
    assertKeepAliveTime(new YarnConfiguration(), 30000);
  }

  private void assertKeepAliveTime(Configuration conf, long expectedMillis) {
    conf.set(YarnConfiguration.FEDERATION_STATESTORE_CLIENT_CLASS,
        MemoryFederationStateStore.class.getName());
    conf.setBoolean(
        YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_ALLOW_CORE_THREAD_TIMEOUT, true);
    FederationClientInterceptor client = new FederationClientInterceptor();
    FederationRMAdminInterceptor admin = new FederationRMAdminInterceptor();
    client.setConf(conf);
    admin.setConf(conf);
    try {
      client.init("test-user");
      admin.init("test-user");
      assertEquals(expectedMillis,
          client.getExecutorService().getKeepAliveTime(TimeUnit.MILLISECONDS));
      assertEquals(expectedMillis,
          admin.getExecutorService().getKeepAliveTime(TimeUnit.MILLISECONDS));
      assertEquals(expectedMillis > 0,
          client.getExecutorService().allowsCoreThreadTimeOut());
      assertEquals(expectedMillis > 0,
          admin.getExecutorService().allowsCoreThreadTimeOut());
    } finally {
      if (client.getExecutorService() != null) {
        client.getExecutorService().shutdownNow();
      }
      if (admin.getExecutorService() != null) {
        admin.getExecutorService().shutdownNow();
      }
    }
  }
}
