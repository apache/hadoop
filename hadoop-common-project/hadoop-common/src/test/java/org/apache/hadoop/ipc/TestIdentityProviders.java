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

package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Test;

import java.util.List;
import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.conf.Configuration;

public class TestIdentityProviders {
  public static class FakeSchedulable implements Schedulable {
    public FakeSchedulable() {
    }

    public UserGroupInformation getUserGroupInformation() {
      try {
        return UserGroupInformation.getCurrentUser();
      } catch (IOException e) {
        return null;
      }
    }

    @Override
    public int getPriorityLevel() {
      return 0;
    }
  }

  @Test
  public void testPluggableIdentityProvider() {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.IPC_IDENTITY_PROVIDER_KEY,
      "org.apache.hadoop.ipc.UserIdentityProvider");

    List<IdentityProvider> providers = conf.getInstances(
      CommonConfigurationKeys.IPC_IDENTITY_PROVIDER_KEY,
      IdentityProvider.class);

    assertThat(providers)
        .describedAs("provider list")
        .hasSize(1);

    IdentityProvider ip = providers.get(0);
    assertNotNull(ip);
    assertEquals(ip.getClass(), UserIdentityProvider.class);
  }

  @Test
  public void testUserIdentityProvider() throws Exception {
    UserIdentityProvider uip = new UserIdentityProvider();
    FakeSchedulable fakeSchedulable = new FakeSchedulable();
    String identity = uip.makeIdentity(fakeSchedulable);

    // Get our username
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String username = ugi.getUserName();

    assertEquals(username, identity);

    // FakeSchedulable doesn't override getCallerContext()
    // accessing it should throw an UnsupportedOperationException
    LambdaTestUtils.intercept(UnsupportedOperationException.class,
        "Invalid operation.", fakeSchedulable::getCallerContext);
  }
}
