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

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestRMProxyUsersConf {

  private static final UserGroupInformation FOO_USER =
      UserGroupInformation.createUserForTesting("foo", new String[] { "foo_group" });
  private static final UserGroupInformation BAR_USER =
      UserGroupInformation.createUserForTesting("bar", new String[] { "bar_group" });
  private final String ipAddress = "127.0.0.1";

  @Parameterized.Parameters
  public static Collection<Object[]> headers() {
    return Arrays.asList(new Object[][] { { 0 }, { 1 }, { 2 } });
  }

  private Configuration conf;

  public TestRMProxyUsersConf(int round) {
    conf = new YarnConfiguration();
    switch (round) {
      case 0:
        // hadoop.proxyuser prefix
        conf.set("hadoop.proxyuser.foo.hosts", ipAddress);
        conf.set("hadoop.proxyuser.foo.users", "bar");
        conf.set("hadoop.proxyuser.foo.groups", "bar_group");
        break;
      case 1:
        // yarn.resourcemanager.proxyuser prefix
        conf.set("yarn.resourcemanager.proxyuser.foo.hosts", ipAddress);
        conf.set("yarn.resourcemanager.proxyuser.foo.users", "bar");
        conf.set("yarn.resourcemanager.proxyuser.foo.groups", "bar_group");
        break;
      case 2:
        // hadoop.proxyuser prefix has been overwritten by
        // yarn.resourcemanager.proxyuser prefix
        conf.set("hadoop.proxyuser.foo.hosts", "xyz");
        conf.set("hadoop.proxyuser.foo.users", "xyz");
        conf.set("hadoop.proxyuser.foo.groups", "xyz");
        conf.set("yarn.resourcemanager.proxyuser.foo.hosts", ipAddress);
        conf.set("yarn.resourcemanager.proxyuser.foo.users", "bar");
        conf.set("yarn.resourcemanager.proxyuser.foo.groups", "bar_group");
        break;
      default:
        break;
    }
  }

  @Test
  public void testProxyUserConfiguration() throws Exception {
    MockRM rm = null;
    try {
      rm = new MockRM(conf);
      rm.start();
      // wait for web server starting
      Thread.sleep(10000);
      UserGroupInformation proxyUser =
          UserGroupInformation.createProxyUser(
              BAR_USER.getShortUserName(), FOO_USER);
      try {
        ProxyUsers.getDefaultImpersonationProvider().authorize(proxyUser,
            ipAddress);
      } catch (AuthorizationException e) {
        // Exception is not expected
        Assert.fail();
      }
    } finally {
      if (rm != null) {
        rm.stop();
        rm.close();
      }
    }
  }

}
