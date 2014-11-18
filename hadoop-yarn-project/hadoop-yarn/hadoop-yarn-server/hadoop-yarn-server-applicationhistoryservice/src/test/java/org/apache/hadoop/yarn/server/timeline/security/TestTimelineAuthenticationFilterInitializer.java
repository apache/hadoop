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

package org.apache.hadoop.yarn.server.timeline.security;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;
import org.mockito.Mockito;


public class TestTimelineAuthenticationFilterInitializer {

  @Test
  public void testProxyUserConfiguration() {
    FilterContainer container = Mockito.mock(FilterContainer.class);
    for (int i = 0; i < 3; ++i) {
      Configuration conf = new YarnConfiguration();
      switch (i) {
        case 0:
          // hadoop.proxyuser prefix
          conf.set("hadoop.proxyuser.foo.hosts", "*");
          conf.set("hadoop.proxyuser.foo.users", "*");
          conf.set("hadoop.proxyuser.foo.groups", "*");
          break;
        case 1:
          // yarn.timeline-service.http-authentication.proxyuser prefix
          conf.set("yarn.timeline-service.http-authentication.proxyuser.foo.hosts",
              "*");
          conf.set("yarn.timeline-service.http-authentication.proxyuser.foo.users",
              "*");
          conf.set("yarn.timeline-service.http-authentication.proxyuser.foo.groups",
              "*");
          break;
        case 2:
          // hadoop.proxyuser prefix has been overwritten by
          // yarn.timeline-service.http-authentication.proxyuser prefix
          conf.set("hadoop.proxyuser.foo.hosts", "bar");
          conf.set("hadoop.proxyuser.foo.users", "bar");
          conf.set("hadoop.proxyuser.foo.groups", "bar");
          conf.set("yarn.timeline-service.http-authentication.proxyuser.foo.hosts",
              "*");
          conf.set("yarn.timeline-service.http-authentication.proxyuser.foo.users",
              "*");
          conf.set("yarn.timeline-service.http-authentication.proxyuser.foo.groups",
              "*");
          break;
        default:
          break;
      }

      TimelineAuthenticationFilterInitializer initializer =
          new TimelineAuthenticationFilterInitializer();
      initializer.initFilter(container, conf);
      Assert.assertEquals(
          "*", initializer.filterConfig.get("proxyuser.foo.hosts"));
      Assert.assertEquals(
          "*", initializer.filterConfig.get("proxyuser.foo.users"));
      Assert.assertEquals(
          "*", initializer.filterConfig.get("proxyuser.foo.groups"));
    }
  }
}
