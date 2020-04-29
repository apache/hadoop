/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security.authorize;

import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/**
 * Test class for @DefaultImpersonationProvider
 */
public class TestDefaultImpersonationProvider {

  private String proxyUser;
  private String user;
  private DefaultImpersonationProvider provider;
  private UserGroupInformation userGroupInformation = Mockito
      .mock(UserGroupInformation.class);
  private UserGroupInformation realUserUGI = Mockito
      .mock(UserGroupInformation.class);
  private Configuration conf;
  @Rule
  public Timeout globalTimeout = new Timeout(10000);

  @Before
  public void setup() {
    conf = new Configuration();
    provider = new DefaultImpersonationProvider();

    // Setup 3 proxy users
    conf.set("hadoop.proxyuser.fakeuser.groups", "*");
    conf.set("hadoop.proxyuser.fakeuser.hosts", "*");
    conf.set("hadoop.proxyuser.test.user.groups", "*");
    conf.set("hadoop.proxyuser.test.user.hosts", "*");
    conf.set("hadoop.proxyuser.test user2.groups", "*");
    conf.set("hadoop.proxyuser.test user2.hosts", "*");
    provider.setConf(conf);
    provider.init(ProxyUsers.CONF_HADOOP_PROXYUSER);
  }

  @Test
  public void testAuthorizationSuccess() throws AuthorizationException {
    proxyUser = "fakeuser";
    user = "dummyUser";
    when(realUserUGI.getShortUserName()).thenReturn(proxyUser);
    when(userGroupInformation.getRealUser()).thenReturn(realUserUGI);
    provider.authorize(userGroupInformation, "2.2.2.2");

    user = "somerandomuser";
    proxyUser = "test.user";
    when(realUserUGI.getShortUserName()).thenReturn(proxyUser);
    when(userGroupInformation.getRealUser()).thenReturn(realUserUGI);
    provider.authorize(userGroupInformation, "2.2.2.2");
  }

  @Test
  public void testAuthorizationFailure() throws Exception {
    user = "dummyUser";
    proxyUser = "test user2";
    when(realUserUGI.getShortUserName()).thenReturn(proxyUser);
    when(realUserUGI.getUserName()).thenReturn(proxyUser);
    when(userGroupInformation.getUserName()).thenReturn(user);
    when(userGroupInformation.getRealUser()).thenReturn(realUserUGI);
    LambdaTestUtils.intercept(AuthorizationException.class, "User: "
        + proxyUser + " is not allowed to impersonate " + user, () ->
        provider.authorize(userGroupInformation, "2.2.2.2"));
  }

  @After
  public void clear() {
    provider = null;
    conf = null;
    userGroupInformation = null;
    realUserUGI = null;
  }

}
