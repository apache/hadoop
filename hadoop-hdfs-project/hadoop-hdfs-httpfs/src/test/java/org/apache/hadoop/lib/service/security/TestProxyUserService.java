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

package org.apache.hadoop.lib.service.security;

import static org.junit.Assert.assertNotNull;

import java.security.AccessControlException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.lib.server.Server;
import org.apache.hadoop.lib.server.ServiceException;
import org.apache.hadoop.lib.service.Groups;
import org.apache.hadoop.lib.service.ProxyUser;
import org.apache.hadoop.test.HTestCase;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.test.TestException;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class TestProxyUserService extends HTestCase {

  @Test
  @TestDir
  public void service() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                    ProxyUserService.class.getName())));
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    ProxyUser proxyUser = server.get(ProxyUser.class);
    assertNotNull(proxyUser);
    server.destroy();
  }

  @Test
  @TestException(exception = ServiceException.class, msgRegExp = "PRXU02.*")
  @TestDir
  public void wrongConfigGroups() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                    ProxyUserService.class.getName())));
    conf.set("server.proxyuser.foo.hosts", "*");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

  @Test
  @TestException(exception = ServiceException.class, msgRegExp = "PRXU01.*")
  @TestDir
  public void wrongHost() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                    ProxyUserService.class.getName())));
    conf.set("server.proxyuser.foo.hosts", "otherhost");
    conf.set("server.proxyuser.foo.groups", "*");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

  @Test
  @TestException(exception = ServiceException.class, msgRegExp = "PRXU02.*")
  @TestDir
  public void wrongConfigHosts() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                    ProxyUserService.class.getName())));
    conf.set("server.proxyuser.foo.groups", "*");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

  @Test
  @TestDir
  public void validateAnyHostAnyUser() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                    ProxyUserService.class.getName())));
    conf.set("server.proxyuser.foo.hosts", "*");
    conf.set("server.proxyuser.foo.groups", "*");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    ProxyUser proxyUser = server.get(ProxyUser.class);
    assertNotNull(proxyUser);
    proxyUser.validate("foo", "localhost", "bar");
    server.destroy();
  }

  @Test(expected = AccessControlException.class)
  @TestDir
  public void invalidProxyUser() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                    ProxyUserService.class.getName())));
    conf.set("server.proxyuser.foo.hosts", "*");
    conf.set("server.proxyuser.foo.groups", "*");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    ProxyUser proxyUser = server.get(ProxyUser.class);
    assertNotNull(proxyUser);
    proxyUser.validate("bar", "localhost", "foo");
    server.destroy();
  }

  @Test
  @TestDir
  public void validateHost() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                    ProxyUserService.class.getName())));
    conf.set("server.proxyuser.foo.hosts", "localhost");
    conf.set("server.proxyuser.foo.groups", "*");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    ProxyUser proxyUser = server.get(ProxyUser.class);
    assertNotNull(proxyUser);
    proxyUser.validate("foo", "localhost", "bar");
    server.destroy();
  }

  private String getGroup() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName())));
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    Groups groups = server.get(Groups.class);
    List<String> g = groups.getGroups(System.getProperty("user.name"));
    server.destroy();
    return g.get(0);
  }

  @Test
  @TestDir
  public void validateGroup() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                    ProxyUserService.class.getName())));
    conf.set("server.proxyuser.foo.hosts", "*");
    conf.set("server.proxyuser.foo.groups", getGroup());
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    ProxyUser proxyUser = server.get(ProxyUser.class);
    assertNotNull(proxyUser);
    proxyUser.validate("foo", "localhost", System.getProperty("user.name"));
    server.destroy();
  }


  @Test(expected = AccessControlException.class)
  @TestDir
  public void unknownHost() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                    ProxyUserService.class.getName())));
    conf.set("server.proxyuser.foo.hosts", "localhost");
    conf.set("server.proxyuser.foo.groups", "*");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    ProxyUser proxyUser = server.get(ProxyUser.class);
    assertNotNull(proxyUser);
    proxyUser.validate("foo", "unknownhost.bar.foo", "bar");
    server.destroy();
  }

  @Test(expected = AccessControlException.class)
  @TestDir
  public void invalidHost() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                    ProxyUserService.class.getName())));
    conf.set("server.proxyuser.foo.hosts", "localhost");
    conf.set("server.proxyuser.foo.groups", "*");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    ProxyUser proxyUser = server.get(ProxyUser.class);
    assertNotNull(proxyUser);
    proxyUser.validate("foo", "www.yahoo.com", "bar");
    server.destroy();
  }

  @Test(expected = AccessControlException.class)
  @TestDir
  public void invalidGroup() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                    ProxyUserService.class.getName())));
    conf.set("server.proxyuser.foo.hosts", "localhost");
    conf.set("server.proxyuser.foo.groups", "nobody");
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    ProxyUser proxyUser = server.get(ProxyUser.class);
    assertNotNull(proxyUser);
    proxyUser.validate("foo", "localhost", System.getProperty("user.name"));
    server.destroy();
  }
}
