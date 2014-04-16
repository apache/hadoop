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
package org.apache.hadoop.security.authorize;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.Test;
import static org.junit.Assert.*;


public class TestProxyUsers {
  private static final Log LOG =
    LogFactory.getLog(TestProxyUsers.class);
  private static final String REAL_USER_NAME = "proxier";
  private static final String PROXY_USER_NAME = "proxied_user";
  private static final String[] GROUP_NAMES =
    new String[] { "foo_group" };
  private static final String[] NETGROUP_NAMES =
    new String[] { "@foo_group" };
  private static final String[] OTHER_GROUP_NAMES =
    new String[] { "bar_group" };
  private static final String PROXY_IP = "1.2.3.4";

  /**
   * Test the netgroups (groups in ACL rules that start with @)
   *
   * This is a  manual test because it requires:
   *   - host setup
   *   - native code compiled
   *   - specify the group mapping class
   *
   * Host setup:
   *
   * /etc/nsswitch.conf should have a line like this:
   * netgroup: files
   *
   * /etc/netgroup should be (the whole file):
   * foo_group (,proxied_user,)
   *
   * To run this test:
   *
   * export JAVA_HOME='path/to/java'
   * mvn test \
   *   -Dtest=TestProxyUsers \
   *   -DTestProxyUsersGroupMapping=$className \
   *   
   * where $className is one of the classes that provide group
   * mapping services, i.e. classes that implement
   * GroupMappingServiceProvider interface, at this time:
   *   - org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMapping
   *   - org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping
   *
   */
  
  @Test
  public void testNetgroups () throws IOException{
  
    if(!NativeCodeLoader.isNativeCodeLoaded()) {
      LOG.info("Not testing netgroups, " +
        "this test only runs when native code is compiled");
      return;
    }

    String groupMappingClassName =
      System.getProperty("TestProxyUsersGroupMapping");

    if(groupMappingClassName == null) {
      LOG.info("Not testing netgroups, no group mapping class specified, " +
        "use -DTestProxyUsersGroupMapping=$className to specify " +
        "group mapping class (must implement GroupMappingServiceProvider " +
        "interface and support netgroups)");
      return;
    }

    LOG.info("Testing netgroups using: " + groupMappingClassName);

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUP_MAPPING,
      groupMappingClassName);

    conf.set(
        ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_NAME),
        StringUtils.join(",", Arrays.asList(NETGROUP_NAMES)));
    conf.set(
        ProxyUsers.getProxySuperuserIpConfKey(REAL_USER_NAME),
        PROXY_IP);
    
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    Groups groups = Groups.getUserToGroupsMappingService(conf);

    // try proxying a group that's allowed
    UserGroupInformation realUserUgi = UserGroupInformation
    .createRemoteUser(REAL_USER_NAME);

    UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, groups.getGroups(PROXY_USER_NAME).toArray(
            new String[groups.getGroups(PROXY_USER_NAME).size()]));

    assertAuthorized(proxyUserUgi, PROXY_IP);
  }

  @Test
  public void testProxyUsers() throws Exception {
    Configuration conf = new Configuration();
    conf.set(
      ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_NAME),
      StringUtils.join(",", Arrays.asList(GROUP_NAMES)));
    conf.set(
      ProxyUsers.getProxySuperuserIpConfKey(REAL_USER_NAME),
      PROXY_IP);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);


    // First try proxying a group that's allowed
    UserGroupInformation realUserUgi = UserGroupInformation
        .createRemoteUser(REAL_USER_NAME);
    UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, GROUP_NAMES);

    // From good IP
    assertAuthorized(proxyUserUgi, "1.2.3.4");
    // From bad IP
    assertNotAuthorized(proxyUserUgi, "1.2.3.5");

    // Now try proxying a group that's not allowed
    realUserUgi = UserGroupInformation.createRemoteUser(REAL_USER_NAME);
    proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, OTHER_GROUP_NAMES);
    
    // From good IP
    assertNotAuthorized(proxyUserUgi, "1.2.3.4");
    // From bad IP
    assertNotAuthorized(proxyUserUgi, "1.2.3.5");
  }

  @Test
  public void testWildcardGroup() {
    Configuration conf = new Configuration();
    conf.set(
      ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_NAME),
      "*");
    conf.set(
      ProxyUsers.getProxySuperuserIpConfKey(REAL_USER_NAME),
      PROXY_IP);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);

    // First try proxying a group that's allowed
    UserGroupInformation realUserUgi = UserGroupInformation
        .createRemoteUser(REAL_USER_NAME);
    UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, GROUP_NAMES);

    // From good IP
    assertAuthorized(proxyUserUgi, "1.2.3.4");
    // From bad IP
    assertNotAuthorized(proxyUserUgi, "1.2.3.5");

    // Now try proxying a different group (just to make sure we aren't getting spill over
    // from the other test case!)
    realUserUgi = UserGroupInformation.createRemoteUser(REAL_USER_NAME);
    proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, OTHER_GROUP_NAMES);
    
    // From good IP
    assertAuthorized(proxyUserUgi, "1.2.3.4");
    // From bad IP
    assertNotAuthorized(proxyUserUgi, "1.2.3.5");
  }

  @Test
  public void testWildcardIP() {
    Configuration conf = new Configuration();
    conf.set(
      ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_NAME),
      StringUtils.join(",", Arrays.asList(GROUP_NAMES)));
    conf.set(
      ProxyUsers.getProxySuperuserIpConfKey(REAL_USER_NAME),
      "*");
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);

    // First try proxying a group that's allowed
    UserGroupInformation realUserUgi = UserGroupInformation
        .createRemoteUser(REAL_USER_NAME);
    UserGroupInformation proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, GROUP_NAMES);

    // From either IP should be fine
    assertAuthorized(proxyUserUgi, "1.2.3.4");
    assertAuthorized(proxyUserUgi, "1.2.3.5");

    // Now set up an unallowed group
    realUserUgi = UserGroupInformation.createRemoteUser(REAL_USER_NAME);
    proxyUserUgi = UserGroupInformation.createProxyUserForTesting(
        PROXY_USER_NAME, realUserUgi, OTHER_GROUP_NAMES);
    
    // Neither IP should be OK
    assertNotAuthorized(proxyUserUgi, "1.2.3.4");
    assertNotAuthorized(proxyUserUgi, "1.2.3.5");
  }

  @Test
  public void testWithDuplicateProxyGroups() throws Exception {
    Configuration conf = new Configuration();
    conf.set(
      ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_NAME),
      StringUtils.join(",", Arrays.asList(GROUP_NAMES,GROUP_NAMES)));
    conf.set(
      ProxyUsers.getProxySuperuserIpConfKey(REAL_USER_NAME),
      PROXY_IP);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    
    Collection<String> groupsToBeProxied = ProxyUsers.getProxyGroups().get(
        ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_NAME));
    
    assertEquals (1,groupsToBeProxied.size());
  }
  
  @Test
  public void testWithDuplicateProxyHosts() throws Exception {
    Configuration conf = new Configuration();
    conf.set(
      ProxyUsers.getProxySuperuserGroupConfKey(REAL_USER_NAME),
      StringUtils.join(",", Arrays.asList(GROUP_NAMES)));
    conf.set(
      ProxyUsers.getProxySuperuserIpConfKey(REAL_USER_NAME),
      StringUtils.join(",", Arrays.asList(PROXY_IP,PROXY_IP)));
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    
    Collection<String> hosts = ProxyUsers.getProxyHosts().get(
        ProxyUsers.getProxySuperuserIpConfKey(REAL_USER_NAME));
    
    assertEquals (1,hosts.size());
  }

  @Test
  public void testProxyServer() {
    Configuration conf = new Configuration();
    assertFalse(ProxyUsers.isProxyServer("1.1.1.1"));
    conf.set(ProxyUsers.CONF_HADOOP_PROXYSERVERS, "2.2.2.2, 3.3.3.3");
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    assertFalse(ProxyUsers.isProxyServer("1.1.1.1"));
    assertTrue(ProxyUsers.isProxyServer("2.2.2.2"));
    assertTrue(ProxyUsers.isProxyServer("3.3.3.3"));
  }

  private void assertNotAuthorized(UserGroupInformation proxyUgi, String host) {
    try {
      ProxyUsers.authorize(proxyUgi, host);
      fail("Allowed authorization of " + proxyUgi + " from " + host);
    } catch (AuthorizationException e) {
      // Expected
    }
  }
  
  private void assertAuthorized(UserGroupInformation proxyUgi, String host) {
    try {
      ProxyUsers.authorize(proxyUgi, host);
    } catch (AuthorizationException e) {
      fail("Did not allowed authorization of " + proxyUgi + " from " + host);
    }
  }
}
