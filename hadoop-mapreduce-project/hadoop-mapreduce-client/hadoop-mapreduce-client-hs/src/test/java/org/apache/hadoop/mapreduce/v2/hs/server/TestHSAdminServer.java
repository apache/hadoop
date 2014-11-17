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

package org.apache.hadoop.mapreduce.v2.hs.server;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;
import org.apache.hadoop.mapreduce.v2.hs.client.HSAdmin;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogDeletionService;

@RunWith(Parameterized.class)
public class TestHSAdminServer {
  private boolean securityEnabled = true;
  private HSAdminServer hsAdminServer = null;
  private HSAdmin hsAdminClient = null;
  JobConf conf = null;
  private static long groupRefreshTimeoutSec = 1;
  JobHistory jobHistoryService = null;
  AggregatedLogDeletionService alds = null;

  public static class MockUnixGroupsMapping implements
      GroupMappingServiceProvider {
    private int i = 0;

    @Override
    public List<String> getGroups(String user) throws IOException {
      System.out.println("Getting groups in MockUnixGroupsMapping");
      String g1 = user + (10 * i + 1);
      String g2 = user + (10 * i + 2);
      List<String> l = new ArrayList<String>(2);
      l.add(g1);
      l.add(g2);
      i++;
      return l;
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
      System.out.println("Refreshing groups in MockUnixGroupsMapping");
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
    }
  }

  @Parameters
  public static Collection<Object[]> testParameters() {
    return Arrays.asList(new Object[][] { { false }, { true } });
  }

  public TestHSAdminServer(boolean enableSecurity) {
    securityEnabled = enableSecurity;
  }

  @Before
  public void init() throws HadoopIllegalArgumentException, IOException {
    conf = new JobConf();
    conf.set(JHAdminConfig.JHS_ADMIN_ADDRESS, "0.0.0.0:0");
    conf.setClass("hadoop.security.group.mapping", MockUnixGroupsMapping.class,
        GroupMappingServiceProvider.class);
    conf.setLong("hadoop.security.groups.cache.secs", groupRefreshTimeoutSec);
    conf.setBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
          securityEnabled);
    Groups.getUserToGroupsMappingService(conf);
    jobHistoryService = mock(JobHistory.class);
    alds = mock(AggregatedLogDeletionService.class);

    hsAdminServer = new HSAdminServer(alds, jobHistoryService) {

      @Override
      protected Configuration createConf() {
        return conf;
      }
    };
    hsAdminServer.init(conf);
    hsAdminServer.start();
    conf.setSocketAddr(JHAdminConfig.JHS_ADMIN_ADDRESS,
        hsAdminServer.clientRpcServer.getListenerAddress());
    hsAdminClient = new HSAdmin(conf);
  }

  @Test
  public void testGetGroups() throws Exception {
    // Get the current user
    String user = UserGroupInformation.getCurrentUser().getUserName();
    String[] args = new String[2];
    args[0] = "-getGroups";
    args[1] = user;
    // Run the getGroups command
    int exitCode = hsAdminClient.run(args);
    assertEquals("Exit code should be 0 but was: " + exitCode, 0, exitCode);
  }

  @Test
  public void testRefreshUserToGroupsMappings() throws Exception {

    String[] args = new String[] { "-refreshUserToGroupsMappings" };
    Groups groups = Groups.getUserToGroupsMappingService(conf);
    String user = UserGroupInformation.getCurrentUser().getUserName();
    System.out.println("first attempt:");
    List<String> g1 = groups.getGroups(user);
    String[] str_groups = new String[g1.size()];
    g1.toArray(str_groups);
    System.out.println(Arrays.toString(str_groups));

    // Now groups of this user has changed but getGroups returns from the
    // cache,so we would see same groups as before
    System.out.println("second attempt, should be same:");
    List<String> g2 = groups.getGroups(user);
    g2.toArray(str_groups);
    System.out.println(Arrays.toString(str_groups));
    for (int i = 0; i < g2.size(); i++) {
      assertEquals("Should be same group ", g1.get(i), g2.get(i));
    }
    // run the command,which clears the cache
    hsAdminClient.run(args);
    System.out
        .println("third attempt(after refresh command), should be different:");
    // Now get groups should return new groups
    List<String> g3 = groups.getGroups(user);
    g3.toArray(str_groups);
    System.out.println(Arrays.toString(str_groups));
    for (int i = 0; i < g3.size(); i++) {
      assertFalse(
          "Should be different group: " + g1.get(i) + " and " + g3.get(i), g1
              .get(i).equals(g3.get(i)));
    }
  }

  @Test
  public void testRefreshSuperUserGroups() throws Exception {

    UserGroupInformation ugi = mock(UserGroupInformation.class);
    UserGroupInformation superUser = mock(UserGroupInformation.class);

    when(ugi.getRealUser()).thenReturn(superUser);
    when(superUser.getShortUserName()).thenReturn("superuser");
    when(superUser.getUserName()).thenReturn("superuser");
    when(ugi.getGroupNames()).thenReturn(new String[] { "group3" });
    when(ugi.getUserName()).thenReturn("regularUser");

    // Set super user groups not to include groups of regularUser
    conf.set("hadoop.proxyuser.superuser.groups", "group1,group2");
    conf.set("hadoop.proxyuser.superuser.hosts", "127.0.0.1");
    String[] args = new String[1];
    args[0] = "-refreshSuperUserGroupsConfiguration";
    hsAdminClient.run(args);

    Throwable th = null;
    try {
      ProxyUsers.authorize(ugi, "127.0.0.1");
    } catch (Exception e) {
      th = e;
    }
    // Exception should be thrown
    assertTrue(th instanceof AuthorizationException);

    // Now add regularUser group to superuser group but not execute
    // refreshSuperUserGroupMapping
    conf.set("hadoop.proxyuser.superuser.groups", "group1,group2,group3");

    // Again,lets run ProxyUsers.authorize and see if regularUser can be
    // impersonated
    // resetting th
    th = null;
    try {
      ProxyUsers.authorize(ugi, "127.0.0.1");
    } catch (Exception e) {
      th = e;
    }
    // Exception should be thrown again since we didn't refresh the configs
    assertTrue(th instanceof AuthorizationException);

    // Lets refresh the config by running refreshSuperUserGroupsConfiguration
    hsAdminClient.run(args);

    th = null;

    try {
      ProxyUsers.authorize(ugi, "127.0.0.1");
    } catch (Exception e) {
      th = e;
    }
    // No exception thrown since regularUser can be impersonated.
    assertNull("Unexpected exception thrown: " + th, th);

  }

  @Test
  public void testRefreshAdminAcls() throws Exception {
    // Setting current user to admin acl
    conf.set(JHAdminConfig.JHS_ADMIN_ACL, UserGroupInformation.getCurrentUser()
        .getUserName());
    String[] args = new String[1];
    args[0] = "-refreshAdminAcls";
    hsAdminClient.run(args);

    // Now I should be able to run any hsadmin command without any exception
    // being thrown
    args[0] = "-refreshSuperUserGroupsConfiguration";
    hsAdminClient.run(args);

    // Lets remove current user from admin acl
    conf.set(JHAdminConfig.JHS_ADMIN_ACL, "notCurrentUser");
    args[0] = "-refreshAdminAcls";
    hsAdminClient.run(args);

    // Now I should get an exception if i run any hsadmin command
    Throwable th = null;
    args[0] = "-refreshSuperUserGroupsConfiguration";
    try {
      hsAdminClient.run(args);
    } catch (Exception e) {
      th = e;
    }
    assertTrue(th instanceof RemoteException);
  }

  @Test
  public void testRefreshLoadedJobCache() throws Exception {
    String[] args = new String[1];
    args[0] = "-refreshLoadedJobCache";
    hsAdminClient.run(args);
    verify(jobHistoryService).refreshLoadedJobCache();
  }
  
  @Test
  public void testRefreshLogRetentionSettings() throws Exception {
    String[] args = new String[1];
    args[0] = "-refreshLogRetentionSettings";
    hsAdminClient.run(args);
    verify(alds).refreshLogRetentionSettings();
  }

  @Test
  public void testRefreshJobRetentionSettings() throws Exception {
    String[] args = new String[1];
    args[0] = "-refreshJobRetentionSettings";
    hsAdminClient.run(args);
    verify(jobHistoryService).refreshJobRetentionSettings();
  }

  @After
  public void cleanUp() {
    if (hsAdminServer != null)
      hsAdminServer.stop();
  }

}
