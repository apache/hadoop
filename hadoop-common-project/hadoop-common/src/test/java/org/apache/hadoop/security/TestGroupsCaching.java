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
package org.apache.hadoop.security;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;


public class TestGroupsCaching {
  public static final Log LOG = LogFactory.getLog(TestGroupsCaching.class);
  private static String[] myGroups = {"grp1", "grp2"};
  private Configuration conf;

  @Before
  public void setup() {
    conf = new Configuration();
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
      FakeGroupMapping.class,
      ShellBasedUnixGroupsMapping.class);
  }

  public static class FakeGroupMapping extends ShellBasedUnixGroupsMapping {
    // any to n mapping
    private static Set<String> allGroups = new HashSet<String>();
    private static Set<String> blackList = new HashSet<String>();

    @Override
    public List<String> getGroups(String user) throws IOException {
      LOG.info("Getting groups for " + user);
      if (blackList.contains(user)) {
        return new LinkedList<String>();
      }
      return new LinkedList<String>(allGroups);
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
      LOG.info("Cache is being refreshed.");
      clearBlackList();
      return;
    }

    public static void clearBlackList() throws IOException {
      LOG.info("Clearing the blacklist");
      blackList.clear();
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
      LOG.info("Adding " + groups + " to groups.");
      allGroups.addAll(groups);
    }

    public static void addToBlackList(String user) throws IOException {
      LOG.info("Adding " + user + " to the blacklist");
      blackList.add(user);
    }
  }

  @Test
  public void testGroupsCaching() throws Exception {
    Groups groups = new Groups(conf);
    groups.cacheGroupsAdd(Arrays.asList(myGroups));
    groups.refresh();
    FakeGroupMapping.clearBlackList();
    FakeGroupMapping.addToBlackList("user1");

    // regular entry
    assertTrue(groups.getGroups("me").size() == 2);

    // this must be cached. blacklisting should have no effect.
    FakeGroupMapping.addToBlackList("me");
    assertTrue(groups.getGroups("me").size() == 2);

    // ask for a negative entry
    try {
      LOG.error("We are not supposed to get here." + groups.getGroups("user1").toString());
      fail();
    } catch (IOException ioe) {
      if(!ioe.getMessage().startsWith("No groups found")) {
        LOG.error("Got unexpected exception: " + ioe.getMessage());
        fail();
      }
    }

    // this shouldn't be cached. remove from the black list and retry.
    FakeGroupMapping.clearBlackList();
    assertTrue(groups.getGroups("user1").size() == 2);
  }

  public static class FakeunPrivilegedGroupMapping extends FakeGroupMapping {
    private static boolean invoked = false;
    @Override
    public List<String> getGroups(String user) throws IOException {
      invoked = true;
      return super.getGroups(user);
    }
  }

  /*
   * Group lookup should not happen for static users
   */
  @Test
  public void testGroupLookupForStaticUsers() throws Exception {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        FakeunPrivilegedGroupMapping.class, ShellBasedUnixGroupsMapping.class);
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "me=;user1=group1;user2=group1,group2");
    Groups groups = new Groups(conf);
    List<String> userGroups = groups.getGroups("me");
    assertTrue("non-empty groups for static user", userGroups.isEmpty());
    assertFalse("group lookup done for static user",
        FakeunPrivilegedGroupMapping.invoked);
    
    List<String> expected = new ArrayList<String>();
    expected.add("group1");

    FakeunPrivilegedGroupMapping.invoked = false;
    userGroups = groups.getGroups("user1");
    assertTrue("groups not correct", expected.equals(userGroups));
    assertFalse("group lookup done for unprivileged user",
        FakeunPrivilegedGroupMapping.invoked);

    expected.add("group2");
    FakeunPrivilegedGroupMapping.invoked = false;
    userGroups = groups.getGroups("user2");
    assertTrue("groups not correct", expected.equals(userGroups));
    assertFalse("group lookup done for unprivileged user",
        FakeunPrivilegedGroupMapping.invoked);

  }
}
