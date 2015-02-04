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

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.FakeTimer;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
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
    FakeGroupMapping.resetRequestCount();
    ExceptionalGroupMapping.resetRequestCount();

    conf = new Configuration();
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
      FakeGroupMapping.class,
      ShellBasedUnixGroupsMapping.class);
  }

  public static class FakeGroupMapping extends ShellBasedUnixGroupsMapping {
    // any to n mapping
    private static Set<String> allGroups = new HashSet<String>();
    private static Set<String> blackList = new HashSet<String>();
    private static int requestCount = 0;
    private static long getGroupsDelayMs = 0;

    @Override
    public List<String> getGroups(String user) throws IOException {
      LOG.info("Getting groups for " + user);
      requestCount++;

      delayIfNecessary();

      if (blackList.contains(user)) {
        return new LinkedList<String>();
      }
      return new LinkedList<String>(allGroups);
    }

    private void delayIfNecessary() {
      if (getGroupsDelayMs > 0) {
        try {
          Thread.sleep(getGroupsDelayMs);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
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

    public static int getRequestCount() {
      return requestCount;
    }

    public static void resetRequestCount() {
      requestCount = 0;
    }

    public static void setGetGroupsDelayMs(long delayMs) {
      getGroupsDelayMs = delayMs;
    }
  }

  public static class ExceptionalGroupMapping extends ShellBasedUnixGroupsMapping {
    private static int requestCount = 0;

    @Override
    public List<String> getGroups(String user) throws IOException {
      requestCount++;
      throw new IOException("For test");
    }

    public static int getRequestCount() {
      return requestCount;
    }

    public static void resetRequestCount() {
      requestCount = 0;
    }
  }

  @Test
  public void testGroupsCaching() throws Exception {
    // Disable negative cache.
    conf.setLong(
        CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS, 0);
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

  @Test
  public void testNegativeGroupCaching() throws Exception {
    final String user = "negcache";
    final String failMessage = "Did not throw IOException: ";
    conf.setLong(
        CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS, 2);
    FakeTimer timer = new FakeTimer();
    Groups groups = new Groups(conf, timer);
    groups.cacheGroupsAdd(Arrays.asList(myGroups));
    groups.refresh();
    FakeGroupMapping.addToBlackList(user);

    // In the first attempt, the user will be put in the negative cache.
    try {
      groups.getGroups(user);
      fail(failMessage + "Failed to obtain groups from FakeGroupMapping.");
    } catch (IOException e) {
      // Expects to raise exception for the first time. But the user will be
      // put into the negative cache
      GenericTestUtils.assertExceptionContains("No groups found for user", e);
    }

    // The second time, the user is in the negative cache.
    try {
      groups.getGroups(user);
      fail(failMessage + "The user is in the negative cache.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("No groups found for user", e);
    }

    // Brings back the backend user-group mapping service.
    FakeGroupMapping.clearBlackList();

    // It should still get groups from the negative cache.
    try {
      groups.getGroups(user);
      fail(failMessage + "The user is still in the negative cache, even " +
          "FakeGroupMapping has resumed.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("No groups found for user", e);
    }

    // Let the elements in the negative cache expire.
    timer.advance(4 * 1000);

    // The groups for the user is expired in the negative cache, a new copy of
    // groups for the user is fetched.
    assertEquals(Arrays.asList(myGroups), groups.getGroups(user));
  }

  @Test
  public void testCachePreventsImplRequest() throws Exception {
    // Disable negative cache.
    conf.setLong(
      CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS, 0);
    Groups groups = new Groups(conf);
    groups.cacheGroupsAdd(Arrays.asList(myGroups));
    groups.refresh();
    FakeGroupMapping.clearBlackList();

    assertEquals(0, FakeGroupMapping.getRequestCount());

    // First call hits the wire
    assertTrue(groups.getGroups("me").size() == 2);
    assertEquals(1, FakeGroupMapping.getRequestCount());

    // Second count hits cache
    assertTrue(groups.getGroups("me").size() == 2);
    assertEquals(1, FakeGroupMapping.getRequestCount());
  }

  @Test
  public void testExceptionsFromImplNotCachedInNegativeCache() {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
      ExceptionalGroupMapping.class,
      ShellBasedUnixGroupsMapping.class);
    conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS, 10000);
    Groups groups = new Groups(conf);
    groups.cacheGroupsAdd(Arrays.asList(myGroups));
    groups.refresh();

    assertEquals(0, ExceptionalGroupMapping.getRequestCount());

    // First call should hit the wire
    try {
      groups.getGroups("anything");
      fail("Should have thrown");
    } catch (IOException e) {
      // okay
    }
    assertEquals(1, ExceptionalGroupMapping.getRequestCount());

    // Second call should hit the wire (no negative caching)
    try {
      groups.getGroups("anything");
      fail("Should have thrown");
    } catch (IOException e) {
      // okay
    }
    assertEquals(2, ExceptionalGroupMapping.getRequestCount());
  }

  @Test
  public void testOnlyOneRequestWhenNoEntryIsCached() throws Exception {
    // Disable negative cache.
    conf.setLong(
      CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS, 0);
    final Groups groups = new Groups(conf);
    groups.cacheGroupsAdd(Arrays.asList(myGroups));
    groups.refresh();
    FakeGroupMapping.clearBlackList();
    FakeGroupMapping.setGetGroupsDelayMs(100);

    ArrayList<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < 10; i++) {
      threads.add(new Thread() {
        public void run() {
          try {
            assertEquals(2, groups.getGroups("me").size());
          } catch (IOException e) {
            fail("Should not happen");
          }
        }
      });
    }

    // We start a bunch of threads who all see no cached value
    for (Thread t : threads) {
      t.start();
    }

    for (Thread t : threads) {
      t.join();
    }

    // But only one thread should have made the request
    assertEquals(1, FakeGroupMapping.getRequestCount());
  }

  @Test
  public void testOnlyOneRequestWhenExpiredEntryExists() throws Exception {
    conf.setLong(
      CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 1);
    FakeTimer timer = new FakeTimer();
    final Groups groups = new Groups(conf, timer);
    groups.cacheGroupsAdd(Arrays.asList(myGroups));
    groups.refresh();
    FakeGroupMapping.clearBlackList();
    FakeGroupMapping.setGetGroupsDelayMs(100);

    // We make an initial request to populate the cache
    groups.getGroups("me");
    int startingRequestCount = FakeGroupMapping.getRequestCount();

    // Then expire that entry
    timer.advance(400 * 1000);
    Thread.sleep(100);

    ArrayList<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < 10; i++) {
      threads.add(new Thread() {
        public void run() {
          try {
            assertEquals(2, groups.getGroups("me").size());
          } catch (IOException e) {
            fail("Should not happen");
          }
        }
      });
    }

    // We start a bunch of threads who all see the cached value
    for (Thread t : threads) {
      t.start();
    }

    for (Thread t : threads) {
      t.join();
    }

    // Only one extra request is made
    assertEquals(startingRequestCount + 1, FakeGroupMapping.getRequestCount());
  }

  @Test
  public void testCacheEntriesExpire() throws Exception {
    conf.setLong(
      CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 1);
    FakeTimer timer = new FakeTimer();
    final Groups groups = new Groups(conf, timer);
    groups.cacheGroupsAdd(Arrays.asList(myGroups));
    groups.refresh();
    FakeGroupMapping.clearBlackList();

    // We make an entry
    groups.getGroups("me");
    int startingRequestCount = FakeGroupMapping.getRequestCount();

    timer.advance(20 * 1000);

    // Cache entry has expired so it results in a new fetch
    groups.getGroups("me");
    assertEquals(startingRequestCount + 1, FakeGroupMapping.getRequestCount());
  }

  @Test
  public void testNegativeCacheClearedOnRefresh() throws Exception {
    conf.setLong(
      CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS, 100);
    final Groups groups = new Groups(conf);
    groups.cacheGroupsAdd(Arrays.asList(myGroups));
    groups.refresh();
    FakeGroupMapping.clearBlackList();
    FakeGroupMapping.addToBlackList("dne");

    try {
      groups.getGroups("dne");
      fail("Should have failed to find this group");
    } catch (IOException e) {
      // pass
    }

    int startingRequestCount = FakeGroupMapping.getRequestCount();

    groups.refresh();
    FakeGroupMapping.addToBlackList("dne");

    try {
      List<String> g = groups.getGroups("dne");
      fail("Should have failed to find this group");
    } catch (IOException e) {
      // pass
    }

    assertEquals(startingRequestCount + 1, FakeGroupMapping.getRequestCount());
  }  

  @Test
  public void testNegativeCacheEntriesExpire() throws Exception {
    conf.setLong(
       CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS, 2);
    FakeTimer timer = new FakeTimer();
    // Ensure that stale entries are removed from negative cache every 2 seconds
    Groups groups = new Groups(conf, timer);
    groups.cacheGroupsAdd(Arrays.asList(myGroups));
    groups.refresh();
    // Add both these users to blacklist so that they
    // can be added to negative cache
    FakeGroupMapping.addToBlackList("user1");
    FakeGroupMapping.addToBlackList("user2");

    // Put user1 in negative cache.
    try {
      groups.getGroups("user1");
      fail("Did not throw IOException : Failed to obtain groups" +
            " from FakeGroupMapping.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("No groups found for user", e);
    }
    // Check if user1 exists in negative cache
    assertTrue(groups.getNegativeCache().contains("user1"));

    // Advance fake timer
    timer.advance(1000);
    // Put user2 in negative cache
    try {
      groups.getGroups("user2");
      fail("Did not throw IOException : Failed to obtain groups" +
            " from FakeGroupMapping.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("No groups found for user", e);
    }
    // Check if user2 exists in negative cache
    assertTrue(groups.getNegativeCache().contains("user2"));

    // Advance timer. Only user2 should be present in negative cache.
    timer.advance(1100);
    assertFalse(groups.getNegativeCache().contains("user1"));
    assertTrue(groups.getNegativeCache().contains("user2"));

    // Advance timer. Even user2 should not be present in negative cache.
    timer.advance(1000);
    assertFalse(groups.getNegativeCache().contains("user2"));
  }
}
