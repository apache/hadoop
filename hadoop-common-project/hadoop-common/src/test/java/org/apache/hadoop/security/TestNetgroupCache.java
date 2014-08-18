/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;

public class TestNetgroupCache {

  private static final String USER1 = "user1";
  private static final String USER2 = "user2";
  private static final String USER3 = "user3";
  private static final String GROUP1 = "group1";
  private static final String GROUP2 = "group2";

  @After
  public void teardown() {
    NetgroupCache.clear();
  }

  /**
   * Cache two groups with a set of users.
   * Test membership correctness.
   */
  @Test
  public void testMembership() {
    List<String> users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER2);
    NetgroupCache.add(GROUP1, users);
    users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER3);
    NetgroupCache.add(GROUP2, users);
    verifyGroupMembership(USER1, 2, GROUP1);
    verifyGroupMembership(USER1, 2, GROUP2);
    verifyGroupMembership(USER2, 1, GROUP1);
    verifyGroupMembership(USER3, 1, GROUP2);
  }

  /**
   * Cache a group with a set of users.
   * Test membership correctness.
   * Clear cache, remove a user from the group and cache the group
   * Test membership correctness.
   */
  @Test
  public void testUserRemoval() {
    List<String> users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER2);
    NetgroupCache.add(GROUP1, users);
    verifyGroupMembership(USER1, 1, GROUP1);
    verifyGroupMembership(USER2, 1, GROUP1);
    users.remove(USER2);
    NetgroupCache.clear();
    NetgroupCache.add(GROUP1, users);
    verifyGroupMembership(USER1, 1, GROUP1);
    verifyGroupMembership(USER2, 0, null);
  }

  /**
   * Cache two groups with a set of users.
   * Test membership correctness.
   * Clear cache, cache only one group.
   * Test membership correctness.
   */
  @Test
  public void testGroupRemoval() {
    List<String> users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER2);
    NetgroupCache.add(GROUP1, users);
    users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER3);
    NetgroupCache.add(GROUP2, users);
    verifyGroupMembership(USER1, 2, GROUP1);
    verifyGroupMembership(USER1, 2, GROUP2);
    verifyGroupMembership(USER2, 1, GROUP1);
    verifyGroupMembership(USER3, 1, GROUP2);
    NetgroupCache.clear();
    users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER2);
    NetgroupCache.add(GROUP1, users);
    verifyGroupMembership(USER1, 1, GROUP1);
    verifyGroupMembership(USER2, 1, GROUP1);
    verifyGroupMembership(USER3, 0, null);
  }

  private void verifyGroupMembership(String user, int size, String group) {
    List<String> groups = new ArrayList<String>();
    NetgroupCache.getNetgroups(user, groups);
    assertEquals(size, groups.size());
    if (size > 0) {
      boolean present = false;
      for (String groupEntry:groups) {
        if (groupEntry.equals(group)) {
          present = true;
          break;
        }
      }
      assertTrue(present);
    }
  }
}
