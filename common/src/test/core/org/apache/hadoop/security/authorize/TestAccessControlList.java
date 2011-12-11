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

import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;


import junit.framework.TestCase;

public class TestAccessControlList extends TestCase {
  
  public void testWildCardAccessControlList() throws Exception {
    AccessControlList acl;
    
    acl = new AccessControlList("*");
    assertTrue(acl.isAllAllowed());
    
    acl = new AccessControlList("  * ");
    assertTrue(acl.isAllAllowed());
    
    acl = new AccessControlList(" *");
    assertTrue(acl.isAllAllowed());
    
    acl = new AccessControlList("*  ");
    assertTrue(acl.isAllAllowed());
  }

  // Check if AccessControlList.toString() works as expected.
  // Also validate if getAclString() for various cases.
  public void testAclString() {
    AccessControlList acl;

    acl = new AccessControlList("*");
    assertTrue(acl.toString().equals("All users are allowed"));
    validateGetAclString(acl);

    acl = new AccessControlList(" ");
    assertTrue(acl.toString().equals("No users are allowed"));

    acl = new AccessControlList("user1,user2");
    assertTrue(acl.toString().equals("Users [user1, user2] are allowed"));
    validateGetAclString(acl);

    acl = new AccessControlList("user1,user2 ");// with space
    assertTrue(acl.toString().equals("Users [user1, user2] are allowed"));
    validateGetAclString(acl);

    acl = new AccessControlList(" group1,group2");
    assertTrue(acl.toString().equals(
        "Members of the groups [group1, group2] are allowed"));
    validateGetAclString(acl);

    acl = new AccessControlList("user1,user2 group1,group2");
    assertTrue(acl.toString().equals(
        "Users [user1, user2] and " +
        "members of the groups [group1, group2] are allowed"));
    validateGetAclString(acl);
  }

  // Validates if getAclString() is working as expected. i.e. if we can build
  // a new ACL instance from the value returned by getAclString().
  private void validateGetAclString(AccessControlList acl) {
    assertTrue(acl.toString().equals(
        new AccessControlList(acl.getAclString()).toString()));
  }

  public void testAccessControlList() throws Exception {
    AccessControlList acl;
    Set<String> users;
    Set<String> groups;
    
    acl = new AccessControlList("drwho tardis");
    users = acl.getUsers();
    assertEquals(users.size(), 1);
    assertEquals(users.iterator().next(), "drwho");
    groups = acl.getGroups();
    assertEquals(groups.size(), 1);
    assertEquals(groups.iterator().next(), "tardis");
    
    acl = new AccessControlList("drwho");
    users = acl.getUsers();
    assertEquals(users.size(), 1);
    assertEquals(users.iterator().next(), "drwho");
    groups = acl.getGroups();
    assertEquals(groups.size(), 0);
    
    acl = new AccessControlList("drwho ");
    users = acl.getUsers();
    assertEquals(users.size(), 1);
    assertEquals(users.iterator().next(), "drwho");
    groups = acl.getGroups();
    assertEquals(groups.size(), 0);
    
    acl = new AccessControlList(" tardis");
    users = acl.getUsers();
    assertEquals(users.size(), 0);
    groups = acl.getGroups();
    assertEquals(groups.size(), 1);
    assertEquals(groups.iterator().next(), "tardis");

    Iterator<String> iter;    
    acl = new AccessControlList("drwho,joe tardis, users");
    users = acl.getUsers();
    assertEquals(users.size(), 2);
    iter = users.iterator();
    assertEquals(iter.next(), "drwho");
    assertEquals(iter.next(), "joe");
    groups = acl.getGroups();
    assertEquals(groups.size(), 2);
    iter = groups.iterator();
    assertEquals(iter.next(), "tardis");
    assertEquals(iter.next(), "users");
  }

  /**
   * Test addUser/Group and removeUser/Group api.
   */
  public void testAddRemoveAPI() {
    AccessControlList acl;
    Set<String> users;
    Set<String> groups;
    acl = new AccessControlList(" ");
    assertEquals(0, acl.getUsers().size());
    assertEquals(0, acl.getGroups().size());
    assertEquals(" ", acl.getAclString());
    
    acl.addUser("drwho");
    users = acl.getUsers();
    assertEquals(users.size(), 1);
    assertEquals(users.iterator().next(), "drwho");
    assertEquals("drwho ", acl.getAclString());
    
    acl.addGroup("tardis");
    groups = acl.getGroups();
    assertEquals(groups.size(), 1);
    assertEquals(groups.iterator().next(), "tardis");
    assertEquals("drwho tardis", acl.getAclString());
    
    acl.addUser("joe");
    acl.addGroup("users");
    users = acl.getUsers();
    assertEquals(users.size(), 2);
    Iterator<String> iter = users.iterator();
    assertEquals(iter.next(), "drwho");
    assertEquals(iter.next(), "joe");
    groups = acl.getGroups();
    assertEquals(groups.size(), 2);
    iter = groups.iterator();
    assertEquals(iter.next(), "tardis");
    assertEquals(iter.next(), "users");
    assertEquals("drwho,joe tardis,users", acl.getAclString());

    acl.removeUser("joe");
    acl.removeGroup("users");
    users = acl.getUsers();
    assertEquals(users.size(), 1);
    assertFalse(users.contains("joe"));
    groups = acl.getGroups();
    assertEquals(groups.size(), 1);
    assertFalse(groups.contains("users"));
    assertEquals("drwho tardis", acl.getAclString());
    
    acl.removeGroup("tardis");
    groups = acl.getGroups();
    assertEquals(0, groups.size());
    assertFalse(groups.contains("tardis"));
    assertEquals("drwho ", acl.getAclString());
    
    acl.removeUser("drwho");
    assertEquals(0, users.size());
    assertFalse(users.contains("drwho"));
    assertEquals(0, acl.getGroups().size());
    assertEquals(0, acl.getUsers().size());
    assertEquals(" ", acl.getAclString());
  }
  
  /**
   * Tests adding/removing wild card as the user/group.
   */
  public void testAddRemoveWildCard() {
    AccessControlList acl = new AccessControlList("drwho tardis");
    
    Throwable th = null;
    try {
      acl.addUser(" * ");
    } catch (Throwable t) {
      th = t;
    }
    assertNotNull(th);
    assertTrue(th instanceof IllegalArgumentException);
    
    th = null;
    try {
      acl.addGroup(" * ");
    } catch (Throwable t) {
      th = t;
    }
    assertNotNull(th);
    assertTrue(th instanceof IllegalArgumentException);
    th = null;
    try {
    acl.removeUser(" * ");
    } catch (Throwable t) {
      th = t;
    }
    assertNotNull(th);
    assertTrue(th instanceof IllegalArgumentException);
    th = null;
    try {
    acl.removeGroup(" * ");
    } catch (Throwable t) {
      th = t;
    }
    assertNotNull(th);
    assertTrue(th instanceof IllegalArgumentException);
  }
  
  /**
   * Tests adding user/group to an wild card acl.
   */
  public void testAddRemoveToWildCardACL() {
    AccessControlList acl = new AccessControlList(" * ");
    assertTrue(acl.isAllAllowed());

    UserGroupInformation drwho =
      UserGroupInformation.createUserForTesting("drwho@APACHE.ORG",
          new String[] { "aliens" });
    UserGroupInformation drwho2 =
      UserGroupInformation.createUserForTesting("drwho2@APACHE.ORG",
          new String[] { "tardis" });

    acl.addUser("drwho");
    assertTrue(acl.isAllAllowed());
    assertFalse(acl.getAclString().contains("drwho"));
    acl.addGroup("tardis");
    assertTrue(acl.isAllAllowed());
    assertFalse(acl.getAclString().contains("tardis"));
   
    acl.removeUser("drwho");
    assertTrue(acl.isAllAllowed());
    assertUserAllowed(drwho, acl);
    acl.removeGroup("tardis");
    assertTrue(acl.isAllAllowed());
    assertUserAllowed(drwho2, acl);
  }

  /**
   * Verify the method isUserAllowed()
   */
  public void testIsUserAllowed() {
    AccessControlList acl;

    UserGroupInformation drwho =
        UserGroupInformation.createUserForTesting("drwho@APACHE.ORG",
            new String[] { "aliens", "humanoids", "timelord" });
    UserGroupInformation susan =
        UserGroupInformation.createUserForTesting("susan@APACHE.ORG",
            new String[] { "aliens", "humanoids", "timelord" });
    UserGroupInformation barbara =
        UserGroupInformation.createUserForTesting("barbara@APACHE.ORG",
            new String[] { "humans", "teachers" });
    UserGroupInformation ian =
        UserGroupInformation.createUserForTesting("ian@APACHE.ORG",
            new String[] { "humans", "teachers" });

    acl = new AccessControlList("drwho humanoids");
    assertUserAllowed(drwho, acl);
    assertUserAllowed(susan, acl);
    assertUserNotAllowed(barbara, acl);
    assertUserNotAllowed(ian, acl);

    acl = new AccessControlList("drwho");
    assertUserAllowed(drwho, acl);
    assertUserNotAllowed(susan, acl);
    assertUserNotAllowed(barbara, acl);
    assertUserNotAllowed(ian, acl);

    acl = new AccessControlList("drwho ");
    assertUserAllowed(drwho, acl);
    assertUserNotAllowed(susan, acl);
    assertUserNotAllowed(barbara, acl);
    assertUserNotAllowed(ian, acl);

    acl = new AccessControlList(" humanoids");
    assertUserAllowed(drwho, acl);
    assertUserAllowed(susan, acl);
    assertUserNotAllowed(barbara, acl);
    assertUserNotAllowed(ian, acl);

    acl = new AccessControlList("drwho,ian aliens,teachers");
    assertUserAllowed(drwho, acl);
    assertUserAllowed(susan, acl);
    assertUserAllowed(barbara, acl);
    assertUserAllowed(ian, acl);
  }

  private void assertUserAllowed(UserGroupInformation ugi,
      AccessControlList acl) {
    assertTrue("User " + ugi + " is not granted the access-control!!",
        acl.isUserAllowed(ugi));
  }

  private void assertUserNotAllowed(UserGroupInformation ugi,
      AccessControlList acl) {
    assertFalse("User " + ugi
        + " is incorrectly granted the access-control!!",
        acl.isUserAllowed(ugi));
  }
}
