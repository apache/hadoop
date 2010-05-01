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
