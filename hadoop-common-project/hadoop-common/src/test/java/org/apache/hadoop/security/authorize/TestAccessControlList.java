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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Test;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class TestAccessControlList {

  private static final Log LOG =
    LogFactory.getLog(TestAccessControlList.class);

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
   * lasVegas (,elvis,)
   * memphis (,elvis,) (,jerryLeeLewis,)
   *
   * To run this test:
   *
   * export JAVA_HOME='path/to/java'
   * ant \
   *   -Dtestcase=TestAccessControlList \
   *   -Dtest.output=yes \
   *   -DTestAccessControlListGroupMapping=$className \
   *   compile-native test
   *
   * where $className is one of the classes that provide group
   * mapping services, i.e. classes that implement
   * GroupMappingServiceProvider interface, at this time:
   *   - org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMapping
   *   - org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping
   *
   */
  @Test
  public void testNetgroups() throws Exception {

    if(!NativeCodeLoader.isNativeCodeLoaded()) {
      LOG.info("Not testing netgroups, " +
        "this test only runs when native code is compiled");
      return;
    }

    String groupMappingClassName =
      System.getProperty("TestAccessControlListGroupMapping");

    if(groupMappingClassName == null) {
      LOG.info("Not testing netgroups, no group mapping class specified, " +
        "use -DTestAccessControlListGroupMapping=$className to specify " +
        "group mapping class (must implement GroupMappingServiceProvider " +
        "interface and support netgroups)");
      return;
    }

    LOG.info("Testing netgroups using: " + groupMappingClassName);

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUP_MAPPING,
      groupMappingClassName);

    Groups groups = Groups.getUserToGroupsMappingService(conf);

    AccessControlList acl;

    // create these ACLs to populate groups cache
    acl = new AccessControlList("ja my"); // plain
    acl = new AccessControlList("sinatra ratpack,@lasVegas"); // netgroup
    acl = new AccessControlList(" somegroup,@someNetgroup"); // no user

    // this ACL will be used for testing ACLs
    acl = new AccessControlList("carlPerkins ratpack,@lasVegas");
    acl.addGroup("@memphis");

    // validate the netgroups before and after rehresh to make
    // sure refresh works correctly
    validateNetgroups(groups, acl);
    groups.refresh();
    validateNetgroups(groups, acl);

  }

  /**
   * Validate the netgroups, both group membership and ACL
   * functionality
   *
   * Note: assumes a specific acl setup done by testNetgroups
   *
   * @param groups group to user mapping service
   * @param acl ACL set up in a specific way, see testNetgroups
   */
  private void validateNetgroups(Groups groups,
    AccessControlList acl) throws Exception {

    // check that the netgroups are working
    List<String> elvisGroups = groups.getGroups("elvis");
    assertTrue(elvisGroups.contains("@lasVegas"));
    assertTrue(elvisGroups.contains("@memphis"));
    List<String> jerryLeeLewisGroups = groups.getGroups("jerryLeeLewis");
    assertTrue(jerryLeeLewisGroups.contains("@memphis"));

    // allowed because his netgroup is in ACL
    UserGroupInformation elvis = 
      UserGroupInformation.createRemoteUser("elvis");
    assertUserAllowed(elvis, acl);

    // allowed because he's in ACL
    UserGroupInformation carlPerkins = 
      UserGroupInformation.createRemoteUser("carlPerkins");
    assertUserAllowed(carlPerkins, acl);

    // not allowed because he's not in ACL and has no netgroups
    UserGroupInformation littleRichard = 
      UserGroupInformation.createRemoteUser("littleRichard");
    assertUserNotAllowed(littleRichard, acl);
  }

  @Test
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
  @Test
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

  @Test
  public void testAccessControlList() throws Exception {
    AccessControlList acl;
    Collection<String> users;
    Collection<String> groups;
    
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
  @Test
  public void testAddRemoveAPI() {
    AccessControlList acl;
    Collection<String> users;
    Collection<String> groups;
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
  @Test
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
  @Test
  public void testAddRemoveToWildCardACL() {
    AccessControlList acl = new AccessControlList(" * ");
    assertTrue(acl.isAllAllowed());

    UserGroupInformation drwho =
      UserGroupInformation.createUserForTesting("drwho@EXAMPLE.COM",
          new String[] { "aliens" });
    UserGroupInformation drwho2 =
      UserGroupInformation.createUserForTesting("drwho2@EXAMPLE.COM",
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
  @Test
  public void testIsUserAllowed() {
    AccessControlList acl;

    UserGroupInformation drwho =
        UserGroupInformation.createUserForTesting("drwho@EXAMPLE.COM",
            new String[] { "aliens", "humanoids", "timelord" });
    UserGroupInformation susan =
        UserGroupInformation.createUserForTesting("susan@EXAMPLE.COM",
            new String[] { "aliens", "humanoids", "timelord" });
    UserGroupInformation barbara =
        UserGroupInformation.createUserForTesting("barbara@EXAMPLE.COM",
            new String[] { "humans", "teachers" });
    UserGroupInformation ian =
        UserGroupInformation.createUserForTesting("ian@EXAMPLE.COM",
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

    acl = new AccessControlList("");
    UserGroupInformation spyUser = spy(drwho);
    acl.isUserAllowed(spyUser);
    verify(spyUser, never()).getGroupNames();
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
