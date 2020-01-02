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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class TestAccessControlList {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAccessControlList.class);

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
    assertThat(acl.toString()).isEqualTo("All users are allowed");
    validateGetAclString(acl);

    acl = new AccessControlList(" ");
    assertThat(acl.toString()).isEqualTo("No users are allowed");

    acl = new AccessControlList("user1,user2");
    assertThat(acl.toString()).isEqualTo("Users [user1, user2] are allowed");
    validateGetAclString(acl);

    acl = new AccessControlList("user1,user2 ");// with space
    assertThat(acl.toString()).isEqualTo("Users [user1, user2] are allowed");
    validateGetAclString(acl);

    acl = new AccessControlList(" group1,group2");
    assertThat(acl.toString()).isEqualTo(
        "Members of the groups [group1, group2] are allowed");
    validateGetAclString(acl);

    acl = new AccessControlList("user1,user2 group1,group2");
    assertThat(acl.toString()).isEqualTo(
        "Users [user1, user2] and " +
        "members of the groups [group1, group2] are allowed");
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
    assertThat(users.size()).isOne();
    assertThat(users.iterator().next()).isEqualTo("drwho");
    groups = acl.getGroups();
    assertThat(groups.size()).isOne();
    assertThat(groups.iterator().next()).isEqualTo("tardis");
    
    acl = new AccessControlList("drwho");
    users = acl.getUsers();
    assertThat(users.size()).isOne();
    assertThat(users.iterator().next()).isEqualTo("drwho");
    groups = acl.getGroups();
    assertThat(groups.size()).isZero();
    
    acl = new AccessControlList("drwho ");
    users = acl.getUsers();
    assertThat(users.size()).isOne();
    assertThat(users.iterator().next()).isEqualTo("drwho");
    groups = acl.getGroups();
    assertThat(groups.size()).isZero();
    
    acl = new AccessControlList(" tardis");
    users = acl.getUsers();
    assertThat(users.size()).isZero();
    groups = acl.getGroups();
    assertThat(groups.size()).isOne();
    assertThat(groups.iterator().next()).isEqualTo("tardis");

    Iterator<String> iter;    
    acl = new AccessControlList("drwho,joe tardis, users");
    users = acl.getUsers();
    assertThat(users.size()).isEqualTo(2);
    iter = users.iterator();
    assertThat(iter.next()).isEqualTo("drwho");
    assertThat(iter.next()).isEqualTo("joe");
    groups = acl.getGroups();
    assertThat(groups.size()).isEqualTo(2);
    iter = groups.iterator();
    assertThat(iter.next()).isEqualTo("tardis");
    assertThat(iter.next()).isEqualTo("users");
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
    assertThat(acl.getUsers().size()).isZero();
    assertThat(acl.getGroups().size()).isZero();
    assertThat(acl.getAclString()).isEqualTo(" ");
    
    acl.addUser("drwho");
    users = acl.getUsers();
    assertThat(users.size()).isOne();
    assertThat(users.iterator().next()).isEqualTo("drwho");
    assertThat(acl.getAclString()).isEqualTo("drwho ");
    
    acl.addGroup("tardis");
    groups = acl.getGroups();
    assertThat(groups.size()).isOne();
    assertThat(groups.iterator().next()).isEqualTo("tardis");
    assertThat(acl.getAclString()).isEqualTo("drwho tardis");
    
    acl.addUser("joe");
    acl.addGroup("users");
    users = acl.getUsers();
    assertThat(users.size()).isEqualTo(2);
    Iterator<String> iter = users.iterator();
    assertThat(iter.next()).isEqualTo("drwho");
    assertThat(iter.next()).isEqualTo("joe");
    groups = acl.getGroups();
    assertThat(groups.size()).isEqualTo(2);
    iter = groups.iterator();
    assertThat(iter.next()).isEqualTo("tardis");
    assertThat(iter.next()).isEqualTo("users");
    assertThat(acl.getAclString()).isEqualTo("drwho,joe tardis,users");

    acl.removeUser("joe");
    acl.removeGroup("users");
    users = acl.getUsers();
    assertThat(users.size()).isOne();
    assertFalse(users.contains("joe"));
    groups = acl.getGroups();
    assertThat(groups.size()).isOne();
    assertFalse(groups.contains("users"));
    assertThat(acl.getAclString()).isEqualTo("drwho tardis");
    
    acl.removeGroup("tardis");
    groups = acl.getGroups();
    assertThat(groups.size()).isZero();
    assertFalse(groups.contains("tardis"));
    assertThat(acl.getAclString()).isEqualTo("drwho ");
    
    acl.removeUser("drwho");
    assertThat(users.size()).isZero();
    assertFalse(users.contains("drwho"));
    assertThat(acl.getGroups().size()).isZero();
    assertThat(acl.getUsers().size()).isZero();
    assertThat(acl.getAclString()).isEqualTo(" ");
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
    assertThat(th).isNotNull();
    assertThat(th).isInstanceOf(IllegalArgumentException.class);
    
    th = null;
    try {
      acl.addGroup(" * ");
    } catch (Throwable t) {
      th = t;
    }
    assertThat(th).isNotNull();
    assertThat(th).isInstanceOf(IllegalArgumentException.class);
    th = null;
    try {
    acl.removeUser(" * ");
    } catch (Throwable t) {
      th = t;
    }
    assertThat(th).isNotNull();
    assertThat(th).isInstanceOf(IllegalArgumentException.class);
    th = null;
    try {
    acl.removeGroup(" * ");
    } catch (Throwable t) {
      th = t;
    }
    assertThat(th).isNotNull();
    assertThat(th).isInstanceOf(IllegalArgumentException.class);
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
