/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCapacitySchedulerConfiguration {

  private static final String ROOT_TEST_PATH = CapacitySchedulerConfiguration.ROOT + ".test";
  private static final QueuePath ROOT_TEST = new QueuePath(ROOT_TEST_PATH);
  private static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  private static final String EMPTY_ACL = "";
  private static final String SPACE_ACL = " ";
  private static final String USER1 = "user1";
  private static final String USER2 = "user2";
  private static final String GROUP1 = "group1";
  private static final String GROUP2 = "group2";
  public static final String ONE_USER_ONE_GROUP_ACL = USER1 + " " + GROUP1;
  public static final String TWO_USERS_TWO_GROUPS_ACL =
      USER1 + "," + USER2 + " " + GROUP1 + ", " + GROUP2;

  private CapacitySchedulerConfiguration createDefaultCsConf() {
    return new CapacitySchedulerConfiguration(new Configuration(false), false);
  }

  private AccessControlList getSubmitAcl(CapacitySchedulerConfiguration csConf, QueuePath queue) {
    return csConf.getAcl(queue, QueueACL.SUBMIT_APPLICATIONS);
  }

  private void setSubmitAppsConfig(CapacitySchedulerConfiguration csConf, QueuePath queue,
      String value) {
    csConf.set(getSubmitAppsConfigKey(queue), value);
  }

  private String getSubmitAppsConfigKey(QueuePath queue) {
    return QueuePrefixes.getQueuePrefix(queue) + "acl_submit_applications";
  }

  private void testWithGivenAclNoOneHasAccess(QueuePath queue, String aclValue) {
    testWithGivenAclNoOneHasAccessInternal(queue, queue, aclValue);
  }

  private void testWithGivenAclNoOneHasAccess(QueuePath queueToSet, QueuePath queueToVerify,
      String aclValue) {
    testWithGivenAclNoOneHasAccessInternal(queueToSet, queueToVerify, aclValue);
  }

  private void testWithGivenAclNoOneHasAccessInternal(QueuePath queueToSet, QueuePath queueToVerify,
      String aclValue) {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    setSubmitAppsConfig(csConf, queueToSet, aclValue);
    AccessControlList acl = getSubmitAcl(csConf, queueToVerify);
    assertTrue(acl.getUsers().isEmpty());
    assertTrue(acl.getGroups().isEmpty());
    assertFalse(acl.isAllAllowed());
  }

  private void testWithGivenAclCorrectUserAndGroupHasAccess(QueuePath queue, String aclValue,
      Set<String> expectedUsers, Set<String> expectedGroups) {
    testWithGivenAclCorrectUserAndGroupHasAccessInternal(queue, queue, aclValue, expectedUsers,
        expectedGroups);
  }

  private void testWithGivenAclCorrectUserAndGroupHasAccessInternal(QueuePath queueToSet,
      QueuePath queueToVerify, String aclValue, Set<String> expectedUsers,
      Set<String> expectedGroups) {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    setSubmitAppsConfig(csConf, queueToSet, aclValue);
    AccessControlList acl = getSubmitAcl(csConf, queueToVerify);
    assertFalse(acl.getUsers().isEmpty());
    assertFalse(acl.getGroups().isEmpty());
    assertEquals(expectedUsers, acl.getUsers());
    assertEquals(expectedGroups, acl.getGroups());
    assertFalse(acl.isAllAllowed());
  }

  @Test
  public void testDefaultSubmitACLForRootAllAllowed() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    AccessControlList acl = getSubmitAcl(csConf, ROOT);
    assertTrue(acl.getUsers().isEmpty());
    assertTrue(acl.getGroups().isEmpty());
    assertTrue(acl.isAllAllowed());
  }

  @Test
  public void testDefaultSubmitACLForRootChildNoneAllowed() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    AccessControlList acl = getSubmitAcl(csConf, ROOT_TEST);
    assertTrue(acl.getUsers().isEmpty());
    assertTrue(acl.getGroups().isEmpty());
    assertFalse(acl.isAllAllowed());
  }

  @Test
  public void testSpecifiedEmptySubmitACLForRoot() {
    testWithGivenAclNoOneHasAccess(ROOT, EMPTY_ACL);
  }

  @Test
  public void testSpecifiedEmptySubmitACLForRootIsNotInherited() {
    testWithGivenAclNoOneHasAccess(ROOT, ROOT_TEST, EMPTY_ACL);
  }

  @Test
  public void testSpecifiedSpaceSubmitACLForRoot() {
    testWithGivenAclNoOneHasAccess(ROOT, SPACE_ACL);
  }

  @Test
  public void testSpecifiedSpaceSubmitACLForRootIsNotInherited() {
    testWithGivenAclNoOneHasAccess(ROOT, ROOT_TEST, SPACE_ACL);
  }

  @Test
  public void testSpecifiedSubmitACLForRoot() {
    Set<String> expectedUsers = Sets.newHashSet(USER1);
    Set<String> expectedGroups = Sets.newHashSet(GROUP1);
    testWithGivenAclCorrectUserAndGroupHasAccess(ROOT, ONE_USER_ONE_GROUP_ACL, expectedUsers,
        expectedGroups);
  }

  @Test
  public void testSpecifiedSubmitACLForRootIsNotInherited() {
    testWithGivenAclNoOneHasAccess(ROOT, ROOT_TEST, ONE_USER_ONE_GROUP_ACL);
  }

  @Test
  public void testSpecifiedSubmitACLTwoUsersTwoGroupsForRoot() {
    Set<String> expectedUsers = Sets.newHashSet(USER1, USER2);
    Set<String> expectedGroups = Sets.newHashSet(GROUP1, GROUP2);
    testWithGivenAclCorrectUserAndGroupHasAccess(ROOT, TWO_USERS_TWO_GROUPS_ACL, expectedUsers,
        expectedGroups);
  }

}