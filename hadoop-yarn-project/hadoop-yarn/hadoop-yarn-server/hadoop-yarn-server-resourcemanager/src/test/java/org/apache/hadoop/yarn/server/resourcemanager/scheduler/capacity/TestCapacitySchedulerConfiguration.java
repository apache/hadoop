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
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @Test
  public void testNonLabeledQueueCapacity() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    csConf.set(queueKey(ROOT, CapacitySchedulerConfiguration.CAPACITY), "42");
    assertEquals(100f, csConf.getNonLabeledQueueCapacity(ROOT), 0f);

    csConf.set("test.capacity", "30");
    csConf.set(queueKey(ROOT_TEST, CapacitySchedulerConfiguration.CAPACITY),
        "${test.capacity}");
    assertEquals(30f, csConf.getLabeledQueueCapacity(ROOT_TEST, ""), 0f);
    csConf.setCapacityByLabel(ROOT_TEST, "", 35f);
    assertEquals(35f, csConf.getNonLabeledQueueCapacity(ROOT_TEST), 0f);
  }

  @Test
  public void testAccessibleNodeLabels() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    assertNull(csConf.getAccessibleNodeLabels(ROOT_TEST));

    csConf.setAccessibleNodeLabels(ROOT, Set.of("red"));
    assertEquals(Set.of(RMNodeLabelsManager.ANY), csConf.getAccessibleNodeLabels(ROOT));

    csConf.set(queueKey(ROOT_TEST, CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS),
        "red,*");
    assertEquals(Set.of(RMNodeLabelsManager.ANY), csConf.getAccessibleNodeLabels(ROOT_TEST));
  }

  @Test
  public void testLegacyQueueModeDefaultsToTrue() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    assertTrue(csConf.isLegacyQueueMode());
    for (String invalid : new String[] {"junk", ""}) {
      csConf.set(CapacitySchedulerConfiguration.PREFIX + "legacy-queue-mode.enabled", invalid);
      assertTrue(csConf.isLegacyQueueMode());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"6w", "[memory=1024,vcores=1]", "[memory=50%,vcores=2w]"})
  public void testPercentageGettersOfNonPercentageCapacity(String capacity) {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    csConf.set(queueKey(ROOT_TEST, CapacitySchedulerConfiguration.CAPACITY), capacity);
    assertEquals(0f, csConf.getNonLabeledQueueCapacity(ROOT_TEST), 0f);
    assertEquals(100f, csConf.getNonLabeledQueueMaximumCapacity(ROOT_TEST), 0f);
  }

  @ParameterizedTest
  @CsvSource({
      "'[memory=2048,vcores=2]', true",
      "'[memory=2048,vcores=2w]', true",
      "'[foo]', true",
      "'[memory=2048,vcores=50%]', false",
      "'memory=2048', false",
      "'[memory=2048,vcores=2w', false"
  })
  public void testAbsoluteResourceConfigTypeCheck(String capacity, boolean expected) {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    csConf.set(queueKey(ROOT_TEST, CapacitySchedulerConfiguration.CAPACITY), capacity);
    csConf.setCapacityByLabel(ROOT_TEST, "gpu", capacity);
    assertEquals(expected, csConf.checkConfigTypeIsAbsoluteResource("", ROOT_TEST, Set.of()));
    assertEquals(expected,
        csConf.checkConfigTypeIsAbsoluteResource("gpu", ROOT_TEST, Set.of()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"3", " 3 ", "0x3", "${test.value}"})
  public void testQueueIntegerSettingsAreReadLikeGetInt(String value) {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    csConf.set("test.value", "3");
    csConf.set(queueKey(ROOT_TEST, CapacitySchedulerConfiguration.MAXIMUM_QUEUE_DEPTH), value);
    csConf.set(queueKey(ROOT_TEST, CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX),
        value);
    assertEquals(3, csConf.getMaximumAutoCreatedQueueDepth(ROOT_TEST));
    assertEquals(3, csConf.getMaximumApplicationsPerQueue(ROOT_TEST));
  }

  @Test
  public void testUnknownAppOrderingPolicyClass() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    csConf.set(queueKey(ROOT_TEST, CapacitySchedulerConfiguration.ORDERING_POLICY),
        "com.example.Missing");
    RuntimeException e = assertThrows(RuntimeException.class,
        () -> csConf.getAppOrderingPolicy(ROOT_TEST));
    assertEquals("Unable to construct ordering policy for: com.example.Missing,"
        + " com.example.Missing", e.getMessage());
  }

  @Test
  public void testAppOrderingPolicyNameAsQueueOrderingPolicy() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    // "fair" is only an application ordering policy name, a parent queue reads it as a class.
    csConf.set(queueKey(ROOT, CapacitySchedulerConfiguration.ORDERING_POLICY), "fair");
    YarnRuntimeException e = assertThrows(YarnRuntimeException.class,
        () -> csConf.getQueueOrderingPolicy(ROOT, null));
    assertEquals("Unable to construct queue ordering policy=fair queue=root", e.getMessage());
  }

  @Test
  public void testOffSwitchPerHeartbeatLimitIsAtLeastOne() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    csConf.setOffSwitchPerHeartbeatLimit(0);
    assertEquals(1, csConf.getOffSwitchPerHeartbeatLimit());
  }

  @Test
  public void testInvalidAutoCreatedQueueManagementPolicyClass() {
    CapacitySchedulerConfiguration csConf = createDefaultCsConf();
    String policyKey = queueKey(ROOT_TEST,
        CapacitySchedulerConfiguration.AUTO_CREATED_QUEUE_MANAGEMENT_POLICY);

    csConf.set(policyKey, "java.lang.String");
    YarnRuntimeException e = assertThrows(YarnRuntimeException.class,
        () -> csConf.getAutoCreatedQueueManagementPolicyClass(ROOT_TEST));
    assertEquals("Class: java.lang.String not instance of org.apache.hadoop.yarn.server."
        + "resourcemanager.scheduler.capacity.AutoCreatedQueueManagementPolicy", e.getMessage());

    csConf.set(policyKey, "com.example.Missing");
    e = assertThrows(YarnRuntimeException.class,
        () -> csConf.getAutoCreatedQueueManagementPolicyClass(ROOT_TEST));
    assertEquals("Could not instantiate AutoCreatedQueueManagementPolicy: com.example.Missing"
        + " for queue: root.test", e.getMessage());
  }

  private static String queueKey(QueuePath queue, String suffix) {
    return QueuePrefixes.getQueuePrefix(queue) + suffix;
  }
}