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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests {@link FifoOrderingPolicyWithExclusivePartitions} ordering policy.
 */
public class TestFifoOrderingPolicyWithExclusivePartitions {

  private static final String PARTITION = "test";
  private static final String PARTITION2 = "test2";

  @Test
  public void testNoConfiguredExclusiveEnforcedPartitions() {
    FifoOrderingPolicyWithExclusivePartitions<MockSchedulableEntity> policy =
        new FifoOrderingPolicyWithExclusivePartitions<>();
    policy.configure(Collections.EMPTY_MAP);

    MockSchedulableEntity p1 = new MockSchedulableEntity(4, 0, false);
    p1.setPartition(PARTITION);
    p1.setId("p1");
    MockSchedulableEntity p2 = new MockSchedulableEntity(3, 1, false);
    p2.setPartition(PARTITION);
    p2.setId("p2");

    MockSchedulableEntity r1 = new MockSchedulableEntity(2, 0, false);
    r1.setId("r1");
    MockSchedulableEntity r2 = new MockSchedulableEntity(1, 0, false);
    r2.setId("r2");

    policy.addSchedulableEntity(p1);
    policy.addAllSchedulableEntities(Arrays.asList(p2, r1, r2));
    Assert.assertEquals(4, policy.getNumSchedulableEntities());
    Assert.assertEquals(4, policy.getSchedulableEntities().size());
    IteratorSelector sel = new IteratorSelector();
    // Should behave like FifoOrderingPolicy, regardless of partition
    verifyAssignmentIteratorOrder(policy,
        IteratorSelector.EMPTY_ITERATOR_SELECTOR, "p2", "r2", "r1", "p1");
    verifyPreemptionIteratorOrder(policy, "p1", "r1", "r2", "p2");
    sel.setPartition(PARTITION);
    verifyAssignmentIteratorOrder(policy, sel, "p2", "r2", "r1", "p1");
    verifyPreemptionIteratorOrder(policy, "p1", "r1", "r2", "p2");

    policy.removeSchedulableEntity(p2);
    policy.removeSchedulableEntity(r2);
    Assert.assertEquals(2, policy.getNumSchedulableEntities());
    Assert.assertEquals(2, policy.getSchedulableEntities().size());
    verifyAssignmentIteratorOrder(policy,
        IteratorSelector.EMPTY_ITERATOR_SELECTOR, "r1", "p1");
    verifyPreemptionIteratorOrder(policy, "p1", "r1");
    sel.setPartition(PARTITION);
    verifyAssignmentIteratorOrder(policy, sel, "r1", "p1");
    verifyPreemptionIteratorOrder(policy, "p1", "r1");
  }

  @Test
  public void testSingleExclusiveEnforcedPartition() {
    FifoOrderingPolicyWithExclusivePartitions<MockSchedulableEntity> policy =
        new FifoOrderingPolicyWithExclusivePartitions<>();
    policy.configure(Collections.singletonMap(
        YarnConfiguration.EXCLUSIVE_ENFORCED_PARTITIONS_SUFFIX, PARTITION));

    // PARTITION iterator should return p2, p1, p3
    MockSchedulableEntity p1 = new MockSchedulableEntity(1, 0, false);
    p1.setPartition(PARTITION);
    p1.setId("p1");
    MockSchedulableEntity p2 = new MockSchedulableEntity(5, 1, false);
    p2.setPartition(PARTITION);
    p2.setId("p2");
    MockSchedulableEntity p3 = new MockSchedulableEntity(3, 0, false);
    p3.setPartition(PARTITION);
    p3.setId("p3");

    // non-PARTITION iterator should return r3, r2, r1
    MockSchedulableEntity r1 = new MockSchedulableEntity(6, 0, false);
    r1.setId("r1");
    MockSchedulableEntity r2 = new MockSchedulableEntity(4, 0, false);
    r2.setId("r2");
    MockSchedulableEntity r3 = new MockSchedulableEntity(2, 1, false);
    r3.setId("r3");

    policy.addSchedulableEntity(r1);
    Assert.assertEquals(1, policy.getNumSchedulableEntities());
    Assert.assertEquals("r1", policy.getSchedulableEntities()
        .iterator().next().getId());
    verifyAssignmentIteratorOrder(policy,
        IteratorSelector.EMPTY_ITERATOR_SELECTOR, "r1");
    verifyPreemptionIteratorOrder(policy, "r1");

    List<MockSchedulableEntity> entities = Arrays.asList(r2, r3, p1, p2);
    policy.addAllSchedulableEntities(entities);
    policy.addSchedulableEntity(p3);
    Assert.assertEquals(6, policy.getNumSchedulableEntities());
    Assert.assertEquals(6, policy.getSchedulableEntities().size());
    // Assignment iterator should return non-PARTITION entities,
    // in order based on FifoOrderingPolicy
    verifyAssignmentIteratorOrder(policy,
        IteratorSelector.EMPTY_ITERATOR_SELECTOR, "r3", "r2", "r1");
    // Preemption iterator should return all entities, in global order
    verifyPreemptionIteratorOrder(policy, "r1", "r2", "p3", "p1", "p2", "r3");
    // Same thing as above, but with a non-empty partition
    IteratorSelector sel = new IteratorSelector();
    sel.setPartition("dummy");
    verifyAssignmentIteratorOrder(policy, sel, "r3", "r2", "r1");
    verifyPreemptionIteratorOrder(policy, "r1", "r2", "p3", "p1", "p2", "r3");
    // Should return PARTITION entities, in order based on FifoOrderingPolicy
    sel.setPartition(PARTITION);
    verifyAssignmentIteratorOrder(policy, sel, "p2", "p1", "p3");
    verifyPreemptionIteratorOrder(policy, "r1", "r2", "p3", "p1", "p2", "r3");

    policy.removeSchedulableEntity(p2);
    policy.removeSchedulableEntity(r2);
    Assert.assertEquals(4, policy.getNumSchedulableEntities());
    Assert.assertEquals(4, policy.getSchedulableEntities().size());
    verifyAssignmentIteratorOrder(policy,
        IteratorSelector.EMPTY_ITERATOR_SELECTOR, "r3", "r1");
    verifyPreemptionIteratorOrder(policy, "r1", "p3", "p1", "r3");
    sel.setPartition(PARTITION);
    verifyAssignmentIteratorOrder(policy, sel, "p1", "p3");
    verifyPreemptionIteratorOrder(policy, "r1", "p3", "p1", "r3");

    policy.removeSchedulableEntity(p1);
    policy.removeSchedulableEntity(p3);
    Assert.assertEquals(2, policy.getNumSchedulableEntities());
    Assert.assertEquals(2, policy.getSchedulableEntities().size());
    verifyAssignmentIteratorOrder(policy,
        IteratorSelector.EMPTY_ITERATOR_SELECTOR, "r3", "r1");
    verifyPreemptionIteratorOrder(policy, "r1", "r3");
    sel.setPartition(PARTITION);
    verifyAssignmentIteratorOrder(policy, sel);
    verifyPreemptionIteratorOrder(policy, "r1", "r3");
  }

  @Test
  public void testMultipleExclusiveEnforcedPartitions() {
    FifoOrderingPolicyWithExclusivePartitions<MockSchedulableEntity> policy =
        new FifoOrderingPolicyWithExclusivePartitions<>();
    policy.configure(Collections.singletonMap(
        YarnConfiguration.EXCLUSIVE_ENFORCED_PARTITIONS_SUFFIX,
        PARTITION + "," + PARTITION2));

    // PARTITION iterator should return p2, p1
    MockSchedulableEntity p1 = new MockSchedulableEntity(1, 0, false);
    p1.setPartition(PARTITION);
    p1.setId("p1");
    MockSchedulableEntity p2 = new MockSchedulableEntity(5, 1, false);
    p2.setPartition(PARTITION);
    p2.setId("p2");

    // PARTITION2 iterator should return r1, r2
    MockSchedulableEntity r1 = new MockSchedulableEntity(3, 0, false);
    r1.setPartition(PARTITION2);
    r1.setId("r1");
    MockSchedulableEntity r2 = new MockSchedulableEntity(4, 0, false);
    r2.setPartition(PARTITION2);
    r2.setId("r2");

    // default iterator should return s2, s1
    MockSchedulableEntity s1 = new MockSchedulableEntity(6, 0, false);
    s1.setId("s1");
    MockSchedulableEntity s2 = new MockSchedulableEntity(2, 0, false);
    s2.setId("s2");

    policy.addAllSchedulableEntities(Arrays.asList(s1, s2, r1));
    Assert.assertEquals(3, policy.getNumSchedulableEntities());
    Assert.assertEquals(3, policy.getSchedulableEntities().size());
    IteratorSelector sel = new IteratorSelector();
    // assignment iterator returns only default (non-partitioned) entities
    verifyAssignmentIteratorOrder(policy,
        IteratorSelector.EMPTY_ITERATOR_SELECTOR, "s2", "s1");
    verifyPreemptionIteratorOrder(policy, "s1", "r1", "s2");
    sel.setPartition(PARTITION2);
    verifyAssignmentIteratorOrder(policy, sel, "r1");

    policy.addAllSchedulableEntities(Arrays.asList(r2, p1, p2));
    Assert.assertEquals(6, policy.getNumSchedulableEntities());
    Assert.assertEquals(6, policy.getSchedulableEntities().size());
    verifyAssignmentIteratorOrder(policy,
        IteratorSelector.EMPTY_ITERATOR_SELECTOR, "s2", "s1");
    sel.setPartition(PARTITION);
    verifyAssignmentIteratorOrder(policy, sel, "p2", "p1");
    sel.setPartition(PARTITION2);
    verifyAssignmentIteratorOrder(policy, sel, "r1", "r2");
    verifyPreemptionIteratorOrder(policy, "s1", "r2", "r1", "s2", "p1", "p2");

    policy.removeSchedulableEntity(p2);
    policy.removeSchedulableEntity(r1);
    policy.removeSchedulableEntity(r2);
    Assert.assertEquals(3, policy.getNumSchedulableEntities());
    Assert.assertEquals(3, policy.getSchedulableEntities().size());
    verifyAssignmentIteratorOrder(policy,
        IteratorSelector.EMPTY_ITERATOR_SELECTOR, "s2", "s1");
    sel.setPartition(PARTITION);
    verifyAssignmentIteratorOrder(policy, sel, "p1");
    sel.setPartition(PARTITION2);
    verifyAssignmentIteratorOrder(policy, sel);
    verifyPreemptionIteratorOrder(policy, "s1", "s2", "p1");
  }

  private void verifyAssignmentIteratorOrder(
      FifoOrderingPolicyWithExclusivePartitions<MockSchedulableEntity> policy,
      IteratorSelector sel, String... ids) {
    verifyIteratorOrder(policy.getAssignmentIterator(sel), ids);
  }

  private void verifyPreemptionIteratorOrder(
      FifoOrderingPolicyWithExclusivePartitions<MockSchedulableEntity> policy,
      String... ids) {
    verifyIteratorOrder(policy.getPreemptionIterator(), ids);
  }

  private void verifyIteratorOrder(Iterator<MockSchedulableEntity> itr,
      String... ids) {
    for (String id : ids) {
      Assert.assertEquals(id, itr.next().getId());
    }
    Assert.assertFalse(itr.hasNext());
  }
}
