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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.Priority;
import org.junit.Assert;
import org.junit.Test;

public
class TestFifoOrderingPolicy {
  
  @Test
  public void testFifoOrderingPolicy() {
    FifoOrderingPolicy<MockSchedulableEntity> policy = 
      new FifoOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    assertEquals("The comparator should return 0 because the entities are created with " +
            "the same values.", 0,
        policy.getComparator().compare(r1, r2));
    
    r1.setSerial(1);
    assertEquals("The lhs entity has a larger serial, the comparator return " +
            "value should be 1.", 1, policy.getComparator().compare(r1, r2));
    
    r2.setSerial(2);
    Assert.assertEquals("The rhs entity has a larger serial, the comparator return " +
        "value should be -1.", -1, policy.getComparator().compare(r1, r2));
  }
  
  @Test
  public void testIterators() {
    OrderingPolicy<MockSchedulableEntity> schedOrder =
     new FifoOrderingPolicy<MockSchedulableEntity>();
    
    MockSchedulableEntity msp1 = new MockSchedulableEntity();
    MockSchedulableEntity msp2 = new MockSchedulableEntity();
    MockSchedulableEntity msp3 = new MockSchedulableEntity();
    
    msp1.setSerial(3);
    msp2.setSerial(2);
    msp3.setSerial(1);
    
    schedOrder.addSchedulableEntity(msp1);
    schedOrder.addSchedulableEntity(msp2);
    schedOrder.addSchedulableEntity(msp3);
    
    //Assignment, oldest to youngest
    checkSerials(Arrays.asList(1L, 2L, 3L), schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR));
    
    //Preemption, youngest to oldest
    checkSerials(Arrays.asList(3L, 2L, 1L), schedOrder.getPreemptionIterator());
  }
  
  public void checkSerials(List<Long> expectedSerials, Iterator<MockSchedulableEntity>
      actualSerialIterator) {
    for (long expectedSerial : expectedSerials) {
      assertEquals(expectedSerial, actualSerialIterator.next().getSerial());
    }
  }
  
  @Test
  public void testFifoOrderingPolicyAlongWithPriority() {
    FifoOrderingPolicy<MockSchedulableEntity> policy =
        new FifoOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    assertEquals("Both r1 and r2 priority is null, the comparator should return 0.", 0,
        policy.getComparator().compare(r1, r2));

    Priority p2 = Priority.newInstance(0);

    // r1 is null and r2 is not null
    r2.setApplicationPriority(p2);
    Assert.assertTrue("The priority of r1 is null, the priority of r2 is not null, " +
            "the comparator should return a negative value.",
        policy.getComparator().compare(r1, r2) < 0);

    Priority p1 = Priority.newInstance(1);

    // r1 is not null and r2 is null
    r1.setApplicationPriority(p1);
    r2.setApplicationPriority(null);
    assertTrue("The priority of r1 is not null, the priority of r2 is null," +
            "the comparator should return a positive value.",
        policy.getComparator().compare(r1, r2) > 0);

    // r1 is not null and r2 is not null
    r1.setApplicationPriority(p1);
    r2.setApplicationPriority(p2);
    Assert.assertTrue("Both priorities are not null, the r1 has higher priority, " +
            "the result should be a negative value.",
        policy.getComparator().compare(r1, r2) < 0);
  }

  @Test
  public void testOrderingUsingAppSubmitTime() {
    FifoOrderingPolicy<MockSchedulableEntity> policy =
        new FifoOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    // R1, R2 has been started at same time
    assertEquals(r1.getStartTime(), r2.getStartTime());

    // No changes, equal
    assertEquals("The submit times are the same, the comparator should return 0.", 0,
        policy.getComparator().compare(r1, r2));

    // R2 has been started after R1
    r1.setStartTime(5);
    r2.setStartTime(10);
    Assert.assertTrue("r2 was started after r1, " +
            "the comparator should return a negative value.",
        policy.getComparator().compare(r1, r2) < 0);

    // R1 has been started after R2
    r1.setStartTime(10);
    r2.setStartTime(5);
    Assert.assertTrue("r2 was started before r1, the comparator should return a positive value.",
        policy.getComparator().compare(r1, r2) > 0);
  }
}
