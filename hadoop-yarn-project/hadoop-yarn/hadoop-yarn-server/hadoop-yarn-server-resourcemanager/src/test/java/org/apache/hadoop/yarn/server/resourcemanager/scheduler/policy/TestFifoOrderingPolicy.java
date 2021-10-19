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

import java.util.*;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.yarn.api.records.Priority;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFifoOrderingPolicy {
  
  @Test
  public void testFifoOrderingPolicy() {
    FifoOrderingPolicy<MockSchedulableEntity> policy = 
      new FifoOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    assertThat(policy.getComparator().compare(r1, r2)).isEqualTo(0);
    
    r1.setSerial(1);
    assertThat(policy.getComparator().compare(r1, r2)).isEqualTo(1);
    
    r2.setSerial(2);
    assertThat(policy.getComparator().compare(r1, r2)).isEqualTo(-1);
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
    checkSerials(schedOrder.getAssignmentIterator(IteratorSelector.EMPTY_ITERATOR_SELECTOR), new long[]{1, 2, 3});
    
    //Preemption, youngest to oldest
    checkSerials(schedOrder.getPreemptionIterator(), new long[]{3, 2, 1});
  }
  
  public void checkSerials(Iterator<MockSchedulableEntity> si, 
      long[] serials) {
    for (int i = 0;i < serials.length;i++) {
      Assert.assertEquals(si.next().getSerial(), 
        serials[i]);
    }
  }
  
  @Test
  public void testFifoOrderingPolicyAlongWithPriorty() {
    FifoOrderingPolicy<MockSchedulableEntity> policy =
        new FifoOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    Priority p1 = Priority.newInstance(1);
    Priority p2 = Priority.newInstance(0);

    // Both r1 and r1 priority is null
    Assert.assertEquals(0, policy.getComparator().compare(r1, r2));

    // r1 is null and r2 is not null
    r2.setApplicationPriority(p2);
    Assert.assertEquals(-1, policy.getComparator().compare(r1, r2));

    // r1 is not null and r2 is null
    r2.setApplicationPriority(null);
    r1.setApplicationPriority(p1);
    Assert.assertEquals(1, policy.getComparator().compare(r1, r2));

    // r1 is not null and r2 is not null
    r1.setApplicationPriority(p1);
    r2.setApplicationPriority(p2);
    Assert.assertEquals(-1, policy.getComparator().compare(r1, r2));
  }

}
