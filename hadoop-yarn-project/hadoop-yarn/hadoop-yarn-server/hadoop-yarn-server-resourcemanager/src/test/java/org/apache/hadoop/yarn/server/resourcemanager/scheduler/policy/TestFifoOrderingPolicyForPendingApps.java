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

import static org.assertj.core.api.Assertions.assertThat;

public class TestFifoOrderingPolicyForPendingApps {

  @Test
  public void testFifoOrderingPolicyForPendingApps() {
    FifoOrderingPolicyForPendingApps<MockSchedulableEntity> policy =
        new FifoOrderingPolicyForPendingApps<MockSchedulableEntity>();

    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    assertThat(policy.getComparator().compare(r1, r2)).isEqualTo(0);

    r1.setSerial(1);
    r1.setRecovering(true);
    assertThat(policy.getComparator().compare(r1, r2)).isEqualTo(-1);

    r1.setRecovering(false);
    r2.setSerial(2);
    r2.setRecovering(true);
    assertThat(policy.getComparator().compare(r1, r2)).isEqualTo(1);
  }

  /**
   * Entities submitted with E1-Recovering, E2-Recovering, E3-Recovering, E4-not
   * recovering, E5-not recovering.
   * Expected Iterator Output : E-3 E-2 E-1 E-5 E-4
   */
  @Test
  public void testIterators() {
    OrderingPolicy<MockSchedulableEntity> schedOrder =
        new FifoOrderingPolicyForPendingApps<MockSchedulableEntity>();

    MockSchedulableEntity msp1 = new MockSchedulableEntity(1, 1, true);
    MockSchedulableEntity msp2 = new MockSchedulableEntity(2, 2, true);
    MockSchedulableEntity msp3 = new MockSchedulableEntity(3, 3, true);
    MockSchedulableEntity msp4 = new MockSchedulableEntity(4, 2, true);
    MockSchedulableEntity msp5 = new MockSchedulableEntity(5, 5, false);
    MockSchedulableEntity msp6 = new MockSchedulableEntity(6, 6, false);
    MockSchedulableEntity msp7 = new MockSchedulableEntity(7, 5, false);

    schedOrder.addSchedulableEntity(msp1);
    schedOrder.addSchedulableEntity(msp2);
    schedOrder.addSchedulableEntity(msp3);
    schedOrder.addSchedulableEntity(msp4);
    schedOrder.addSchedulableEntity(msp5);
    schedOrder.addSchedulableEntity(msp6);
    schedOrder.addSchedulableEntity(msp7);

    // Assignment with serial id's are 3,2,4,1,6,5,7
    checkSerials(schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR), new long[] {3, 2, 4, 1,
        6, 5, 7});

    //Preemption, youngest to oldest
    checkSerials(schedOrder.getPreemptionIterator(), new long[] { 7, 5, 6, 1,
        4, 2, 3 });
  }

  public void checkSerials(Iterator<MockSchedulableEntity> si,
      long[] serials) {
    for (int i = 0; i < serials.length; i++) {
      Assert.assertEquals(si.next().getSerial(), serials[i]);
    }
  }
}
