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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

public class TestFairOrderingPolicy {

  final static int GB = 1024;

  @Test
  public void testSimpleComparison() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
      new FairOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    Assert.assertTrue(policy.getComparator().compare(r1, r2) == 0);

    //consumption
    r1.setUsed(Resources.createResource(1, 0));
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r1.getSchedulingResourceUsage());
    Assert.assertTrue(policy.getComparator().compare(r1, r2) > 0);
  }

  @Test
  public void testSizeBasedWeight() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
      new FairOrderingPolicy<MockSchedulableEntity>();
    policy.setSizeBasedWeight(true);
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    //No changes, equal
    Assert.assertTrue(policy.getComparator().compare(r1, r2) == 0);

    r1.setUsed(Resources.createResource(4 * GB));
    r2.setUsed(Resources.createResource(4 * GB));

    r1.setPending(Resources.createResource(4 * GB));
    r2.setPending(Resources.createResource(4 * GB));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());

    //Same, equal
    Assert.assertTrue(policy.getComparator().compare(r1, r2) == 0);

    r2.setUsed(Resources.createResource(5 * GB));
    r2.setPending(Resources.createResource(5 * GB));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());

    //More demand and consumption, but not enough more demand to overcome
    //additional consumption
    Assert.assertTrue(policy.getComparator().compare(r1, r2) < 0);

    //High demand, enough to reverse sbw
    r2.setPending(Resources.createResource(100 * GB));
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());
    Assert.assertTrue(policy.getComparator().compare(r1, r2) > 0);
  }

  @Test
  public void testIterators() {
    OrderingPolicy<MockSchedulableEntity> schedOrder =
     new FairOrderingPolicy<MockSchedulableEntity>();

    MockSchedulableEntity msp1 = new MockSchedulableEntity();
    MockSchedulableEntity msp2 = new MockSchedulableEntity();
    MockSchedulableEntity msp3 = new MockSchedulableEntity();

    msp1.setId("1");
    msp2.setId("2");
    msp3.setId("3");

    msp1.setUsed(Resources.createResource(3));
    msp2.setUsed(Resources.createResource(2));
    msp3.setUsed(Resources.createResource(1));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp2.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp2.getSchedulingResourceUsage());

    schedOrder.addSchedulableEntity(msp1);
    schedOrder.addSchedulableEntity(msp2);
    schedOrder.addSchedulableEntity(msp3);


    //Assignment, least to greatest consumption
    checkIds(schedOrder.getAssignmentIterator(), new String[]{"3", "2", "1"});

    //Preemption, greatest to least
    checkIds(schedOrder.getPreemptionIterator(), new String[]{"1", "2", "3"});

    //Change value without inform, should see no change
    msp2.setUsed(Resources.createResource(6));
    checkIds(schedOrder.getAssignmentIterator(), new String[]{"3", "2", "1"});
    checkIds(schedOrder.getPreemptionIterator(), new String[]{"1", "2", "3"});

    //Do inform, will reorder
    schedOrder.containerAllocated(msp2, null);
    checkIds(schedOrder.getAssignmentIterator(), new String[]{"3", "1", "2"});
    checkIds(schedOrder.getPreemptionIterator(), new String[]{"2", "1", "3"});
  }

  public void checkIds(Iterator<MockSchedulableEntity> si,
      String[] ids) {
    for (int i = 0;i < ids.length;i++) {
      Assert.assertEquals(si.next().getId(),
        ids[i]);
    }
  }

}
