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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSchedulingPolicy {

  @Test(timeout = 1000)
  public void testParseSchedulingPolicy()
      throws AllocationConfigurationException {

    // Class name
    SchedulingPolicy sm = SchedulingPolicy
        .parse(FairSharePolicy.class.getName());
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSharePolicy.NAME));

    // Canonical name
    sm = SchedulingPolicy.parse(FairSharePolicy.class
        .getCanonicalName());
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSharePolicy.NAME));

    // Class
    sm = SchedulingPolicy.getInstance(FairSharePolicy.class);
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSharePolicy.NAME));

    // Shortname - drf
    sm = SchedulingPolicy.parse("drf");
    assertTrue("Invalid scheduler name",
        sm.getName().equals(DominantResourceFairnessPolicy.NAME));
    
    // Shortname - fair
    sm = SchedulingPolicy.parse("fair");
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FairSharePolicy.NAME));

    // Shortname - fifo
    sm = SchedulingPolicy.parse("fifo");
    assertTrue("Invalid scheduler name",
        sm.getName().equals(FifoPolicy.NAME));
  }

  /**
   * Trivial tests that make sure
   * {@link SchedulingPolicy#isApplicableTo(SchedulingPolicy, byte)} works as
   * expected for the possible values of depth
   * 
   * @throws AllocationConfigurationException
   */
  @Test(timeout = 1000)
  public void testIsApplicableTo() throws AllocationConfigurationException {
    final String ERR = "Broken SchedulingPolicy#isApplicableTo";
    
    // fifo
    SchedulingPolicy policy = SchedulingPolicy.parse("fifo");
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_LEAF));
    assertFalse(ERR, SchedulingPolicy.isApplicableTo(
        SchedulingPolicy.parse("fifo"), SchedulingPolicy.DEPTH_INTERMEDIATE));
    assertFalse(ERR, SchedulingPolicy.isApplicableTo(
        SchedulingPolicy.parse("fifo"), SchedulingPolicy.DEPTH_ROOT));

    
    // fair
    policy = SchedulingPolicy.parse("fair"); 
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_LEAF));
    assertTrue(ERR, SchedulingPolicy.isApplicableTo(policy,
        SchedulingPolicy.DEPTH_INTERMEDIATE));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ROOT));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_PARENT));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ANY));
    
    // drf
    policy = SchedulingPolicy.parse("drf"); 
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_LEAF));
    assertTrue(ERR, SchedulingPolicy.isApplicableTo(policy,
        SchedulingPolicy.DEPTH_INTERMEDIATE));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ROOT));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_PARENT));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ANY));
    
    policy = Mockito.mock(SchedulingPolicy.class);
    Mockito.when(policy.getApplicableDepth()).thenReturn(
        SchedulingPolicy.DEPTH_PARENT);
    assertTrue(ERR, SchedulingPolicy.isApplicableTo(policy,
        SchedulingPolicy.DEPTH_INTERMEDIATE));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ROOT));
    assertTrue(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_PARENT));
    assertFalse(ERR,
        SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_ANY));
  }
}
