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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for BlockPlacementStatusWithUpgradeDomain class.
 */
public class TestBlockPlacementStatusWithUpgradeDomain {

  private Set<String> upgradeDomains;
  private BlockPlacementStatusDefault bpsd =
      mock(BlockPlacementStatusDefault.class);

  @Before
  public void setup() {
    upgradeDomains = new HashSet<String>();
    upgradeDomains.add("1");
    upgradeDomains.add("2");
    upgradeDomains.add("3");
    when(bpsd.isPlacementPolicySatisfied()).thenReturn(true);
  }

  @Test
  public void testIsPolicySatisfiedParentFalse() {
    when(bpsd.isPlacementPolicySatisfied()).thenReturn(false);
    when(bpsd.getAdditionalReplicasRequired()).thenReturn(1);
    BlockPlacementStatusWithUpgradeDomain bps =
        new BlockPlacementStatusWithUpgradeDomain(bpsd, upgradeDomains, 3, 3);

    // Parent policy is not satisfied but upgrade domain policy is
    assertFalse(bps.isPlacementPolicySatisfied());
    assertEquals(1, bps.getAdditionalReplicasRequired());
  }

  @Test
  public void testIsPolicySatisfiedAllEqual() {
    BlockPlacementStatusWithUpgradeDomain bps =
        new BlockPlacementStatusWithUpgradeDomain(bpsd, upgradeDomains, 3, 3);
    // Number of domains, replicas and upgradeDomainFactor is equal and parent
    // policy is satisfied
    assertTrue(bps.isPlacementPolicySatisfied());
    assertEquals(0, bps.getAdditionalReplicasRequired());
  }

  @Test
  public void testIsPolicySatisfiedSmallDomains() {
    // Number of domains is less than replicas but equal to factor
    BlockPlacementStatusWithUpgradeDomain bps =
        new BlockPlacementStatusWithUpgradeDomain(bpsd, upgradeDomains, 4, 3);
    assertTrue(bps.isPlacementPolicySatisfied());
    assertEquals(0, bps.getAdditionalReplicasRequired());

    // Same as above but replicas is greater than factor
    bps = new BlockPlacementStatusWithUpgradeDomain(bpsd, upgradeDomains, 4, 2);
    assertTrue(bps.isPlacementPolicySatisfied());
    assertEquals(0, bps.getAdditionalReplicasRequired());

    // Number of domains is less than replicas and factor
    bps = new BlockPlacementStatusWithUpgradeDomain(bpsd, upgradeDomains, 4, 4);
    assertFalse(bps.isPlacementPolicySatisfied());
    assertEquals(1, bps.getAdditionalReplicasRequired());
  }

  @Test
  public void testIsPolicySatisfiedSmallReplicas() {
    // Replication factor 1 file
    upgradeDomains.clear();
    upgradeDomains.add("1");
    BlockPlacementStatusWithUpgradeDomain bps =
        new BlockPlacementStatusWithUpgradeDomain(bpsd, upgradeDomains, 1, 3);
    assertTrue(bps.isPlacementPolicySatisfied());
    assertEquals(0, bps.getAdditionalReplicasRequired());

    // Replication factor 2 file, but one domain
    bps =
        new BlockPlacementStatusWithUpgradeDomain(bpsd, upgradeDomains, 2, 3);
    assertFalse(bps.isPlacementPolicySatisfied());
    assertEquals(1, bps.getAdditionalReplicasRequired());

    // Replication factor 2 file, but two domains
    upgradeDomains.add("2");
    bps =
        new BlockPlacementStatusWithUpgradeDomain(bpsd, upgradeDomains, 2, 3);
    assertTrue(bps.isPlacementPolicySatisfied());
    assertEquals(0, bps.getAdditionalReplicasRequired());
  }

  @Test
  public void testPolicyIsNotSatisfiedInsufficientDomains() {
    // Insufficient Domains - 1 domain, replication factor 3
    upgradeDomains.clear();
    upgradeDomains.add("1");
    BlockPlacementStatusWithUpgradeDomain bps =
        new BlockPlacementStatusWithUpgradeDomain(bpsd, upgradeDomains, 3, 3);
    assertFalse(bps.isPlacementPolicySatisfied());
    assertEquals(2, bps.getAdditionalReplicasRequired());

    // One domain, replication factor 2 file
    bps =
        new BlockPlacementStatusWithUpgradeDomain(bpsd, upgradeDomains, 2, 3);
    assertFalse(bps.isPlacementPolicySatisfied());
    assertEquals(1, bps.getAdditionalReplicasRequired());

    // 2 domains, replication factor 3
    upgradeDomains.add("2");
    bps =
        new BlockPlacementStatusWithUpgradeDomain(bpsd, upgradeDomains, 3, 3);
    assertFalse(bps.isPlacementPolicySatisfied());
    assertEquals(1, bps.getAdditionalReplicasRequired());
  }
}
