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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

/**
 * Unit tests to validate the BlockPlacementStatusDefault policy, focusing on
 * the getAdditionAlReplicasRequired method.
 */
public class TestBlockPlacementStatusDefault {

  @Test
  public void testIsPolicySatisfiedCorrectly() {
    // 2 current racks and 2 expected
    BlockPlacementStatusDefault bps =
        new BlockPlacementStatusDefault(2, 2, 5);
    assertTrue(bps.isPlacementPolicySatisfied());
    assertEquals(0, bps.getAdditionalReplicasRequired());

    // 1 current rack and 2 expected
    bps =
        new BlockPlacementStatusDefault(1, 2, 5);
    assertFalse(bps.isPlacementPolicySatisfied());
    assertEquals(1, bps.getAdditionalReplicasRequired());

    // 3 current racks and 2 expected
    bps =
        new BlockPlacementStatusDefault(3, 2, 5);
    assertTrue(bps.isPlacementPolicySatisfied());
    assertEquals(0, bps.getAdditionalReplicasRequired());

    // 1 current rack and 2 expected, but only 1 rack on the cluster
    bps =
        new BlockPlacementStatusDefault(1, 2, 1);
    assertTrue(bps.isPlacementPolicySatisfied());
    assertEquals(0, bps.getAdditionalReplicasRequired());
  }
}