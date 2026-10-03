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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue.CapacityConfigType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueStructureChecks.QueueStructureInput;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.Test;

public class TestQueueStructureChecks {

  private static QueueStructureInput input(String path, boolean root, int childCount,
      boolean reservable, boolean autoQueueCreationEnabledParent) {
    String name = path.substring(path.lastIndexOf('.') + 1);
    return new QueueStructureInput(path, name, root, childCount, reservable,
        autoQueueCreationEnabledParent);
  }

  @Test
  public void testIsParent() {
    assertFalse(QueueStructureChecks.isParent(input("root.a", false, 0, false, false)));
    assertTrue(QueueStructureChecks.isParent(input("root.a", false, 2, false, false)));
    assertTrue(QueueStructureChecks.isParent(input("root.a", false, 0, false, true)));
  }

  @Test
  public void testRootHasChildQueues() {
    assertNull(QueueStructureChecks.checkRootHasChildQueues(
        input("root", true, 1, false, false)));
    assertNull(QueueStructureChecks.checkRootHasChildQueues(
        input("root", true, 0, false, true)));
    assertNull(QueueStructureChecks.checkRootHasChildQueues(
        input("root.a", false, 0, false, false)));
    assertEquals("Queue configuration missing child queue names for root",
        QueueStructureChecks.checkRootHasChildQueues(input("root", true, 0, false, false)));
  }

  @Test
  public void testReservable() {
    assertNull(QueueStructureChecks.checkReservable(input("root.a", false, 0, true, false)));
    assertNull(QueueStructureChecks.checkReservable(input("root.a", false, 2, false, false)));
    assertEquals("Only Leaf Queues can be reservable for root.a.b",
        QueueStructureChecks.checkReservable(input("root.a.b", false, 1, true, false)));
    assertEquals("Only Leaf Queues can be reservable for root.a",
        QueueStructureChecks.checkReservable(input("root.a", false, 0, true, true)));
  }

  @Test
  public void testPlanQueueChildren() {
    assertNull(QueueStructureChecks.checkPlanQueueChildren(1));
    String expected = "Reservable Queue should not have sub-queues in theconfiguration expect"
        + " the default reservation queue";
    assertEquals(expected, QueueStructureChecks.checkPlanQueueChildren(0));
    assertEquals(expected, QueueStructureChecks.checkPlanQueueChildren(2));
  }

  @Test
  public void testLeafQueueTemplateConfigType() {
    Resource min = Resource.newInstance(1024, 1);
    assertNull(QueueStructureChecks.checkLeafQueueTemplateConfigType("root.m",
        CapacityConfigType.PERCENTAGE, Resources.none()));
    assertNull(QueueStructureChecks.checkLeafQueueTemplateConfigType("root.m",
        CapacityConfigType.ABSOLUTE_RESOURCE, min));
    assertNull(QueueStructureChecks.checkLeafQueueTemplateConfigType("root.m",
        CapacityConfigType.NONE, min));
    assertEquals("Managed Parent Queue root.m config type is different from leaf queue"
        + " template config type", QueueStructureChecks.checkLeafQueueTemplateConfigType(
            "root.m", CapacityConfigType.PERCENTAGE, min));
  }
}
