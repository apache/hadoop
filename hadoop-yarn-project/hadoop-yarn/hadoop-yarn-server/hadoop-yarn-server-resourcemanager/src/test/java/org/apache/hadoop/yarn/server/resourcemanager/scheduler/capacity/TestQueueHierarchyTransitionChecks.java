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
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueHierarchyTransitionChecks.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueHierarchyTransitionChecks.QueueSnapshot;
import org.junit.jupiter.api.Test;

public class TestQueueHierarchyTransitionChecks {

  private static QueueSnapshot queue(String path, QueueKind kind, QueueState state) {
    return new QueueSnapshot(path, kind, state, false);
  }

  @Test
  public void testParseConfiguredState() {
    assertNull(QueueHierarchyTransitionChecks.parseConfiguredState(null, "root.a"));
    assertEquals(QueueState.STOPPED,
        QueueHierarchyTransitionChecks.parseConfiguredState("STOPPED", "root.a"));
    assertEquals(QueueState.DRAINING,
        QueueHierarchyTransitionChecks.parseConfiguredState("DRAINING", "root.a"));
    // unlike the queue state getter, the removal check is case sensitive
    assertNull(QueueHierarchyTransitionChecks.parseConfiguredState("stopped", "root.a"));
    assertNull(QueueHierarchyTransitionChecks.parseConfiguredState("invalid", "root.a"));
  }

  @Test
  public void testQueueRemoval() {
    assertNull(QueueHierarchyTransitionChecks.checkQueueRemoval(
        queue("root.a", QueueKind.LEAF, QueueState.STOPPED), null));
    assertNull(QueueHierarchyTransitionChecks.checkQueueRemoval(
        queue("root.a", QueueKind.LEAF, QueueState.RUNNING), QueueState.STOPPED));
    assertNull(QueueHierarchyTransitionChecks.checkQueueRemoval(
        new QueueSnapshot("root.a", QueueKind.PARENT, QueueState.RUNNING, true), null));
    assertEquals("root.a cannot be deleted from the capacity scheduler configuration, as the"
        + " queue is not yet in stopped state. Current State : RUNNING",
        QueueHierarchyTransitionChecks.checkQueueRemoval(
            queue("root.a", QueueKind.LEAF, QueueState.RUNNING), null));
    assertEquals("root.a cannot be deleted from the capacity scheduler configuration, as the"
        + " queue is not yet in stopped state. Current State : DRAINING",
        QueueHierarchyTransitionChecks.checkQueueRemoval(
            queue("root.a", QueueKind.LEAF, QueueState.DRAINING), QueueState.RUNNING));
  }

  @Test
  public void testSameQueuePath() {
    assertNull(QueueHierarchyTransitionChecks.checkSameQueuePath(
        queue("root.a", QueueKind.LEAF, QueueState.RUNNING),
        queue("root.a", QueueKind.LEAF, QueueState.RUNNING)));
    assertEquals("root.a is moved from:root.a to:root.b.a after refresh, which is not allowed.",
        QueueHierarchyTransitionChecks.checkSameQueuePath(
            queue("root.a", QueueKind.LEAF, QueueState.RUNNING),
            queue("root.b.a", QueueKind.LEAF, QueueState.RUNNING)));
  }

  @Test
  public void testParentQueueConversion() {
    QueueSnapshot parent = queue("root.a", QueueKind.PARENT, QueueState.RUNNING);
    QueueSnapshot managed = queue("root.a", QueueKind.MANAGED_PARENT, QueueState.RUNNING);
    QueueSnapshot leaf = queue("root.a", QueueKind.LEAF, QueueState.RUNNING);
    assertNull(QueueHierarchyTransitionChecks.checkParentQueueConversion(parent, parent));
    assertNull(QueueHierarchyTransitionChecks.checkParentQueueConversion(managed, managed));
    assertNull(QueueHierarchyTransitionChecks.checkParentQueueConversion(parent, leaf));
    // the leaf queue conversion check handles leaf queues
    assertNull(QueueHierarchyTransitionChecks.checkParentQueueConversion(leaf, managed));
    assertEquals("Can not convert parent queue: root.a to auto create enabled parent queue"
        + " since it could have other pre-configured queues which is not supported",
        QueueHierarchyTransitionChecks.checkParentQueueConversion(parent, managed));
    String toLeaf = "Cannot convert auto create enabled parent queue: root.a to leaf queue."
        + " Please check  parent queue's configuration auto-create-child-queue.enabled is set"
        + " to true";
    assertEquals(toLeaf,
        QueueHierarchyTransitionChecks.checkParentQueueConversion(managed, leaf));
    assertEquals(toLeaf,
        QueueHierarchyTransitionChecks.checkParentQueueConversion(managed, parent));
  }

  @Test
  public void testLeafQueueConversion() {
    QueueSnapshot runningLeaf = queue("root.a", QueueKind.LEAF, QueueState.RUNNING);
    QueueSnapshot stoppedLeaf = queue("root.a", QueueKind.LEAF, QueueState.STOPPED);
    QueueSnapshot runningParent = queue("root.a", QueueKind.PARENT, QueueState.RUNNING);
    QueueSnapshot stoppedParent = queue("root.a", QueueKind.PARENT, QueueState.STOPPED);
    assertNull(QueueHierarchyTransitionChecks.checkLeafQueueConversion(
        runningLeaf, runningLeaf));
    assertNull(QueueHierarchyTransitionChecks.checkLeafQueueConversion(
        stoppedLeaf, runningParent));
    assertNull(QueueHierarchyTransitionChecks.checkLeafQueueConversion(
        runningLeaf, stoppedParent));
    assertNull(QueueHierarchyTransitionChecks.checkLeafQueueConversion(
        runningParent, runningLeaf));
    String expected = "Can not convert the leaf queue: root.a to parent queue since it is not"
        + " yet in stopped state. Current State : RUNNING";
    assertEquals(expected, QueueHierarchyTransitionChecks.checkLeafQueueConversion(
        runningLeaf, runningParent));
    assertEquals(expected, QueueHierarchyTransitionChecks.checkLeafQueueConversion(
        runningLeaf, queue("root.a", QueueKind.MANAGED_PARENT, QueueState.RUNNING)));
    assertEquals("Can not convert the leaf queue: root.a to parent queue since it is not"
        + " yet in stopped state. Current State : DRAINING",
        QueueHierarchyTransitionChecks.checkLeafQueueConversion(
            queue("root.a", QueueKind.LEAF, QueueState.DRAINING), runningParent));
  }
}
