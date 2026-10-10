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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueStateHelper.InitialStateInput;
import org.junit.jupiter.api.Test;

public class TestQueueStateHelper {

  @Test
  public void testCheckConfiguredState() {
    assertNull(QueueStateHelper.checkConfiguredState(null));
    assertNull(QueueStateHelper.checkConfiguredState(QueueState.RUNNING));
    assertNull(QueueStateHelper.checkConfiguredState(QueueState.STOPPED));
    assertEquals("Invalid queue state configuration. We can only use RUNNING or STOPPED.",
        QueueStateHelper.checkConfiguredState(QueueState.DRAINING));
  }

  @Test
  public void testCheckInitialStatePasses() {
    assertNull(QueueStateHelper.checkInitialState(
        new InitialStateInput("root", QueueState.RUNNING, null, null)));
    assertNull(QueueStateHelper.checkInitialState(
        new InitialStateInput("root.a", QueueState.RUNNING, "root", QueueState.RUNNING)));
    assertNull(QueueStateHelper.checkInitialState(
        new InitialStateInput("root.a", QueueState.STOPPED, "root", QueueState.RUNNING)));
    assertNull(QueueStateHelper.checkInitialState(
        new InitialStateInput("root.a", QueueState.STOPPED, "root", QueueState.STOPPED)));
    // a child without a configured state inherits the state of a stopped parent
    assertNull(QueueStateHelper.checkInitialState(
        new InitialStateInput("root.a", null, "root", QueueState.STOPPED)));
  }

  @Test
  public void testCheckInitialStateFails() {
    String expected = "The parent queue:root.a cannot be STOPPED as the child queue:root.a.b"
        + " is in RUNNING state.";
    assertEquals(expected, QueueStateHelper.checkInitialState(
        new InitialStateInput("root.a.b", QueueState.RUNNING, "root.a", QueueState.STOPPED)));
    assertEquals(expected, QueueStateHelper.checkInitialState(
        new InitialStateInput("root.a.b", QueueState.RUNNING, "root.a", QueueState.DRAINING)));
  }

  @Test
  public void testGetInitialState() {
    assertEquals(QueueState.RUNNING, QueueStateHelper.getInitialState(null, null));
    assertEquals(QueueState.STOPPED, QueueStateHelper.getInitialState(QueueState.STOPPED, null));
    assertEquals(QueueState.RUNNING,
        QueueStateHelper.getInitialState(null, QueueState.RUNNING));
    assertEquals(QueueState.STOPPED,
        QueueStateHelper.getInitialState(null, QueueState.STOPPED));
    assertEquals(QueueState.STOPPED,
        QueueStateHelper.getInitialState(null, QueueState.DRAINING));
    assertEquals(QueueState.RUNNING,
        QueueStateHelper.getInitialState(QueueState.RUNNING, QueueState.STOPPED));
  }

  @Test
  public void testCheckParentRunning() {
    assertNull(QueueStateHelper.checkParentRunning(null, null));
    assertNull(QueueStateHelper.checkParentRunning("root", QueueState.RUNNING));
    String expected = "The parent Queue:root.a is not running."
        + " Please activate the parent queue first";
    assertEquals(expected,
        QueueStateHelper.checkParentRunning("root.a", QueueState.STOPPED));
    assertEquals(expected,
        QueueStateHelper.checkParentRunning("root.a", QueueState.DRAINING));
  }
}
