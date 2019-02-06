/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.replication;

import static org.junit.Assert.*;

import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for ReplicationActivityStatus.
 */
public class TestReplicationActivityStatus {

  private static EventQueue eventQueue;
  private static ReplicationActivityStatus replicationActivityStatus;

  @BeforeClass
  public static void setup() {
    eventQueue = new EventQueue();
    replicationActivityStatus = new ReplicationActivityStatus();
    eventQueue.addHandler(SCMEvents.START_REPLICATION,
        replicationActivityStatus.getReplicationStatusListener());
    eventQueue.addHandler(SCMEvents.CHILL_MODE_STATUS,
        replicationActivityStatus.getChillModeStatusListener());
  }

  @Test
  public void testReplicationStatusForChillMode()
      throws TimeoutException, InterruptedException {
    assertFalse(replicationActivityStatus.isReplicationEnabled());
    // In chill mode replication process should be stopped.
    eventQueue.fireEvent(SCMEvents.CHILL_MODE_STATUS, true);
    assertFalse(replicationActivityStatus.isReplicationEnabled());

    // Replication should be enabled when chill mode if off.
    eventQueue.fireEvent(SCMEvents.CHILL_MODE_STATUS, false);
    GenericTestUtils.waitFor(() -> {
      return replicationActivityStatus.isReplicationEnabled();
    }, 10, 1000*5);
    assertTrue(replicationActivityStatus.isReplicationEnabled());
  }
}