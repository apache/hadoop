/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;

public class TestDeactivatedLeafQueuesByLabel {
  @Test
  public void testGetMaxLeavesToBeActivated() {
    DeactivatedLeafQueuesByLabel d1 = spy(DeactivatedLeafQueuesByLabel.class);
    d1.setAvailableCapacity(0.17f);
    d1.setLeafQueueTemplateAbsoluteCapacity(0.03f);
    assertEquals(1, d1.getMaxLeavesToBeActivated(1));

    DeactivatedLeafQueuesByLabel d2 = spy(DeactivatedLeafQueuesByLabel.class);
    d2.setAvailableCapacity(0.17f);
    d2.setLeafQueueTemplateAbsoluteCapacity(0.03f);
    assertEquals(5, d2.getMaxLeavesToBeActivated(7));

    DeactivatedLeafQueuesByLabel d3 = spy(DeactivatedLeafQueuesByLabel.class);
    d3.setAvailableCapacity(0f);
    d3.setLeafQueueTemplateAbsoluteCapacity(0.03f);
    assertEquals(0, d3.getMaxLeavesToBeActivated(10));
  }
}