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

package org.apache.hadoop.yarn.api;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTimelineEntityGroupId {

  @Test
  void testTimelineEntityGroupId() {
    ApplicationId appId1 = ApplicationId.newInstance(1234, 1);
    ApplicationId appId2 = ApplicationId.newInstance(1234, 2);
    TimelineEntityGroupId group1 = TimelineEntityGroupId.newInstance(appId1, "1");
    TimelineEntityGroupId group2 = TimelineEntityGroupId.newInstance(appId1, "2");
    TimelineEntityGroupId group3 = TimelineEntityGroupId.newInstance(appId2, "1");
    TimelineEntityGroupId group4 = TimelineEntityGroupId.newInstance(appId1, "1");

    assertEquals(group1, group4);
    assertNotEquals(group1, group2);
    assertNotEquals(group1, group3);

    assertTrue(group1.compareTo(group4) == 0);
    assertTrue(group1.compareTo(group2) < 0);
    assertTrue(group1.compareTo(group3) < 0);

    assertTrue(group1.hashCode() == group4.hashCode());
    assertFalse(group1.hashCode() == group2.hashCode());
    assertFalse(group1.hashCode() == group3.hashCode());

    assertEquals("timelineEntityGroupId_1234_1_1", group1.toString());
    assertEquals(TimelineEntityGroupId.fromString("timelineEntityGroupId_1234_1_1"), group1);
  }
}
