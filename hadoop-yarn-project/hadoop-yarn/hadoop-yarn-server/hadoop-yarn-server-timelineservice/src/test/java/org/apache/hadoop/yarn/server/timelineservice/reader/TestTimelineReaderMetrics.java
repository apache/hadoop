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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.server.timelineservice.metrics.TimelineReaderMetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test TimelineReaderMetrics.
 */
public class TestTimelineReaderMetrics {

  private TimelineReaderMetrics metrics;

  @Test
  void testTimelineReaderMetrics() {
    assertNotNull(metrics);
    assertEquals(10,
        metrics.getGetEntitiesSuccessLatency().getInterval());
    assertEquals(10,
        metrics.getGetEntitiesFailureLatency().getInterval());
    assertEquals(10,
        metrics.getGetEntityTypesSuccessLatency().getInterval());
    assertEquals(10,
        metrics.getGetEntityTypesFailureLatency().getInterval());
  }

  @BeforeEach
  public void setup() {
    metrics = TimelineReaderMetrics.getInstance();
  }

  @AfterEach
  public void tearDown() {
    TimelineReaderMetrics.destroy();
  }
}