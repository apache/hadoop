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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricOperation;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestTimelineCollector {

  private TimelineEntities generateTestEntities(int groups, int entities) {
    TimelineEntities te = new TimelineEntities();
    for (int j = 0; j < groups; j++) {
      for (int i = 0; i < entities; i++) {
        TimelineEntity entity = new TimelineEntity();
        String containerId = "container_1000178881110_2002_" + i;
        entity.setId(containerId);
        String entityType = "TEST_" + j;
        entity.setType(entityType);
        long cTime = 1425016501000L;
        entity.setCreatedTime(cTime);

        // add metrics
        Set<TimelineMetric> metrics = new HashSet<>();
        TimelineMetric m1 = new TimelineMetric();
        m1.setId("HDFS_BYTES_WRITE");
        m1.setRealtimeAggregationOp(TimelineMetricOperation.SUM);
        long ts = System.currentTimeMillis();
        m1.addValue(ts - 20000, 100L);
        metrics.add(m1);

        TimelineMetric m2 = new TimelineMetric();
        m2.setId("VCORES_USED");
        m2.setRealtimeAggregationOp(TimelineMetricOperation.SUM);
        m2.addValue(ts - 20000, 3L);
        metrics.add(m2);

        // m3 should not show up in the aggregation
        TimelineMetric m3 = new TimelineMetric();
        m3.setId("UNRELATED_VALUES");
        m3.addValue(ts - 20000, 3L);
        metrics.add(m3);

        TimelineMetric m4 = new TimelineMetric();
        m4.setId("TXN_FINISH_TIME");
        m4.setRealtimeAggregationOp(TimelineMetricOperation.MAX);
        m4.addValue(ts - 20000, i);
        metrics.add(m4);

        entity.addMetrics(metrics);
        te.addEntity(entity);
      }
    }

    return te;
  }

  @Test
  public void testAggregation() throws Exception {
    // Test aggregation with multiple groups.
    int groups = 3;
    int n = 50;
    TimelineEntities testEntities = generateTestEntities(groups, n);
    TimelineEntity resultEntity = TimelineCollector.aggregateEntities(
        testEntities, "test_result", "TEST_AGGR", true);
    assertEquals(resultEntity.getMetrics().size(), groups * 3);

    for (int i = 0; i < groups; i++) {
      Set<TimelineMetric> metrics = resultEntity.getMetrics();
      for (TimelineMetric m : metrics) {
        if (m.getId().startsWith("HDFS_BYTES_WRITE")) {
          assertEquals(100 * n, m.getSingleDataValue().intValue());
        } else if (m.getId().startsWith("VCORES_USED")) {
          assertEquals(3 * n, m.getSingleDataValue().intValue());
        } else if (m.getId().startsWith("TXN_FINISH_TIME")) {
          assertEquals(n - 1, m.getSingleDataValue());
        } else {
          fail("Unrecognized metric! " + m.getId());
        }
      }
    }

    // Test aggregation with a single group.
    TimelineEntities testEntities1 = generateTestEntities(1, n);
    TimelineEntity resultEntity1 = TimelineCollector.aggregateEntities(
        testEntities1, "test_result", "TEST_AGGR", false);
    assertEquals(resultEntity1.getMetrics().size(), 3);

    Set<TimelineMetric> metrics = resultEntity1.getMetrics();
    for (TimelineMetric m : metrics) {
      if (m.getId().equals("HDFS_BYTES_WRITE")) {
        assertEquals(100 * n, m.getSingleDataValue().intValue());
      } else if (m.getId().equals("VCORES_USED")) {
        assertEquals(3 * n, m.getSingleDataValue().intValue());
      } else if (m.getId().equals("TXN_FINISH_TIME")) {
        assertEquals(n - 1, m.getSingleDataValue());
      } else {
        fail("Unrecognized metric! " + m.getId());
      }
    }

  }

  /**
   * Test TimelineCollector's interaction with TimelineWriter upon
   * putEntity() calls.
   */
  @Test
  public void testPutEntity() throws IOException {
    TimelineWriter writer = mock(TimelineWriter.class);
    TimelineCollector collector = new TimelineCollectorForTest(writer);

    TimelineEntities entities = generateTestEntities(1, 1);
    collector.putEntities(
        entities, UserGroupInformation.createRemoteUser("test-user"));

    verify(writer, times(1)).write(
        anyString(), anyString(), anyString(), anyString(), anyLong(),
        anyString(), any(TimelineEntities.class));
    verify(writer, times(1)).flush();
  }

  /**
   * Test TimelineCollector's interaction with TimelineWriter upon
   * putEntityAsync() calls.
   */
  @Test
  public void testPutEntityAsync() throws IOException {
    TimelineWriter writer = mock(TimelineWriter.class);
    TimelineCollector collector = new TimelineCollectorForTest(writer);

    TimelineEntities entities = generateTestEntities(1, 1);
    collector.putEntitiesAsync(
        entities, UserGroupInformation.createRemoteUser("test-user"));

    verify(writer, times(1)).write(
        anyString(), anyString(), anyString(), anyString(), anyLong(),
        anyString(), any(TimelineEntities.class));
    verify(writer, never()).flush();
  }

  private static class TimelineCollectorForTest extends TimelineCollector {
    private final TimelineCollectorContext context =
        new TimelineCollectorContext();

    TimelineCollectorForTest(TimelineWriter writer) {
      super("TimelineCollectorForTest");
      setWriter(writer);
    }

    @Override
    public TimelineCollectorContext getTimelineEntityContext() {
      return context;
    }
  }
}
