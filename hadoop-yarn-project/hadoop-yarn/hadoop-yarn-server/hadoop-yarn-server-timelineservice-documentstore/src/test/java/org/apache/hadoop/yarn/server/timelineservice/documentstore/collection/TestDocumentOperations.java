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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.collection;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricOperation;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreTestUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineMetricSubDoc;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun.FlowRunDocument;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Timeline Entity Document merge and aggregation test.
 */
public class TestDocumentOperations {

  private static final String MEMORY_ID = "MEMORY";
  private static final String FLOW_NAME = "DistributedShell";
  private static final String FLOW_VERSION = "1";

  @Test
  public void testTimelineEntityDocMergeOperation() throws IOException {
    TimelineEntityDocument actualEntityDoc =
        new TimelineEntityDocument();
    TimelineEntityDocument expectedEntityDoc =
        DocumentStoreTestUtils.bakeTimelineEntityDoc();

    assertEquals(1, actualEntityDoc.getInfo().size());
    assertEquals(0, actualEntityDoc.getMetrics().size());
    assertEquals(0, actualEntityDoc.getEvents().size());
    assertEquals(0, actualEntityDoc.getConfigs().size());
    assertEquals(0, actualEntityDoc.getIsRelatedToEntities().size());
    assertEquals(0, actualEntityDoc.getRelatesToEntities().size());

    actualEntityDoc.merge(expectedEntityDoc);

    assertEquals(expectedEntityDoc.getInfo().size(),
        actualEntityDoc.getInfo().size());
    assertEquals(expectedEntityDoc.getMetrics().size(),
        actualEntityDoc.getMetrics().size());
    assertEquals(expectedEntityDoc.getEvents().size(),
        actualEntityDoc.getEvents().size());
    assertEquals(expectedEntityDoc.getConfigs().size(),
        actualEntityDoc.getConfigs().size());
    assertEquals(expectedEntityDoc.getRelatesToEntities().size(),
        actualEntityDoc.getIsRelatedToEntities().size());
    assertEquals(expectedEntityDoc.getRelatesToEntities().size(),
        actualEntityDoc.getRelatesToEntities().size());
  }

  @Test
  public void testFlowActivityDocMergeOperation() throws IOException {
    FlowActivityDocument actualFlowActivityDoc = new FlowActivityDocument();
    FlowActivityDocument expectedFlowActivityDoc =
        DocumentStoreTestUtils.bakeFlowActivityDoc();

    assertEquals(0, actualFlowActivityDoc.getDayTimestamp());
    assertEquals(0, actualFlowActivityDoc.getFlowActivities().size());
    assertNull(actualFlowActivityDoc.getFlowName());
    assertEquals(TimelineEntityType.YARN_FLOW_ACTIVITY.toString(),
        actualFlowActivityDoc.getType());
    assertNull(actualFlowActivityDoc.getUser());
    assertNull(actualFlowActivityDoc.getId());

    actualFlowActivityDoc.merge(expectedFlowActivityDoc);

    assertEquals(expectedFlowActivityDoc.getDayTimestamp(),
        actualFlowActivityDoc.getDayTimestamp());
    assertEquals(expectedFlowActivityDoc.getFlowActivities().size(),
        actualFlowActivityDoc.getFlowActivities().size());
    assertEquals(expectedFlowActivityDoc.getFlowName(),
        actualFlowActivityDoc.getFlowName());
    assertEquals(expectedFlowActivityDoc.getType(),
        actualFlowActivityDoc.getType());
    assertEquals(expectedFlowActivityDoc.getUser(),
        actualFlowActivityDoc.getUser());
    assertEquals(expectedFlowActivityDoc.getId(),
        actualFlowActivityDoc.getId());

    expectedFlowActivityDoc.addFlowActivity(FLOW_NAME,
        FLOW_VERSION, System.currentTimeMillis());

    actualFlowActivityDoc.merge(expectedFlowActivityDoc);

    assertEquals(expectedFlowActivityDoc.getDayTimestamp(),
        actualFlowActivityDoc.getDayTimestamp());
    assertEquals(expectedFlowActivityDoc.getFlowActivities().size(),
        actualFlowActivityDoc.getFlowActivities().size());
    assertEquals(expectedFlowActivityDoc.getFlowName(),
        actualFlowActivityDoc.getFlowName());
    assertEquals(expectedFlowActivityDoc.getType(),
        actualFlowActivityDoc.getType());
    assertEquals(expectedFlowActivityDoc.getUser(),
        actualFlowActivityDoc.getUser());
    assertEquals(expectedFlowActivityDoc.getId(),
        actualFlowActivityDoc.getId());
  }

  @Test
  public void testFlowRunDocMergeAndAggOperation() throws IOException {
    FlowRunDocument actualFlowRunDoc = new FlowRunDocument();
    FlowRunDocument expectedFlowRunDoc = DocumentStoreTestUtils
        .bakeFlowRunDoc();

    final long timestamp = System.currentTimeMillis();
    final long value = 98586624;
    TimelineMetric timelineMetric = new TimelineMetric();
    timelineMetric.setId(MEMORY_ID);
    timelineMetric.setType(TimelineMetric.Type.SINGLE_VALUE);
    timelineMetric.setRealtimeAggregationOp(TimelineMetricOperation.SUM);
    timelineMetric.addValue(timestamp, value);
    TimelineMetricSubDoc metricSubDoc = new TimelineMetricSubDoc(
        timelineMetric);
    expectedFlowRunDoc.getMetrics().put(MEMORY_ID, metricSubDoc);

    assertNull(actualFlowRunDoc.getClusterId());
    assertNull(actualFlowRunDoc.getFlowName());
    assertNull(actualFlowRunDoc.getFlowRunId());
    assertNull(actualFlowRunDoc.getFlowVersion());
    assertNull(actualFlowRunDoc.getId());
    assertNull(actualFlowRunDoc.getUsername());
    assertEquals(actualFlowRunDoc.getType(), TimelineEntityType.
        YARN_FLOW_RUN.toString());
    assertEquals(0, actualFlowRunDoc.getMinStartTime());
    assertEquals(0, actualFlowRunDoc.getMaxEndTime());
    assertEquals(0, actualFlowRunDoc.getMetrics().size());

    actualFlowRunDoc.merge(expectedFlowRunDoc);

    assertEquals(expectedFlowRunDoc.getClusterId(),
        actualFlowRunDoc.getClusterId());
    assertEquals(expectedFlowRunDoc.getFlowName(),
        actualFlowRunDoc.getFlowName());
    assertEquals(expectedFlowRunDoc.getFlowRunId(),
        actualFlowRunDoc.getFlowRunId());
    assertEquals(expectedFlowRunDoc.getFlowVersion(),
        actualFlowRunDoc.getFlowVersion());
    assertEquals(expectedFlowRunDoc.getId(), actualFlowRunDoc.getId());
    assertEquals(expectedFlowRunDoc.getUsername(),
        actualFlowRunDoc.getUsername());
    assertEquals(expectedFlowRunDoc.getType(),
        actualFlowRunDoc.getType());
    assertEquals(expectedFlowRunDoc.getMinStartTime(),
        actualFlowRunDoc.getMinStartTime());
    assertEquals(expectedFlowRunDoc.getMaxEndTime(),
        actualFlowRunDoc.getMaxEndTime());
    assertEquals(expectedFlowRunDoc.getMetrics().size(),
        actualFlowRunDoc.getMetrics().size());

    actualFlowRunDoc.merge(expectedFlowRunDoc);

    assertEquals(value + value, actualFlowRunDoc.getMetrics()
        .get(MEMORY_ID).getSingleDataValue());
  }
}