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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

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

    Assertions.assertEquals(1, actualEntityDoc.getInfo().size());
    Assertions.assertEquals(0, actualEntityDoc.getMetrics().size());
    Assertions.assertEquals(0, actualEntityDoc.getEvents().size());
    Assertions.assertEquals(0, actualEntityDoc.getConfigs().size());
    Assertions.assertEquals(0, actualEntityDoc.getIsRelatedToEntities().size());
    Assertions.assertEquals(0, actualEntityDoc.getRelatesToEntities().size());

    actualEntityDoc.merge(expectedEntityDoc);

    Assertions.assertEquals(expectedEntityDoc.getInfo().size(),
        actualEntityDoc.getInfo().size());
    Assertions.assertEquals(expectedEntityDoc.getMetrics().size(),
        actualEntityDoc.getMetrics().size());
    Assertions.assertEquals(expectedEntityDoc.getEvents().size(),
        actualEntityDoc.getEvents().size());
    Assertions.assertEquals(expectedEntityDoc.getConfigs().size(),
        actualEntityDoc.getConfigs().size());
    Assertions.assertEquals(expectedEntityDoc.getRelatesToEntities().size(),
        actualEntityDoc.getIsRelatedToEntities().size());
    Assertions.assertEquals(expectedEntityDoc.getRelatesToEntities().size(),
        actualEntityDoc.getRelatesToEntities().size());
  }

  @Test
  public void testFlowActivityDocMergeOperation() throws IOException {
    FlowActivityDocument actualFlowActivityDoc = new FlowActivityDocument();
    FlowActivityDocument expectedFlowActivityDoc =
        DocumentStoreTestUtils.bakeFlowActivityDoc();

    Assertions.assertEquals(0, actualFlowActivityDoc.getDayTimestamp());
    Assertions.assertEquals(0, actualFlowActivityDoc.getFlowActivities().size());
    Assertions.assertNull(actualFlowActivityDoc.getFlowName());
    Assertions.assertEquals(TimelineEntityType.YARN_FLOW_ACTIVITY.toString(),
        actualFlowActivityDoc.getType());
    Assertions.assertNull(actualFlowActivityDoc.getUser());
    Assertions.assertNull(actualFlowActivityDoc.getId());

    actualFlowActivityDoc.merge(expectedFlowActivityDoc);

    Assertions.assertEquals(expectedFlowActivityDoc.getDayTimestamp(),
        actualFlowActivityDoc.getDayTimestamp());
    Assertions.assertEquals(expectedFlowActivityDoc.getFlowActivities().size(),
        actualFlowActivityDoc.getFlowActivities().size());
    Assertions.assertEquals(expectedFlowActivityDoc.getFlowName(),
        actualFlowActivityDoc.getFlowName());
    Assertions.assertEquals(expectedFlowActivityDoc.getType(),
        actualFlowActivityDoc.getType());
    Assertions.assertEquals(expectedFlowActivityDoc.getUser(),
        actualFlowActivityDoc.getUser());
    Assertions.assertEquals(expectedFlowActivityDoc.getId(),
        actualFlowActivityDoc.getId());

    expectedFlowActivityDoc.addFlowActivity(FLOW_NAME,
        FLOW_VERSION, System.currentTimeMillis());

    actualFlowActivityDoc.merge(expectedFlowActivityDoc);

    Assertions.assertEquals(expectedFlowActivityDoc.getDayTimestamp(),
        actualFlowActivityDoc.getDayTimestamp());
    Assertions.assertEquals(expectedFlowActivityDoc.getFlowActivities().size(),
        actualFlowActivityDoc.getFlowActivities().size());
    Assertions.assertEquals(expectedFlowActivityDoc.getFlowName(),
        actualFlowActivityDoc.getFlowName());
    Assertions.assertEquals(expectedFlowActivityDoc.getType(),
        actualFlowActivityDoc.getType());
    Assertions.assertEquals(expectedFlowActivityDoc.getUser(),
        actualFlowActivityDoc.getUser());
    Assertions.assertEquals(expectedFlowActivityDoc.getId(),
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

    Assertions.assertNull(actualFlowRunDoc.getClusterId());
    Assertions.assertNull(actualFlowRunDoc.getFlowName());
    Assertions.assertNull(actualFlowRunDoc.getFlowRunId());
    Assertions.assertNull(actualFlowRunDoc.getFlowVersion());
    Assertions.assertNull(actualFlowRunDoc.getId());
    Assertions.assertNull(actualFlowRunDoc.getUsername());
    Assertions.assertEquals(actualFlowRunDoc.getType(), TimelineEntityType.
        YARN_FLOW_RUN.toString());
    Assertions.assertEquals(0, actualFlowRunDoc.getMinStartTime());
    Assertions.assertEquals(0, actualFlowRunDoc.getMaxEndTime());
    Assertions.assertEquals(0, actualFlowRunDoc.getMetrics().size());

    actualFlowRunDoc.merge(expectedFlowRunDoc);

    Assertions.assertEquals(expectedFlowRunDoc.getClusterId(),
        actualFlowRunDoc.getClusterId());
    Assertions.assertEquals(expectedFlowRunDoc.getFlowName(),
        actualFlowRunDoc.getFlowName());
    Assertions.assertEquals(expectedFlowRunDoc.getFlowRunId(),
        actualFlowRunDoc.getFlowRunId());
    Assertions.assertEquals(expectedFlowRunDoc.getFlowVersion(),
        actualFlowRunDoc.getFlowVersion());
    Assertions.assertEquals(expectedFlowRunDoc.getId(), actualFlowRunDoc.getId());
    Assertions.assertEquals(expectedFlowRunDoc.getUsername(),
        actualFlowRunDoc.getUsername());
    Assertions.assertEquals(expectedFlowRunDoc.getType(),
        actualFlowRunDoc.getType());
    Assertions.assertEquals(expectedFlowRunDoc.getMinStartTime(),
        actualFlowRunDoc.getMinStartTime());
    Assertions.assertEquals(expectedFlowRunDoc.getMaxEndTime(),
        actualFlowRunDoc.getMaxEndTime());
    Assertions.assertEquals(expectedFlowRunDoc.getMetrics().size(),
        actualFlowRunDoc.getMetrics().size());

    actualFlowRunDoc.merge(expectedFlowRunDoc);

    Assertions.assertEquals(value + value, actualFlowRunDoc.getMetrics()
        .get(MEMORY_ID).getSingleDataValue());
  }
}