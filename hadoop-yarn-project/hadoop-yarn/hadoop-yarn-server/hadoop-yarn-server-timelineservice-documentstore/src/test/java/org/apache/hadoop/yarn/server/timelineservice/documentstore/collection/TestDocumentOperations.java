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
import org.junit.Assert;
import org.junit.Test;

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

    Assert.assertEquals(1, actualEntityDoc.getInfo().size());
    Assert.assertEquals(0, actualEntityDoc.getMetrics().size());
    Assert.assertEquals(0, actualEntityDoc.getEvents().size());
    Assert.assertEquals(0, actualEntityDoc.getConfigs().size());
    Assert.assertEquals(0, actualEntityDoc.getIsRelatedToEntities().size());
    Assert.assertEquals(0, actualEntityDoc.getRelatesToEntities().size());

    actualEntityDoc.merge(expectedEntityDoc);

    Assert.assertEquals(expectedEntityDoc.getInfo().size(),
        actualEntityDoc.getInfo().size());
    Assert.assertEquals(expectedEntityDoc.getMetrics().size(),
        actualEntityDoc.getMetrics().size());
    Assert.assertEquals(expectedEntityDoc.getEvents().size(),
        actualEntityDoc.getEvents().size());
    Assert.assertEquals(expectedEntityDoc.getConfigs().size(),
        actualEntityDoc.getConfigs().size());
    Assert.assertEquals(expectedEntityDoc.getRelatesToEntities().size(),
        actualEntityDoc.getIsRelatedToEntities().size());
    Assert.assertEquals(expectedEntityDoc.getRelatesToEntities().size(),
        actualEntityDoc.getRelatesToEntities().size());
  }

  @Test
  public void testFlowActivityDocMergeOperation() throws IOException {
    FlowActivityDocument actualFlowActivityDoc = new FlowActivityDocument();
    FlowActivityDocument expectedFlowActivityDoc =
        DocumentStoreTestUtils.bakeFlowActivityDoc();

    Assert.assertEquals(0, actualFlowActivityDoc.getDayTimestamp());
    Assert.assertEquals(0, actualFlowActivityDoc.getFlowActivities().size());
    Assert.assertNull(actualFlowActivityDoc.getFlowName());
    Assert.assertEquals(TimelineEntityType.YARN_FLOW_ACTIVITY.toString(),
        actualFlowActivityDoc.getType());
    Assert.assertNull(actualFlowActivityDoc.getUser());
    Assert.assertNull(actualFlowActivityDoc.getId());

    actualFlowActivityDoc.merge(expectedFlowActivityDoc);

    Assert.assertEquals(expectedFlowActivityDoc.getDayTimestamp(),
        actualFlowActivityDoc.getDayTimestamp());
    Assert.assertEquals(expectedFlowActivityDoc.getFlowActivities().size(),
        actualFlowActivityDoc.getFlowActivities().size());
    Assert.assertEquals(expectedFlowActivityDoc.getFlowName(),
        actualFlowActivityDoc.getFlowName());
    Assert.assertEquals(expectedFlowActivityDoc.getType(),
        actualFlowActivityDoc.getType());
    Assert.assertEquals(expectedFlowActivityDoc.getUser(),
        actualFlowActivityDoc.getUser());
    Assert.assertEquals(expectedFlowActivityDoc.getId(),
        actualFlowActivityDoc.getId());

    expectedFlowActivityDoc.addFlowActivity(FLOW_NAME,
        FLOW_VERSION, System.currentTimeMillis());

    actualFlowActivityDoc.merge(expectedFlowActivityDoc);

    Assert.assertEquals(expectedFlowActivityDoc.getDayTimestamp(),
        actualFlowActivityDoc.getDayTimestamp());
    Assert.assertEquals(expectedFlowActivityDoc.getFlowActivities().size(),
        actualFlowActivityDoc.getFlowActivities().size());
    Assert.assertEquals(expectedFlowActivityDoc.getFlowName(),
        actualFlowActivityDoc.getFlowName());
    Assert.assertEquals(expectedFlowActivityDoc.getType(),
        actualFlowActivityDoc.getType());
    Assert.assertEquals(expectedFlowActivityDoc.getUser(),
        actualFlowActivityDoc.getUser());
    Assert.assertEquals(expectedFlowActivityDoc.getId(),
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

    Assert.assertNull(actualFlowRunDoc.getClusterId());
    Assert.assertNull(actualFlowRunDoc.getFlowName());
    Assert.assertNull(actualFlowRunDoc.getFlowRunId());
    Assert.assertNull(actualFlowRunDoc.getFlowVersion());
    Assert.assertNull(actualFlowRunDoc.getId());
    Assert.assertNull(actualFlowRunDoc.getUsername());
    Assert.assertEquals(actualFlowRunDoc.getType(), TimelineEntityType.
        YARN_FLOW_RUN.toString());
    Assert.assertEquals(0, actualFlowRunDoc.getMinStartTime());
    Assert.assertEquals(0, actualFlowRunDoc.getMaxEndTime());
    Assert.assertEquals(0, actualFlowRunDoc.getMetrics().size());

    actualFlowRunDoc.merge(expectedFlowRunDoc);

    Assert.assertEquals(expectedFlowRunDoc.getClusterId(),
        actualFlowRunDoc.getClusterId());
    Assert.assertEquals(expectedFlowRunDoc.getFlowName(),
        actualFlowRunDoc.getFlowName());
    Assert.assertEquals(expectedFlowRunDoc.getFlowRunId(),
        actualFlowRunDoc.getFlowRunId());
    Assert.assertEquals(expectedFlowRunDoc.getFlowVersion(),
        actualFlowRunDoc.getFlowVersion());
    Assert.assertEquals(expectedFlowRunDoc.getId(), actualFlowRunDoc.getId());
    Assert.assertEquals(expectedFlowRunDoc.getUsername(),
        actualFlowRunDoc.getUsername());
    Assert.assertEquals(expectedFlowRunDoc.getType(),
        actualFlowRunDoc.getType());
    Assert.assertEquals(expectedFlowRunDoc.getMinStartTime(),
        actualFlowRunDoc.getMinStartTime());
    Assert.assertEquals(expectedFlowRunDoc.getMaxEndTime(),
        actualFlowRunDoc.getMaxEndTime());
    Assert.assertEquals(expectedFlowRunDoc.getMetrics().size(),
        actualFlowRunDoc.getMetrics().size());

    actualFlowRunDoc.merge(expectedFlowRunDoc);

    Assert.assertEquals(value + value, actualFlowRunDoc.getMetrics()
        .get(MEMORY_ID).getSingleDataValue());
  }
}