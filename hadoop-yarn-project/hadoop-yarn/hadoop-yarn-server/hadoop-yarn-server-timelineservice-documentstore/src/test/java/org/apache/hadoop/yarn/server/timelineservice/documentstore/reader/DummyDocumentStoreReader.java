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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.reader;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreTestUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun.FlowRunDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Dummy Document Store Reader for mocking backend calls for unit test.
 */
public class DummyDocumentStoreReader<TimelineDoc extends TimelineDocument>
    implements DocumentStoreReader<TimelineDoc> {

  private final TimelineEntityDocument entityDoc;
  private final List<TimelineEntityDocument> entityDocs;
  private final FlowRunDocument flowRunDoc;
  private final FlowActivityDocument flowActivityDoc;

  public DummyDocumentStoreReader() {
    try {
      entityDoc = DocumentStoreTestUtils.bakeTimelineEntityDoc();
      entityDocs = DocumentStoreTestUtils.bakeYarnAppTimelineEntities();
      flowRunDoc = DocumentStoreTestUtils.bakeFlowRunDoc();
      flowActivityDoc = DocumentStoreTestUtils.bakeFlowActivityDoc();
    } catch (IOException e) {
      throw new RuntimeException("Unable to create " +
          "DummyDocumentStoreReader : ", e);
    }
  }


  @Override
  @SuppressWarnings("unchecked")
  public TimelineDoc readDocument(String collectionName, TimelineReaderContext
      context, Class<TimelineDoc> docClass) {
    switch (TimelineEntityType.valueOf(context.getEntityType())) {
    case YARN_FLOW_ACTIVITY:
      return (TimelineDoc) flowActivityDoc;
    case YARN_FLOW_RUN:
      return (TimelineDoc) flowRunDoc;
    default:
      return (TimelineDoc) entityDoc;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<TimelineDoc> readDocumentList(String collectionName,
      TimelineReaderContext context, Class<TimelineDoc> docClass, long size) {

    switch (TimelineEntityType.valueOf(context.getEntityType())) {
    case YARN_FLOW_ACTIVITY:
      List<FlowActivityDocument> flowActivityDocs = new ArrayList<>();
      flowActivityDocs.add(flowActivityDoc);
      if (size > flowActivityDocs.size()) {
        size = flowActivityDocs.size();
      }
      return (List<TimelineDoc>) flowActivityDocs.subList(0, (int) size);
    case YARN_FLOW_RUN:
      List<FlowRunDocument> flowRunDocs = new ArrayList<>();
      flowRunDocs.add(flowRunDoc);
      if (size > flowRunDocs.size()) {
        size = flowRunDocs.size();
      }
      return (List<TimelineDoc>) flowRunDocs.subList(0, (int) size);
    case YARN_APPLICATION:
      List<TimelineEntityDocument> applicationEntities = new ArrayList<>();
      applicationEntities.add(entityDoc);
      if (size > applicationEntities.size()) {
        size = applicationEntities.size();
      }
      return (List<TimelineDoc>) applicationEntities.subList(0, (int) size);
    default:
      if (size > entityDocs.size() || size == -1) {
        size = entityDocs.size();
      }
      return (List<TimelineDoc>) entityDocs.subList(0, (int) size);
    }
  }

  @Override
  public Set<String> fetchEntityTypes(String collectionName,
      TimelineReaderContext context) {
    return entityDocs.stream().map(TimelineEntityDocument::getType)
        .collect(Collectors.toSet());
  }

  @Override
  public void close() {
  }
}