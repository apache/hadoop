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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivitySubDoc;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun.FlowRunDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreFactory;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * This is a generic Collection reader for reading documents belonging to a
 * {@link CollectionType} under a specific {@link DocumentStoreVendor} backend.
 */
public  class TimelineCollectionReader {

  private static final Logger LOG = LoggerFactory
      .getLogger(TimelineCollectionReader.class);

  private final DocumentStoreReader<TimelineEntityDocument>
      genericEntityDocReader;
  private final DocumentStoreReader<FlowRunDocument>
      flowRunDocReader;
  private final DocumentStoreReader<FlowActivityDocument>
      flowActivityDocReader;

  public TimelineCollectionReader(
      Configuration conf) throws YarnException {
    LOG.info("Initializing TimelineCollectionReader...");
    genericEntityDocReader = DocumentStoreFactory
        .createDocumentStoreReader(conf);
    flowRunDocReader = DocumentStoreFactory
        .createDocumentStoreReader(conf);
    flowActivityDocReader = DocumentStoreFactory
        .createDocumentStoreReader(conf);
  }

  /**
   * Read a document from {@link DocumentStoreVendor} backend for
   * a {@link CollectionType}.
   * @param context
   *               of the timeline reader
   * @return TimelineEntityDocument as response
   * @throws IOException on error while reading
   */
  public TimelineEntityDocument readDocument(
      TimelineReaderContext context) throws IOException {
    LOG.debug("Fetching document for entity type {}", context.getEntityType());
    switch (TimelineEntityType.valueOf(context.getEntityType())) {
    case YARN_APPLICATION:
      return genericEntityDocReader.readDocument(
          CollectionType.APPLICATION.getCollectionName(), context,
           TimelineEntityDocument.class);
    case YARN_FLOW_RUN:
      FlowRunDocument flowRunDoc = flowRunDocReader.readDocument(
          CollectionType.FLOW_RUN.getCollectionName(), context,
          FlowRunDocument.class);
      FlowRunEntity flowRun = createFlowRunEntity(flowRunDoc);
      return new TimelineEntityDocument(flowRun);
    case YARN_FLOW_ACTIVITY:
      FlowActivityDocument flowActivityDoc = flowActivityDocReader
          .readDocument(CollectionType.FLOW_RUN.getCollectionName(),
              context, FlowActivityDocument.class);
      FlowActivityEntity flowActivity = createFlowActivityEntity(context,
          flowActivityDoc);
      return  new TimelineEntityDocument(flowActivity);
    default:
      return genericEntityDocReader.readDocument(
          CollectionType.ENTITY.getCollectionName(), context,
          TimelineEntityDocument.class);
    }
  }

  /**
   * Read a list of  documents from {@link DocumentStoreVendor} backend for
   * a {@link CollectionType}.
   * @param context
   *               of the timeline reader
   * @param documentsSize
   *               to limit
   * @return List of TimelineEntityDocument as response
   * @throws IOException on error while reading
   */
  public List<TimelineEntityDocument> readDocuments(
      TimelineReaderContext context, long documentsSize) throws IOException {
    List<TimelineEntityDocument> entityDocs = new ArrayList<>();
    LOG.debug("Fetching documents for entity type {}", context.getEntityType());
    switch (TimelineEntityType.valueOf(context.getEntityType())) {
    case YARN_APPLICATION:
      return genericEntityDocReader.readDocumentList(
          CollectionType.APPLICATION.getCollectionName(), context,
           TimelineEntityDocument.class, documentsSize);
    case YARN_FLOW_RUN:
      List<FlowRunDocument> flowRunDocs = flowRunDocReader.readDocumentList(
          CollectionType.FLOW_RUN.getCollectionName(), context,
               FlowRunDocument.class, documentsSize);
      for (FlowRunDocument flowRunDoc : flowRunDocs) {
        entityDocs.add(new TimelineEntityDocument(createFlowRunEntity(
            flowRunDoc)));
      }
      return entityDocs;
    case YARN_FLOW_ACTIVITY:
      List<FlowActivityDocument> flowActivityDocs = flowActivityDocReader
          .readDocumentList(CollectionType.FLOW_ACTIVITY.getCollectionName(),
              context, FlowActivityDocument.class, documentsSize);
      for(FlowActivityDocument flowActivityDoc : flowActivityDocs) {
        entityDocs.add(new TimelineEntityDocument(
            createFlowActivityEntity(context, flowActivityDoc)));
      }
      return entityDocs;
    default:
      return genericEntityDocReader.readDocumentList(
          CollectionType.ENTITY.getCollectionName(), context,
          TimelineEntityDocument.class, documentsSize);
    }
  }

  /**
   * Fetches the list of Entity Types i.e (YARN_CONTAINER,
   * YARN_APPLICATION_ATTEMPT etc.) for an application Id.
   * @param context
   *               of the timeline reader
   * @return List of EntityTypes as response
   */
  public Set<String> fetchEntityTypes(
      TimelineReaderContext context) {
    LOG.debug("Fetching all entity-types for appId : {}", context.getAppId());
    return genericEntityDocReader.fetchEntityTypes(
        CollectionType.ENTITY.getCollectionName(), context);
  }

  private FlowActivityEntity createFlowActivityEntity(
      TimelineReaderContext context, FlowActivityDocument flowActivityDoc) {
    FlowActivityEntity flowActivity = new FlowActivityEntity(
        context.getClusterId(), flowActivityDoc.getDayTimestamp(),
        flowActivityDoc.getUser(), flowActivityDoc.getFlowName());
    flowActivity.setId(flowActivityDoc.getId());
    // get the list of run ids along with the version that are associated with
    // this flow on this day
    for (FlowActivitySubDoc activity : flowActivityDoc
        .getFlowActivities()) {
      FlowRunEntity flowRunEntity = new FlowRunEntity();
      flowRunEntity.setUser(flowActivityDoc.getUser());
      flowRunEntity.setName(activity.getFlowName());
      flowRunEntity.setRunId(activity.getFlowRunId());
      flowRunEntity.setVersion(activity.getFlowVersion());
      flowRunEntity.setId(flowRunEntity.getId());
      flowActivity.addFlowRun(flowRunEntity);
    }
    flowActivity.getInfo().put(TimelineReaderUtils.FROMID_KEY,
        flowActivityDoc.getId());
    flowActivity.setCreatedTime(flowActivityDoc.getDayTimestamp());
    return flowActivity;
  }

  private FlowRunEntity createFlowRunEntity(FlowRunDocument flowRunDoc) {
    FlowRunEntity flowRun = new FlowRunEntity();
    flowRun.setRunId(flowRunDoc.getFlowRunId());
    flowRun.setUser(flowRunDoc.getUsername());
    flowRun.setName(flowRunDoc.getFlowName());

    // read the start time
    if (flowRunDoc.getMinStartTime() > 0) {
      flowRun.setStartTime(flowRunDoc.getMinStartTime());
    }

    // read the end time if available
    if (flowRunDoc.getMaxEndTime() > 0) {
      flowRun.setMaxEndTime(flowRunDoc.getMaxEndTime());
    }

    // read the flow version
    if (!DocumentStoreUtils.isNullOrEmpty(flowRunDoc.getFlowVersion())) {
      flowRun.setVersion(flowRunDoc.getFlowVersion());
    }
    flowRun.setMetrics(flowRunDoc.fetchTimelineMetrics());
    flowRun.setId(flowRunDoc.getId());
    flowRun.getInfo().put(TimelineReaderUtils.FROMID_KEY, flowRunDoc.getId());
    return flowRun;
  }

  public void close() throws Exception {
    genericEntityDocReader.close();
    flowRunDocReader.close();
    flowActivityDocReader.close();
  }
}