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

package org.apache.hadoop.yarn.server.timelineservice.documentstore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.*;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineAggregationTrack;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun.FlowRunDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.TimelineCollectionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * This is a generic document store timeline writer for storing the timeline
 * entity information. Based on the {@link DocumentStoreVendor} that is
 * configured, the documents are written to that backend.
 */
public class DocumentStoreTimelineWriterImpl extends AbstractService
    implements TimelineWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(DocumentStoreTimelineWriterImpl.class);
  private static final String DOC_ID_DELIMITER = "!";

  private DocumentStoreVendor storeType;
  private TimelineCollectionWriter<TimelineEntityDocument> appCollWriter;
  private TimelineCollectionWriter<TimelineEntityDocument>
      entityCollWriter;
  private TimelineCollectionWriter<FlowActivityDocument> flowActivityCollWriter;
  private TimelineCollectionWriter<FlowRunDocument> flowRunCollWriter;


  public DocumentStoreTimelineWriterImpl() {
    super(DocumentStoreTimelineWriterImpl.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    storeType = DocumentStoreUtils.getStoreVendor(conf);
    LOG.info("Initializing Document Store Writer for : " + storeType);
    super.serviceInit(conf);

    this.appCollWriter = new TimelineCollectionWriter<>(
        CollectionType.APPLICATION, conf);
    this.entityCollWriter = new TimelineCollectionWriter<>(
        CollectionType.ENTITY, conf);
    this.flowActivityCollWriter = new TimelineCollectionWriter<>(
        CollectionType.FLOW_ACTIVITY, conf);
    this.flowRunCollWriter = new TimelineCollectionWriter<>(
        CollectionType.FLOW_RUN, conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    appCollWriter.close();
    entityCollWriter.close();
    flowActivityCollWriter.close();
    flowRunCollWriter.close();
  }

  @Override
  public TimelineWriteResponse write(TimelineCollectorContext
      context, TimelineEntities data, UserGroupInformation callerUgi) {
    LOG.debug("Writing Timeline Entity for appID : {}", context.getAppId());
    TimelineWriteResponse putStatus = new TimelineWriteResponse();
    String subApplicationUser = callerUgi.getShortUserName();

    //Avoiding NPE for document id
    if (DocumentStoreUtils.isNullOrEmpty(context.getFlowName(),
        context.getAppId(), context.getClusterId(), context.getUserId())) {
      LOG.warn("Found NULL for one of: flowName={} appId={} " +
          "userId={} clusterId={} . Not proceeding on writing to store : " +
          storeType);
      return putStatus;
    }

    for (TimelineEntity timelineEntity : data.getEntities()) {
      // a set can have at most 1 null
      if(timelineEntity == null) {
        continue;
      }

      TimelineEntityDocument entityDocument;
      //If the entity is application, it will be stored in Application
      // Collection
      if (ApplicationEntity.isApplicationEntity(timelineEntity)) {
        entityDocument = createTimelineEntityDoc(context, subApplicationUser,
            timelineEntity, true);
        // if it's an application entity, store metrics for aggregation
        FlowRunDocument flowRunDoc = createFlowRunDoc(context,
            timelineEntity.getMetrics());
        // fetch flow activity if App is created or finished
        FlowActivityDocument flowActivityDoc = getFlowActivityDoc(context,
            timelineEntity, flowRunDoc, entityDocument);
        writeApplicationDoc(entityDocument);
        writeFlowRunDoc(flowRunDoc);
        if(flowActivityDoc != null) {
          storeFlowActivityDoc(flowActivityDoc);
        }
      } else {
        entityDocument = createTimelineEntityDoc(context, subApplicationUser,
            timelineEntity, false);
        appendSubAppUserIfExists(context, subApplicationUser);
        // The entity will be stored in Entity Collection
        entityDocument.setCreatedTime(fetchEntityCreationTime(timelineEntity));
        writeEntityDoc(entityDocument);
      }
    }
    return putStatus;
  }

  @Override
  public TimelineWriteResponse write(TimelineCollectorContext context,
      TimelineDomain domain) throws IOException {
    return null;
  }

  private void appendSubAppUserIfExists(TimelineCollectorContext context,
      String subApplicationUser) {
    String userId = context.getUserId();
    if (!userId.equals(subApplicationUser) &&
        !userId.contains(subApplicationUser)) {
      userId = userId.concat(DOC_ID_DELIMITER).concat(subApplicationUser);
      context.setUserId(userId);
    }
  }

  private TimelineEntityDocument createTimelineEntityDoc(
      TimelineCollectorContext context, String subApplicationUser,
      TimelineEntity timelineEntity, boolean isAppEntity) {
    TimelineEntityDocument entityDocument =
        new TimelineEntityDocument(timelineEntity);
    entityDocument.setContext(context);
    entityDocument.setFlowVersion(context.getFlowVersion());
    entityDocument.setSubApplicationUser(subApplicationUser);
    if (isAppEntity) {
      entityDocument.setId(DocumentStoreUtils.constructTimelineEntityDocId(
          context, timelineEntity.getType()));
    } else {
      entityDocument.setId(DocumentStoreUtils.constructTimelineEntityDocId(
          context, timelineEntity.getType(), timelineEntity.getId()));
    }
    return entityDocument;
  }

  private FlowRunDocument createFlowRunDoc(TimelineCollectorContext context,
      Set<TimelineMetric> metrics) {
    FlowRunDocument flowRunDoc = new FlowRunDocument(context, metrics);
    flowRunDoc.setFlowVersion(context.getFlowVersion());
    flowRunDoc.setId(DocumentStoreUtils.constructFlowRunDocId(context));
    return flowRunDoc;
  }

  private long fetchEntityCreationTime(TimelineEntity timelineEntity) {
    TimelineEvent event;
    switch (TimelineEntityType.valueOf(timelineEntity.getType())) {
    case YARN_CONTAINER:
      event = DocumentStoreUtils.fetchEvent(
          timelineEntity, ContainerMetricsConstants.CREATED_EVENT_TYPE);
      if (event != null) {
        return event.getTimestamp();
      }
      break;
    case YARN_APPLICATION_ATTEMPT:
      event = DocumentStoreUtils.fetchEvent(
          timelineEntity, AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE);
      if (event != null) {
        return event.getTimestamp();
      }
      break;
    default:
      //NO Op
    }
    if (timelineEntity.getCreatedTime() == null) {
      return 0;
    }
    return timelineEntity.getCreatedTime();
  }

  private FlowActivityDocument getFlowActivityDoc(
      TimelineCollectorContext context,
      TimelineEntity timelineEntity, FlowRunDocument flowRunDoc,
      TimelineEntityDocument entityDocument) {
    FlowActivityDocument flowActivityDoc = null;
    // check if the application is created
    TimelineEvent event = DocumentStoreUtils.fetchEvent(
        timelineEntity, ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    if (event != null) {
      entityDocument.setCreatedTime(event.getTimestamp());
      flowRunDoc.setMinStartTime(event.getTimestamp());
      flowActivityDoc = createFlowActivityDoc(context, context.getFlowName(),
          context.getFlowVersion(), context.getFlowRunId(), event);
    }

    // if application has finished, store it's finish time
    event = DocumentStoreUtils.fetchEvent(timelineEntity,
        ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    if (event != null) {
      flowRunDoc.setMaxEndTime(event.getTimestamp());

      // this check is to handle in case both create and finish event exist
      // under the single list of events for an TimelineEntity
      if (flowActivityDoc == null) {
        flowActivityDoc = createFlowActivityDoc(context, context.getFlowName(),
            context.getFlowVersion(), context.getFlowRunId(), event);
      }
    }
    return flowActivityDoc;
  }

  private FlowActivityDocument createFlowActivityDoc(
      TimelineCollectorContext context, String flowName, String flowVersion,
      long flowRunId, TimelineEvent event) {
    FlowActivityDocument flowActivityDoc = new FlowActivityDocument(flowName,
        flowVersion, flowRunId);
    flowActivityDoc.setDayTimestamp(DocumentStoreUtils.getTopOfTheDayTimestamp(
        event.getTimestamp()));
    flowActivityDoc.setFlowName(flowName);
    flowActivityDoc.setUser(context.getUserId());
    flowActivityDoc.setId(DocumentStoreUtils.constructFlowActivityDocId(
        context, event.getTimestamp()));
    return flowActivityDoc;
  }

  private void writeFlowRunDoc(FlowRunDocument flowRunDoc) {
    flowRunCollWriter.writeDocument(flowRunDoc);
  }

  private void storeFlowActivityDoc(FlowActivityDocument flowActivityDoc) {
    flowActivityCollWriter.writeDocument(flowActivityDoc);
  }

  private void writeEntityDoc(TimelineEntityDocument entityDocument) {
    entityCollWriter.writeDocument(entityDocument);
  }

  private void writeApplicationDoc(TimelineEntityDocument entityDocument) {
    appCollWriter.writeDocument(entityDocument);
  }

  public TimelineWriteResponse aggregate(TimelineEntity data,
      TimelineAggregationTrack track) {
    return null;
  }

  @Override
  public void flush() {
  }
}