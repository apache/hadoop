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
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.TimelineCollectionReader;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This is a generic document store timeline reader for reading the timeline
 * entity information. Based on the {@link DocumentStoreVendor} that is
 * configured, the  documents are read from that backend.
 */
public class DocumentStoreTimelineReaderImpl
    extends AbstractService implements TimelineReader {

  private static final Logger LOG = LoggerFactory
      .getLogger(DocumentStoreTimelineReaderImpl.class);

  private TimelineCollectionReader collectionReader;

  public DocumentStoreTimelineReaderImpl() {
    super(DocumentStoreTimelineReaderImpl.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    DocumentStoreVendor storeType = DocumentStoreUtils.getStoreVendor(conf);
    LOG.info("Initializing Document Store Reader for : " + storeType);
    collectionReader = new TimelineCollectionReader(conf);
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
    LOG.info("Stopping Document Timeline Store reader...");
    collectionReader.close();
  }

  public TimelineEntity getEntity(TimelineReaderContext context,
      TimelineDataToRetrieve dataToRetrieve) throws IOException {
    TimelineEntityDocument timelineEntityDoc;
    switch (TimelineEntityType.valueOf(context.getEntityType())) {
    case YARN_FLOW_ACTIVITY:
    case YARN_FLOW_RUN:
      timelineEntityDoc =
          collectionReader.readDocument(context);
      return DocumentStoreUtils.createEntityToBeReturned(
          timelineEntityDoc, dataToRetrieve.getConfsToRetrieve(),
          dataToRetrieve.getMetricsToRetrieve());
    default:
      timelineEntityDoc =
          collectionReader.readDocument(context);
    }
    return DocumentStoreUtils.createEntityToBeReturned(
        timelineEntityDoc, dataToRetrieve);
  }

  public Set<TimelineEntity> getEntities(TimelineReaderContext context,
      TimelineEntityFilters filters, TimelineDataToRetrieve dataToRetrieve)
      throws IOException {
    List<TimelineEntityDocument> entityDocs =
        collectionReader.readDocuments(context, filters.getLimit());

    return applyFilters(filters, dataToRetrieve, entityDocs);
  }

  public Set<String> getEntityTypes(TimelineReaderContext context) {
    return collectionReader.fetchEntityTypes(context);
  }

  @Override
  public TimelineHealth getHealthStatus() {
    if (collectionReader != null) {
      return new TimelineHealth(TimelineHealth.TimelineHealthStatus.RUNNING,
          "");
    } else {
      return new TimelineHealth(
          TimelineHealth.TimelineHealthStatus.READER_CONNECTION_FAILURE,
          "Timeline store reader not initialized.");
    }
  }

  // for honoring all filters from {@link TimelineEntityFilters}
  private Set<TimelineEntity> applyFilters(TimelineEntityFilters filters,
      TimelineDataToRetrieve dataToRetrieve,
      List<TimelineEntityDocument> entityDocs) throws IOException {
    Set<TimelineEntity> timelineEntities = new HashSet<>();
    for (TimelineEntityDocument entityDoc : entityDocs) {
      final TimelineEntity timelineEntity = entityDoc.fetchTimelineEntity();

      if (DocumentStoreUtils.isFilterNotMatching(filters, timelineEntity)) {
        continue;
      }

      TimelineEntity entityToBeReturned = DocumentStoreUtils
          .createEntityToBeReturned(entityDoc, dataToRetrieve);
      timelineEntities.add(entityToBeReturned);
    }
    return timelineEntities;
  }
}