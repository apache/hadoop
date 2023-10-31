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

import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelinePrefixFilter;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEventSubDoc;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineMetricSubDoc;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun.FlowRunDocument;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * This class consists of all the utils required for reading or writing
 * documents for a {@link DocumentStoreVendor}.
 */
public final class DocumentStoreUtils {

  private DocumentStoreUtils(){}

  /** milliseconds in one day. */
  private static final long MILLIS_ONE_DAY = 86400000L;

  private static final String TIMELINE_STORE_TYPE =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "document-store-type";
  static final String TIMELINE_SERVICE_COSMOSDB_ENDPOINT =
      "yarn.timeline-service.document-store.cosmos-db.endpoint";
  static final String TIMELINE_SERVICE_COSMOSDB_MASTER_KEY =
      "yarn.timeline-service.document-store.cosmos-db.masterkey";
  static final String TIMELINE_SERVICE_DOCUMENTSTORE_DATABASE_NAME =
      "yarn.timeline-service.document-store.db-name";
  private static final String
      DEFAULT_TIMELINE_SERVICE_DOCUMENTSTORE_DATABASE_NAME = "timeline_service";

  /**
   * Checks whether the cosmosdb conf are set properly in yarn-site.xml conf.
   * @param conf
   *             related to yarn
   * @throws YarnException if required config properties are missing
   */
  public static void validateCosmosDBConf(Configuration conf)
      throws YarnException {
    if (conf == null) {
      throw new NullPointerException("Configuration cannot be null");
    }
    if (isNullOrEmpty(conf.get(TIMELINE_SERVICE_COSMOSDB_ENDPOINT),
        conf.get(TIMELINE_SERVICE_COSMOSDB_MASTER_KEY))) {
      throw new YarnException("One or more CosmosDB configuration property is" +
          " missing in yarn-site.xml");
    }
  }

  /**
   * Retrieves {@link DocumentStoreVendor} configured.
   * @param conf
   *             related to yarn
   * @return Returns the {@link DocumentStoreVendor} that is configured, else
   *         uses {@link DocumentStoreVendor#COSMOS_DB} as default
   */
  public static DocumentStoreVendor getStoreVendor(Configuration conf) {
    return DocumentStoreVendor.getStoreType(conf.get(TIMELINE_STORE_TYPE,
        DocumentStoreVendor.COSMOS_DB.name()));
  }

  /**
   * Retrieves a {@link TimelineEvent} from {@link TimelineEntity#events}.
   * @param timelineEntity
   *                      from which the set of events are examined.
   * @param eventType
   *                that has to be checked.
   * @return {@link TimelineEvent} if found else null
   */
  public static TimelineEvent fetchEvent(TimelineEntity timelineEntity,
      String eventType) {
    for (TimelineEvent event : timelineEntity.getEvents()) {
      if (event.getId().equals(eventType)) {
        return event;
      }
    }
    return null;
  }

  /**
   * Checks if the string is null or empty.
   * @param values
   *             array of string to be checked
   * @return false if any of the string is null or empty else true
   */
  public static boolean isNullOrEmpty(String...values) {
    if (values == null || values.length == 0) {
      return true;
    }

    for (String value : values) {
      if (value == null || value.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Creates CosmosDB Async Document Client.
   * @param conf
   *          to retrieve cosmos db endpoint and key
   * @return async document client for CosmosDB
   */
  public static AsyncDocumentClient createCosmosDBAsyncClient(
      Configuration conf){
    return new AsyncDocumentClient.Builder()
      .withServiceEndpoint(DocumentStoreUtils.getCosmosDBEndpoint(conf))
      .withMasterKeyOrResourceToken(
          DocumentStoreUtils.getCosmosDBMasterKey(conf))
      .withConnectionPolicy(ConnectionPolicy.GetDefault())
      .withConsistencyLevel(ConsistencyLevel.Session)
      .build();
  }

  /**
   * Returns the timestamp of the day's start (which is midnight 00:00:00 AM)
   * for a given input timestamp.
   *
   * @param timeStamp Timestamp.
   * @return timestamp of that day's beginning (midnight)
   */
  public static long getTopOfTheDayTimestamp(long timeStamp) {
    return timeStamp - (timeStamp % MILLIS_ONE_DAY);
  }

  /**
   * Creates a composite key for storing {@link TimelineEntityDocument}.
   * @param collectorContext
   *              of the timeline writer
   * @param type
   *            of the entity
   * @return composite key delimited with !
   */
  public static String constructTimelineEntityDocId(TimelineCollectorContext
      collectorContext, String type) {
    return String.format("%s!%s!%s!%d!%s!%s",
        collectorContext.getClusterId(), collectorContext.getUserId(),
        collectorContext.getFlowName(), collectorContext.getFlowRunId(),
        collectorContext.getAppId(), type);
  }

  /**
   * Creates a composite key for storing {@link TimelineEntityDocument}.
   * @param collectorContext
   *              of the timeline writer
   * @param type
   *            of the entity
   * @param id
   *            of the entity
   * @return composite key delimited with !
   */
  public static String constructTimelineEntityDocId(TimelineCollectorContext
      collectorContext, String type, String id) {
    return String.format("%s!%s!%s!%d!%s!%s!%s",
        collectorContext.getClusterId(), collectorContext.getUserId(),
        collectorContext.getFlowName(), collectorContext.getFlowRunId(),
        collectorContext.getAppId(), type, id);
  }

  /**
   * Creates a composite key for storing {@link FlowRunDocument}.
   * @param collectorContext
   *              of the timeline writer
   * @return composite key delimited with !
   */
  public static String constructFlowRunDocId(TimelineCollectorContext
      collectorContext) {
    return String.format("%s!%s!%s!%s", collectorContext.getClusterId(),
        collectorContext.getUserId(), collectorContext.getFlowName(),
        collectorContext.getFlowRunId());
  }

  /**
   * Creates a composite key for storing {@link FlowActivityDocument}.
   * @param collectorContext
   *              of the timeline writer
   * @param eventTimestamp
   *              of the timeline entity
   * @return composite key delimited with !
   */
  public static String constructFlowActivityDocId(TimelineCollectorContext
      collectorContext, long eventTimestamp) {
    return String.format("%s!%s!%s!%s", collectorContext.getClusterId(),
        getTopOfTheDayTimestamp(eventTimestamp),
        collectorContext.getUserId(), collectorContext.getFlowName());
  }

  private static String getCosmosDBEndpoint(Configuration conf) {
    return conf.get(TIMELINE_SERVICE_COSMOSDB_ENDPOINT);
  }

  private static String getCosmosDBMasterKey(Configuration conf) {
    return conf.get(TIMELINE_SERVICE_COSMOSDB_MASTER_KEY);
  }

  public static String getCosmosDBDatabaseName(Configuration conf) {
    return conf.get(TIMELINE_SERVICE_DOCUMENTSTORE_DATABASE_NAME,
        getDefaultTimelineServiceDBName(conf));
  }

  private static String getDefaultTimelineServiceDBName(
      Configuration conf) {
    return getClusterId(conf) + "_" +
        DEFAULT_TIMELINE_SERVICE_DOCUMENTSTORE_DATABASE_NAME;
  }

  private static String getClusterId(Configuration conf) {
    return conf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
  }

  private static boolean isTimeInRange(long time, long timeBegin,
      long timeEnd) {
    return (time >= timeBegin) && (time <= timeEnd);
  }

  /**
   * Checks if the {@link TimelineEntityFilters} are not matching for a given
   * {@link TimelineEntity}.
   * @param filters
   *              that has to be checked for an entity
   * @param timelineEntity
 *                for which the filters would be applied
   * @return true if any one of the filter is not matching else false
   * @throws IOException if an unsupported filter is being matched.
   */
  static boolean isFilterNotMatching(TimelineEntityFilters filters,
      TimelineEntity timelineEntity) throws IOException {
    if (timelineEntity.getCreatedTime() != null && !isTimeInRange(timelineEntity
        .getCreatedTime(), filters.getCreatedTimeBegin(),
        filters.getCreatedTimeEnd())) {
      return true;
    }

    if (filters.getRelatesTo() != null &&
        !filters.getRelatesTo().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchRelatesTo(timelineEntity,
            filters.getRelatesTo())) {
      return true;
    }

    if (filters.getIsRelatedTo() != null &&
        !filters.getIsRelatedTo().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchIsRelatedTo(timelineEntity,
            filters.getIsRelatedTo())) {
      return true;
    }

    if (filters.getInfoFilters() != null &&
        !filters.getInfoFilters().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchInfoFilters(timelineEntity,
            filters.getInfoFilters())) {
      return true;
    }

    if (filters.getConfigFilters() != null &&
        !filters.getConfigFilters().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchConfigFilters(timelineEntity,
            filters.getConfigFilters())) {
      return true;
    }

    if (filters.getMetricFilters() != null &&
        !filters.getMetricFilters().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchMetricFilters(timelineEntity,
            filters.getMetricFilters())) {
      return true;
    }

    return filters.getEventFilters() != null &&
        !filters.getEventFilters().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchEventFilters(timelineEntity,
            filters.getEventFilters());
  }

  /**
   * Creates the final entity to be returned as the result.
   * @param timelineEntityDocument
   *                         which has all the information for the entity
   * @param dataToRetrieve
   *                     specifies filters and fields to retrieve
   * @return {@link TimelineEntity} as the result
   */
  public static TimelineEntity createEntityToBeReturned(
      TimelineEntityDocument timelineEntityDocument,
      TimelineDataToRetrieve dataToRetrieve) {
    TimelineEntity entityToBeReturned = createTimelineEntity(
        timelineEntityDocument.getType(),
        timelineEntityDocument.fetchTimelineEntity());

    entityToBeReturned.setIdentifier(new TimelineEntity.Identifier(
        timelineEntityDocument.getType(), timelineEntityDocument.getId()));
    entityToBeReturned.setCreatedTime(
        timelineEntityDocument.getCreatedTime());
    entityToBeReturned.setInfo(timelineEntityDocument.getInfo());

    if (dataToRetrieve.getFieldsToRetrieve() != null) {
      fillFields(entityToBeReturned, timelineEntityDocument,
          dataToRetrieve);
    }
    return entityToBeReturned;
  }

  /**
   * Creates the final entity to be returned as the result.
   * @param timelineEntityDocument
   *                         which has all the information for the entity
   * @param confsToRetrieve
   *                     specifies config filters to be applied
   * @param metricsToRetrieve
   *                     specifies metric filters to be applied
   *
   * @return {@link TimelineEntity} as the result
   */
  public static TimelineEntity createEntityToBeReturned(
      TimelineEntityDocument timelineEntityDocument,
      TimelineFilterList confsToRetrieve,
      TimelineFilterList metricsToRetrieve) {
    TimelineEntity timelineEntity = timelineEntityDocument
        .fetchTimelineEntity();
    if (confsToRetrieve != null) {
      timelineEntity.setConfigs(DocumentStoreUtils.applyConfigFilter(
          confsToRetrieve, timelineEntity.getConfigs()));
    }
    if (metricsToRetrieve != null) {
      timelineEntity.setMetrics(DocumentStoreUtils.transformMetrics(
          metricsToRetrieve, timelineEntityDocument.getMetrics()));
    }
    return timelineEntity;
  }

  private static TimelineEntity createTimelineEntity(String type,
      TimelineEntity timelineEntity) {
    switch (TimelineEntityType.valueOf(type)) {
    case YARN_APPLICATION:
      return new ApplicationEntity();
    case YARN_FLOW_RUN:
      return new FlowRunEntity();
    case YARN_FLOW_ACTIVITY:
      FlowActivityEntity flowActivityEntity =
          (FlowActivityEntity) timelineEntity;
      FlowActivityEntity newFlowActivity = new FlowActivityEntity();
      newFlowActivity.addFlowRuns(flowActivityEntity.getFlowRuns());
      return newFlowActivity;
    default:
      return new TimelineEntity();
    }
  }

  // fetch required fields for final entity to be returned
  private static void fillFields(TimelineEntity finalEntity,
      TimelineEntityDocument entityDoc,
      TimelineDataToRetrieve dataToRetrieve) {
    EnumSet<TimelineReader.Field> fieldsToRetrieve =
        dataToRetrieve.getFieldsToRetrieve();
    if (fieldsToRetrieve.contains(TimelineReader.Field.ALL)) {
      fieldsToRetrieve = EnumSet.allOf(TimelineReader.Field.class);
    }
    for (TimelineReader.Field field : fieldsToRetrieve) {
      switch(field) {
      case CONFIGS:
        finalEntity.setConfigs(applyConfigFilter(dataToRetrieve
                .getConfsToRetrieve(), entityDoc.getConfigs()));
        break;
      case METRICS:
        finalEntity.setMetrics(transformMetrics(dataToRetrieve
                .getMetricsToRetrieve(), entityDoc.getMetrics()));
        break;
      case INFO:
        finalEntity.setInfo(entityDoc.getInfo());
        break;
      case IS_RELATED_TO:
        finalEntity.setIsRelatedToEntities(entityDoc.getIsRelatedToEntities());
        break;
      case RELATES_TO:
        finalEntity.setIsRelatedToEntities(entityDoc.getIsRelatedToEntities());
        break;
      case EVENTS:
        finalEntity.setEvents(transformEvents(entityDoc.getEvents().values()));
        break;
      default:
      }
    }
  }

  /* Transforms Collection<Set<TimelineEventSubDoc>> to
     NavigableSet<TimelineEvent> */
  private static NavigableSet<TimelineEvent> transformEvents(
      Collection<Set<TimelineEventSubDoc>> eventSetColl) {
    NavigableSet<TimelineEvent> timelineEvents = new TreeSet<>();
    for (Set<TimelineEventSubDoc> eventSubDocs : eventSetColl) {
      for (TimelineEventSubDoc eventSubDoc : eventSubDocs) {
        timelineEvents.add(eventSubDoc.fetchTimelineEvent());
      }
    }
    return timelineEvents;
  }

  public static Set<TimelineMetric> transformMetrics(
      TimelineFilterList metricsToRetrieve,
      Map<String, Set<TimelineMetricSubDoc>> metrics) {
    if (metricsToRetrieve == null ||
        hasDataToBeRetrieve(metricsToRetrieve, metrics.keySet())) {
      Set<TimelineMetric> metricSet = new HashSet<>();
      for(Set<TimelineMetricSubDoc> metricSubDocs : metrics.values()) {
        for(TimelineMetricSubDoc metricSubDoc : metricSubDocs) {
          metricSet.add(metricSubDoc.fetchTimelineMetric());
        }
      }
      return metricSet;
    }
    return new HashSet<>();
  }

  public static Map<String, String> applyConfigFilter(
      TimelineFilterList configsToRetrieve, Map<String, String> configs) {
    if (configsToRetrieve == null ||
        hasDataToBeRetrieve(configsToRetrieve, configs.keySet())) {
      return configs;
    }
    return new HashMap<>();
  }

  private static boolean hasDataToBeRetrieve(
      TimelineFilterList timelineFilters, Set<String> dataSet) {
    Set<String> dataToBeRetrieved = new HashSet<>();
    TimelinePrefixFilter timelinePrefixFilter;
    for (TimelineFilter timelineFilter : timelineFilters.getFilterList()) {
      timelinePrefixFilter = (TimelinePrefixFilter) timelineFilter;
      dataToBeRetrieved.add(timelinePrefixFilter.getPrefix());
    }
    switch (timelineFilters.getOperator()) {
    case OR:
      if (dataToBeRetrieved.size() == 0 ||
          !Collections.disjoint(dataSet, dataToBeRetrieved)) {
        return true;
      }
    case AND:
      if (dataToBeRetrieved.size() == 0 ||
          dataSet.containsAll(dataToBeRetrieved)) {
        return true;
      }
    default:
      return false;
    }
  }
}
