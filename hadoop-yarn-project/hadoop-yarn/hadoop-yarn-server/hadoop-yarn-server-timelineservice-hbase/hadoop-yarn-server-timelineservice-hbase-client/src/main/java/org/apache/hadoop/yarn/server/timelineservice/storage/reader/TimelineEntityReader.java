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
package org.apache.hadoop.yarn.server.timelineservice.storage.reader;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.EventColumnName;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.EventColumnNameConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.StringKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base class for reading and deserializing timeline entities from the
 * HBase storage. Different types can be defined for different types of the
 * entities that are being requested.
 */
public abstract class TimelineEntityReader extends
    AbstractTimelineStorageReader {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineEntityReader.class);

  private final boolean singleEntityRead;
  private TimelineDataToRetrieve dataToRetrieve;
  // used only for multiple entity read mode
  private TimelineEntityFilters filters;

  /**
   * Main table the entity reader uses.
   */
  private BaseTableRW<?> table;

  /**
   * Used to convert strings key components to and from storage format.
   */
  private final KeyConverter<String> stringKeyConverter =
      new StringKeyConverter();

  /**
   * Instantiates a reader for multiple-entity reads.
   *
   * @param ctxt Reader context which defines the scope in which query has to be
   *     made.
   * @param entityFilters Filters which limit the entities returned.
   * @param toRetrieve Data to retrieve for each entity.
   */
  protected TimelineEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt);
    this.singleEntityRead = false;
    this.dataToRetrieve = toRetrieve;
    this.filters = entityFilters;

    this.setTable(getTable());
  }

  /**
   * Instantiates a reader for single-entity reads.
   *
   * @param ctxt Reader context which defines the scope in which query has to be
   *     made.
   * @param toRetrieve Data to retrieve for each entity.
   */
  protected TimelineEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt);
    this.singleEntityRead = true;
    this.dataToRetrieve = toRetrieve;

    this.setTable(getTable());
  }

  /**
   * Creates a {@link FilterList} based on fields, confs and metrics to
   * retrieve. This filter list will be set in Scan/Get objects to trim down
   * results fetched from HBase back-end storage. This is called only for
   * multiple entity reads.
   *
   * @return a {@link FilterList} object.
   * @throws IOException if any problem occurs while creating filter list.
   */
  protected abstract FilterList constructFilterListBasedOnFields()
      throws IOException;

  /**
   * Creates a {@link FilterList} based on info, config and metric filters. This
   * filter list will be set in HBase Get to trim down results fetched from
   * HBase back-end storage.
   *
   * @return a {@link FilterList} object.
   * @throws IOException if any problem occurs while creating filter list.
   */
  protected abstract FilterList constructFilterListBasedOnFilters()
      throws IOException;

  /**
   * Combines filter lists created based on fields and based on filters.
   *
   * @return a {@link FilterList} object if it can be constructed. Returns null,
   * if filter list cannot be created either on the basis of filters or on the
   * basis of fields.
   * @throws IOException if any problem occurs while creating filter list.
   */
  private FilterList createFilterList() throws IOException {
    FilterList listBasedOnFilters = constructFilterListBasedOnFilters();
    boolean hasListBasedOnFilters = listBasedOnFilters != null &&
        !listBasedOnFilters.getFilters().isEmpty();
    FilterList listBasedOnFields = constructFilterListBasedOnFields();
    boolean hasListBasedOnFields = listBasedOnFields != null &&
        !listBasedOnFields.getFilters().isEmpty();
    // If filter lists based on both filters and fields can be created,
    // combine them in a new filter list and return it.
    // If either one of them has been created, return that filter list.
    // Return null, if none of the filter lists can be created. This indicates
    // that no filter list needs to be added to HBase Scan as filters are not
    // specified for the query or only the default view of entity needs to be
    // returned.
    if (hasListBasedOnFilters && hasListBasedOnFields) {
      FilterList list = new FilterList();
      list.addFilter(listBasedOnFilters);
      list.addFilter(listBasedOnFields);
      return list;
    } else if (hasListBasedOnFilters) {
      return listBasedOnFilters;
    } else if (hasListBasedOnFields) {
      return listBasedOnFields;
    }
    return null;
  }

  protected TimelineDataToRetrieve getDataToRetrieve() {
    return dataToRetrieve;
  }

  protected TimelineEntityFilters getFilters() {
    return filters;
  }

  /**
   * Create a {@link TimelineEntityFilters} object with default values for
   * filters.
   */
  protected void createFiltersIfNull() {
    if (filters == null) {
      filters = new TimelineEntityFilters.Builder().build();
    }
  }

  /**
   * Reads and deserializes a single timeline entity from the HBase storage.
   *
   * @param hbaseConf HBase Configuration.
   * @param conn HBase Connection.
   * @return A <cite>TimelineEntity</cite> object.
   * @throws IOException if there is any exception encountered while reading
   *     entity.
   */
  public TimelineEntity readEntity(Configuration hbaseConf, Connection conn)
      throws IOException {
    validateParams();
    augmentParams(hbaseConf, conn);

    FilterList filterList = constructFilterListBasedOnFields();
    if (LOG.isDebugEnabled() && filterList != null) {
      LOG.debug("FilterList created for get is - " + filterList);
    }
    Result result = getResult(hbaseConf, conn, filterList);
    if (result == null || result.isEmpty()) {
      // Could not find a matching row.
      LOG.info("Cannot find matching entity of type " +
          getContext().getEntityType());
      return null;
    }
    return parseEntity(result);
  }

  /**
   * Reads and deserializes a set of timeline entities from the HBase storage.
   * It goes through all the results available, and returns the number of
   * entries as specified in the limit in the entity's natural sort order.
   *
   * @param hbaseConf HBase Configuration.
   * @param conn HBase Connection.
   * @return a set of <cite>TimelineEntity</cite> objects.
   * @throws IOException if any exception is encountered while reading entities.
   */
  public Set<TimelineEntity> readEntities(Configuration hbaseConf,
      Connection conn) throws IOException {
    validateParams();
    augmentParams(hbaseConf, conn);

    Set<TimelineEntity> entities = new LinkedHashSet<>();
    FilterList filterList = createFilterList();
    if (LOG.isDebugEnabled() && filterList != null) {
      LOG.debug("FilterList created for scan is - " + filterList);
    }
    ResultScanner results = getResults(hbaseConf, conn, filterList);
    try {
      for (Result result : results) {
        TimelineEntity entity = parseEntity(result);
        if (entity == null) {
          continue;
        }
        entities.add(entity);
        if (entities.size() == filters.getLimit()) {
          break;
        }
      }
      return entities;
    } finally {
      results.close();
    }
  }

  /**
   * Returns the main table to be used by the entity reader.
   *
   * @return A reference to the table.
   */
  protected BaseTableRW<?> getTable() {
    return table;
  }

  /**
   * Fetches a {@link Result} instance for a single-entity read.
   *
   * @param hbaseConf HBase Configuration.
   * @param conn HBase Connection.
   * @param filterList filter list which will be applied to HBase Get.
   * @return the {@link Result} instance or null if no such record is found.
   * @throws IOException if any exception is encountered while getting result.
   */
  protected abstract Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException;

  /**
   * Fetches a {@link ResultScanner} for a multi-entity read.
   *
   * @param hbaseConf HBase Configuration.
   * @param conn HBase Connection.
   * @param filterList filter list which will be applied to HBase Scan.
   * @return the {@link ResultScanner} instance.
   * @throws IOException if any exception is encountered while getting results.
   */
  protected abstract ResultScanner getResults(Configuration hbaseConf,
      Connection conn, FilterList filterList) throws IOException;

  /**
   * Parses the result retrieved from HBase backend and convert it into a
   * {@link TimelineEntity} object.
   *
   * @param result Single row result of a Get/Scan.
   * @return the <cite>TimelineEntity</cite> instance or null if the entity is
   *     filtered.
   * @throws IOException if any exception is encountered while parsing entity.
   */
  protected abstract TimelineEntity parseEntity(Result result)
      throws IOException;

  /**
   * Helper method for reading and deserializing {@link TimelineMetric} objects
   * using the specified column prefix. The timeline metrics then are added to
   * the given timeline entity.
   *
   * @param entity {@link TimelineEntity} object.
   * @param result {@link Result} object retrieved from backend.
   * @param columnPrefix Metric column prefix
   * @throws IOException if any exception is encountered while reading metrics.
   */
  protected void readMetrics(TimelineEntity entity, Result result,
      ColumnPrefix<?> columnPrefix) throws IOException {
    NavigableMap<String, NavigableMap<Long, Number>> metricsResult =
        ColumnRWHelper.readResultsWithTimestamps(
            result, columnPrefix, stringKeyConverter);
    for (Map.Entry<String, NavigableMap<Long, Number>> metricResult:
        metricsResult.entrySet()) {
      TimelineMetric metric = new TimelineMetric();
      metric.setId(metricResult.getKey());
      // Simply assume that if the value set contains more than 1 elements, the
      // metric is a TIME_SERIES metric, otherwise, it's a SINGLE_VALUE metric
      TimelineMetric.Type metricType = metricResult.getValue().size() > 1 ?
          TimelineMetric.Type.TIME_SERIES : TimelineMetric.Type.SINGLE_VALUE;
      metric.setType(metricType);
      metric.addValues(metricResult.getValue());
      entity.addMetric(metric);
    }
  }

  /**
   * Checks whether the reader has been created to fetch single entity or
   * multiple entities.
   *
   * @return true, if query is for single entity, false otherwise.
   */
  public boolean isSingleEntityRead() {
    return singleEntityRead;
  }

  protected void setTable(BaseTableRW<?> baseTable) {
    this.table = baseTable;
  }

  /**
   * Check if we have a certain field amongst fields to retrieve. This method
   * checks against {@link Field#ALL} as well because that would mean field
   * passed needs to be matched.
   *
   * @param fieldsToRetrieve fields to be retrieved.
   * @param requiredField fields to be checked in fieldsToRetrieve.
   * @return true if has the required field, false otherwise.
   */
  protected boolean hasField(EnumSet<Field> fieldsToRetrieve,
      Field requiredField) {
    return fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(requiredField);
  }

  /**
   * Create a filter list of qualifier filters based on passed set of columns.
   *
   * @param <T> Describes the type of column prefix.
   * @param colPrefix Column Prefix.
   * @param columns set of column qualifiers.
   * @return filter list.
   */
  protected <T extends BaseTable<T>> FilterList
      createFiltersFromColumnQualifiers(
          ColumnPrefix<T> colPrefix, Set<String> columns) {
    FilterList list = new FilterList(Operator.MUST_PASS_ONE);
    for (String column : columns) {
      // For columns which have compound column qualifiers (eg. events), we need
      // to include the required separator.
      byte[] compoundColQual = createColQualifierPrefix(colPrefix, column);
      list.addFilter(new QualifierFilter(CompareOp.EQUAL,
          new BinaryPrefixComparator(colPrefix
              .getColumnPrefixBytes(compoundColQual))));
    }
    return list;
  }

  protected <T extends BaseTable<T>> byte[] createColQualifierPrefix(
      ColumnPrefix<T> colPrefix, String column) {
    if (colPrefix == ApplicationColumnPrefix.EVENT
        || colPrefix == EntityColumnPrefix.EVENT) {
      return new EventColumnName(column, null, null).getColumnQualifier();
    } else {
      return stringKeyConverter.encode(column);
    }
  }

  /**
   * Helper method for reading relationship.
   *
   * @param <T> Describes the type of column prefix.
   * @param entity entity to fill.
   * @param result result from HBase.
   * @param prefix column prefix.
   * @param isRelatedTo if true, means relationship is to be added to
   *          isRelatedTo, otherwise its added to relatesTo.
   * @throws IOException if any problem is encountered while reading result.
   */
  protected <T extends BaseTable<T>> void readRelationship(
      TimelineEntity entity, Result result,
      ColumnPrefix<T> prefix, boolean isRelatedTo) throws IOException {
    // isRelatedTo and relatesTo are of type Map<String, Set<String>>
    Map<String, Object> columns = ColumnRWHelper.readResults(
        result, prefix, stringKeyConverter);
    for (Map.Entry<String, Object> column : columns.entrySet()) {
      for (String id : Separator.VALUES.splitEncoded(column.getValue()
          .toString())) {
        if (isRelatedTo) {
          entity.addIsRelatedToEntity(column.getKey(), id);
        } else {
          entity.addRelatesToEntity(column.getKey(), id);
        }
      }
    }
  }

  /**
   * Read events from the entity table or the application table. The column name
   * is of the form "eventId=timestamp=infoKey" where "infoKey" may be omitted
   * if there is no info associated with the event.
   *
   * @param <T> Describes the type of column prefix.
   * @param entity entity to fill.
   * @param result HBase Result.
   * @param prefix column prefix.
   * @throws IOException if any problem is encountered while reading result.
   */
  protected static <T extends BaseTable<T>> void readEvents(
      TimelineEntity entity, Result result,
      ColumnPrefix<T> prefix) throws IOException {
    Map<String, TimelineEvent> eventsMap = new HashMap<>();
    Map<EventColumnName, Object> eventsResult = ColumnRWHelper.readResults(
        result, prefix, new EventColumnNameConverter());
    for (Map.Entry<EventColumnName, Object>
             eventResult : eventsResult.entrySet()) {
      EventColumnName eventColumnName = eventResult.getKey();
      String key = eventColumnName.getId() +
          Long.toString(eventColumnName.getTimestamp());
      // Retrieve previously seen event to add to it
      TimelineEvent event = eventsMap.get(key);
      if (event == null) {
        // First time we're seeing this event, add it to the eventsMap
        event = new TimelineEvent();
        event.setId(eventColumnName.getId());
        event.setTimestamp(eventColumnName.getTimestamp());
        eventsMap.put(key, event);
      }
      if (eventColumnName.getInfoKey() != null) {
        event.addInfo(eventColumnName.getInfoKey(), eventResult.getValue());
      }
    }
    Set<TimelineEvent> eventsSet = new HashSet<>(eventsMap.values());
    entity.addEvents(eventsSet);
  }
}
