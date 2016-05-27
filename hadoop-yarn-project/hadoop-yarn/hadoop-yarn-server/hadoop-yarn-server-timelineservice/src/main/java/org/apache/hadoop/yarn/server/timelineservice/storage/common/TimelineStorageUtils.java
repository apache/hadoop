/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineExistsFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter.TimelineFilterType;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValueFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValuesFilter;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationOperation;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTable;

/**
 * A bunch of utility functions used across TimelineReader and TimelineWriter.
 */
@Public
@Unstable
public final class TimelineStorageUtils {
  private TimelineStorageUtils() {
  }

  private static final Log LOG = LogFactory.getLog(TimelineStorageUtils.class);

  /** milliseconds in one day. */
  public static final long MILLIS_ONE_DAY = 86400000L;

  /**
   * Converts a timestamp into it's inverse timestamp to be used in (row) keys
   * where we want to have the most recent timestamp in the top of the table
   * (scans start at the most recent timestamp first).
   *
   * @param key value to be inverted so that the latest version will be first in
   *          a scan.
   * @return inverted long
   */
  public static long invertLong(long key) {
    return Long.MAX_VALUE - key;
  }

  /**
   * Converts an int into it's inverse int to be used in (row) keys
   * where we want to have the largest int value in the top of the table
   * (scans start at the largest int first).
   *
   * @param key value to be inverted so that the latest version will be first in
   *          a scan.
   * @return inverted int
   */
  public static int invertInt(int key) {
    return Integer.MAX_VALUE - key;
  }

  /**
   * returns the timestamp of that day's start (which is midnight 00:00:00 AM)
   * for a given input timestamp.
   *
   * @param ts Timestamp.
   * @return timestamp of that day's beginning (midnight)
   */
  public static long getTopOfTheDayTimestamp(long ts) {
    long dayTimestamp = ts - (ts % MILLIS_ONE_DAY);
    return dayTimestamp;
  }

  /**
   * Combines the input array of attributes and the input aggregation operation
   * into a new array of attributes.
   *
   * @param attributes Attributes to be combined.
   * @param aggOp Aggregation operation.
   * @return array of combined attributes.
   */
  public static Attribute[] combineAttributes(Attribute[] attributes,
      AggregationOperation aggOp) {
    int newLength = getNewLengthCombinedAttributes(attributes, aggOp);
    Attribute[] combinedAttributes = new Attribute[newLength];

    if (attributes != null) {
      System.arraycopy(attributes, 0, combinedAttributes, 0, attributes.length);
    }

    if (aggOp != null) {
      Attribute a2 = aggOp.getAttribute();
      combinedAttributes[newLength - 1] = a2;
    }
    return combinedAttributes;
  }

  /**
   * Returns a number for the new array size. The new array is the combination
   * of input array of attributes and the input aggregation operation.
   *
   * @param attributes Attributes.
   * @param aggOp Aggregation operation.
   * @return the size for the new array
   */
  private static int getNewLengthCombinedAttributes(Attribute[] attributes,
      AggregationOperation aggOp) {
    int oldLength = getAttributesLength(attributes);
    int aggLength = getAppOpLength(aggOp);
    return oldLength + aggLength;
  }

  private static int getAppOpLength(AggregationOperation aggOp) {
    if (aggOp != null) {
      return 1;
    }
    return 0;
  }

  private static int getAttributesLength(Attribute[] attributes) {
    if (attributes != null) {
      return attributes.length;
    }
    return 0;
  }

  /**
   * checks if an application has finished.
   *
   * @param te TimlineEntity object.
   * @return true if application has finished else false
   */
  public static boolean isApplicationFinished(TimelineEntity te) {
    SortedSet<TimelineEvent> allEvents = te.getEvents();
    if ((allEvents != null) && (allEvents.size() > 0)) {
      TimelineEvent event = allEvents.last();
      if (event.getId().equals(
          ApplicationMetricsConstants.FINISHED_EVENT_TYPE)) {
        return true;
      }
    }
    return false;
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
  public static boolean hasField(EnumSet<Field> fieldsToRetrieve,
      Field requiredField) {
    return fieldsToRetrieve.contains(Field.ALL) ||
        fieldsToRetrieve.contains(requiredField);
  }

  /**
   * Checks if the input TimelineEntity object is an ApplicationEntity.
   *
   * @param te TimelineEntity object.
   * @return true if input is an ApplicationEntity, false otherwise
   */
  public static boolean isApplicationEntity(TimelineEntity te) {
    return te.getType().equals(TimelineEntityType.YARN_APPLICATION.toString());
  }

  /**
   * @param te TimelineEntity object.
   * @param eventId event with this id needs to be fetched
   * @return TimelineEvent if TimelineEntity contains the desired event.
   */
  public static TimelineEvent getApplicationEvent(TimelineEntity te,
      String eventId) {
    if (isApplicationEntity(te)) {
      for (TimelineEvent event : te.getEvents()) {
        if (event.getId().equals(eventId)) {
          return event;
        }
      }
    }
    return null;
  }

  /**
   * Returns the first seen aggregation operation as seen in the list of input
   * tags or null otherwise.
   *
   * @param tags list of HBase tags.
   * @return AggregationOperation
   */
  public static AggregationOperation getAggregationOperationFromTagsList(
      List<Tag> tags) {
    for (AggregationOperation aggOp : AggregationOperation.values()) {
      for (Tag tag : tags) {
        if (tag.getType() == aggOp.getTagType()) {
          return aggOp;
        }
      }
    }
    return null;
  }

  /**
   * Creates a {@link Tag} from the input attribute.
   *
   * @param attribute Attribute from which tag has to be fetched.
   * @return a HBase Tag.
   */
  public static Tag getTagFromAttribute(Entry<String, byte[]> attribute) {
    // attribute could be either an Aggregation Operation or
    // an Aggregation Dimension
    // Get the Tag type from either
    AggregationOperation aggOp = AggregationOperation
        .getAggregationOperation(attribute.getKey());
    if (aggOp != null) {
      Tag t = new Tag(aggOp.getTagType(), attribute.getValue());
      return t;
    }

    AggregationCompactionDimension aggCompactDim =
        AggregationCompactionDimension.getAggregationCompactionDimension(
            attribute.getKey());
    if (aggCompactDim != null) {
      Tag t = new Tag(aggCompactDim.getTagType(), attribute.getValue());
      return t;
    }
    return null;
  }

  /**
   * Matches key-values filter. Used for relatesTo/isRelatedTo filters.
   *
   * @param entity entity which holds relatesTo/isRelatedTo relations which we
   *     will match against.
   * @param keyValuesFilter key-values filter.
   * @param entityFiltersType type of filters we are trying to match.
   * @return true, if filter matches, false otherwise.
   */
  private static boolean matchKeyValuesFilter(TimelineEntity entity,
      TimelineKeyValuesFilter keyValuesFilter,
      TimelineEntityFiltersType entityFiltersType) {
    Map<String, Set<String>> relations = null;
    if (entityFiltersType == TimelineEntityFiltersType.IS_RELATED_TO) {
      relations = entity.getIsRelatedToEntities();
    } else if (entityFiltersType == TimelineEntityFiltersType.RELATES_TO) {
      relations = entity.getRelatesToEntities();
    }
    if (relations == null) {
      return false;
    }
    Set<String> ids = relations.get(keyValuesFilter.getKey());
    if (ids == null) {
      return false;
    }
    boolean matched = false;
    for (Object id : keyValuesFilter.getValues()) {
      // Matches if id is found amongst the relationships for an entity and
      // filter's compare op is EQUAL.
      // If compare op is NOT_EQUAL, for a match to occur, id should not be
      // found amongst relationships for an entity.
      matched = !(ids.contains(id) ^
          keyValuesFilter.getCompareOp() == TimelineCompareOp.EQUAL);
      if (!matched) {
        return false;
      }
    }
    return true;
  }

  /**
   * Matches relatesto.
   *
   * @param entity entity which holds relatesto relations.
   * @param relatesTo the relations for filtering.
   * @return true, if filter matches, false otherwise.
   * @throws IOException if an unsupported filter for matching relations is
   *     being matched.
   */
  public static boolean matchRelatesTo(TimelineEntity entity,
      TimelineFilterList relatesTo) throws IOException {
    return matchFilters(
        entity, relatesTo, TimelineEntityFiltersType.RELATES_TO);
  }

  /**
   * Matches isrelatedto.
   *
   * @param entity entity which holds isRelatedTo relations.
   * @param isRelatedTo the relations for filtering.
   * @return true, if filter matches, false otherwise.
   * @throws IOException if an unsupported filter for matching relations is
   *     being matched.
   */
  public static boolean matchIsRelatedTo(TimelineEntity entity,
      TimelineFilterList isRelatedTo) throws IOException {
    return matchFilters(
        entity, isRelatedTo, TimelineEntityFiltersType.IS_RELATED_TO);
  }

  /**
   * Matches key-value filter. Used for config and info filters.
   *
   * @param entity entity which holds the config/info which we will match
   *     against.
   * @param kvFilter a key-value filter.
   * @param entityFiltersType type of filters we are trying to match.
   * @return true, if filter matches, false otherwise.
   */
  private static boolean matchKeyValueFilter(TimelineEntity entity,
      TimelineKeyValueFilter kvFilter,
      TimelineEntityFiltersType entityFiltersType) {
    Map<String, ? extends Object> map = null;
    // Supported only for config and info filters.
    if (entityFiltersType == TimelineEntityFiltersType.CONFIG) {
      map = entity.getConfigs();
    } else if (entityFiltersType == TimelineEntityFiltersType.INFO) {
      map = entity.getInfo();
    }
    if (map == null) {
      return false;
    }
    Object value = map.get(kvFilter.getKey());
    if (value == null) {
      return false;
    }
    // Matches if filter's value is equal to the value of the key and filter's
    // compare op is EQUAL.
    // If compare op is NOT_EQUAL, for a match to occur, value should not be
    // equal to the value of the key.
    return !(value.equals(kvFilter.getValue()) ^
        kvFilter.getCompareOp() == TimelineCompareOp.EQUAL);
  }

  /**
   * Matches config filters.
   *
   * @param entity entity which holds a map of config key-value pairs.
   * @param configFilters list of info filters.
   * @return a boolean flag to indicate if both match.
   * @throws IOException if an unsupported filter for matching config filters is
   *     being matched.
   */
  public static boolean matchConfigFilters(TimelineEntity entity,
      TimelineFilterList configFilters) throws IOException {
    return
        matchFilters(entity, configFilters, TimelineEntityFiltersType.CONFIG);
  }

  /**
   * Matches info filters.
   *
   * @param entity entity which holds a map of info key-value pairs.
   * @param infoFilters list of info filters.
   * @return a boolean flag to indicate if both match.
   * @throws IOException if an unsupported filter for matching info filters is
   *     being matched.
   */
  public static boolean matchInfoFilters(TimelineEntity entity,
      TimelineFilterList infoFilters) throws IOException {
    return matchFilters(entity, infoFilters, TimelineEntityFiltersType.INFO);
  }

  /**
   * Matches exists filter. Used for event filters.
   *
   * @param entity entity which holds the events which we will match against.
   * @param existsFilter exists filter.
   * @param entityFiltersType type of filters we are trying to match.
   * @return true, if filter matches, false otherwise.
   */
  private static boolean matchExistsFilter(TimelineEntity entity,
      TimelineExistsFilter existsFilter,
      TimelineEntityFiltersType entityFiltersType) {
    // Currently exists filter is only supported for event filters.
    if (entityFiltersType != TimelineEntityFiltersType.EVENT) {
      return false;
    }
    Set<String> eventIds = new HashSet<String>();
    for (TimelineEvent event : entity.getEvents()) {
      eventIds.add(event.getId());
    }
    // Matches if filter's value is contained in the list of events filter's
    // compare op is EQUAL.
    // If compare op is NOT_EQUAL, for a match to occur, value should not be
    // contained in the list of events.
    return !(eventIds.contains(existsFilter.getValue()) ^
        existsFilter.getCompareOp() == TimelineCompareOp.EQUAL);
  }

  /**
   * Matches event filters.
   *
   * @param entity entity which holds a set of event objects.
   * @param eventFilters the set of event Ids for filtering.
   * @return a boolean flag to indicate if both match.
   * @throws IOException if an unsupported filter for matching event filters is
   *     being matched.
   */
  public static boolean matchEventFilters(TimelineEntity entity,
      TimelineFilterList eventFilters) throws IOException {
    return matchFilters(entity, eventFilters, TimelineEntityFiltersType.EVENT);
  }

  /**
   * Compare two values based on comparison operator.
   *
   * @param compareOp comparison operator.
   * @param val1 value 1.
   * @param val2 value 2.
   * @return true, if relation matches, false otherwise
   */
  private static boolean compareValues(TimelineCompareOp compareOp,
      long val1, long val2) {
    switch (compareOp) {
    case LESS_THAN:
      return val1 < val2;
    case LESS_OR_EQUAL:
      return val1 <= val2;
    case EQUAL:
      return val1 == val2;
    case NOT_EQUAL:
      return val1 != val2;
    case GREATER_OR_EQUAL:
      return val1 >= val2;
    case GREATER_THAN:
      return val1 > val2;
    default:
      throw new RuntimeException("Unknown TimelineCompareOp " +
          compareOp.name());
    }
  }

  /**
   * Matches compare filter. Used for metric filters.
   *
   * @param entity entity which holds the metrics which we will match against.
   * @param compareFilter compare filter.
   * @param entityFiltersType type of filters we are trying to match.
   * @return true, if filter matches, false otherwise.
   * @throws IOException if metric filters holds non integral values.
   */
  private static boolean matchCompareFilter(TimelineEntity entity,
      TimelineCompareFilter compareFilter,
      TimelineEntityFiltersType entityFiltersType) throws IOException {
    // Currently exists filter is only supported for metric filters.
    if (entityFiltersType != TimelineEntityFiltersType.METRIC) {
      return false;
    }
    // We expect only integral values(short/int/long) for metric filters.
    if (!isIntegralValue(compareFilter.getValue())) {
      throw new IOException("Metric filters has non integral values");
    }
    Map<String, TimelineMetric> metricMap =
        new HashMap<String, TimelineMetric>();
    for (TimelineMetric metric : entity.getMetrics()) {
      metricMap.put(metric.getId(), metric);
    }
    TimelineMetric metric = metricMap.get(compareFilter.getKey());
    if (metric == null) {
      return false;
    }
    // We will be using the latest value of metric to compare.
    return compareValues(compareFilter.getCompareOp(),
        metric.getValuesJAXB().firstEntry().getValue().longValue(),
        ((Number)compareFilter.getValue()).longValue());
  }

  /**
   * Matches metric filters.
   *
   * @param entity entity which holds a set of metric objects.
   * @param metricFilters list of metric filters.
   * @return a boolean flag to indicate if both match.
   * @throws IOException if an unsupported filter for matching metric filters is
   *     being matched.
   */
  public static boolean matchMetricFilters(TimelineEntity entity,
      TimelineFilterList metricFilters) throws IOException {
    return matchFilters(
        entity, metricFilters, TimelineEntityFiltersType.METRIC);
  }

  /**
   * Common routine to match different filters. Iterates over a filter list and
   * calls routines based on filter type.
   *
   * @param entity Timeline entity.
   * @param filters filter list.
   * @param entityFiltersType type of filters which are being matched.
   * @return a boolean flag to indicate if filter matches.
   * @throws IOException if an unsupported filter for matching this specific
   *     filter is being matched.
   */
  private static boolean matchFilters(TimelineEntity entity,
      TimelineFilterList filters, TimelineEntityFiltersType entityFiltersType)
      throws IOException {
    if (filters == null || filters.getFilterList().isEmpty()) {
      return false;
    }
    TimelineFilterList.Operator operator = filters.getOperator();
    for (TimelineFilter filter : filters.getFilterList()) {
      TimelineFilterType filterType = filter.getFilterType();
      if (!entityFiltersType.isValidFilter(filterType)) {
        throw new IOException("Unsupported filter " + filterType);
      }
      boolean matched = false;
      switch (filterType) {
      case LIST:
        matched = matchFilters(entity, (TimelineFilterList)filter,
            entityFiltersType);
        break;
      case COMPARE:
        matched = matchCompareFilter(entity, (TimelineCompareFilter)filter,
            entityFiltersType);
        break;
      case EXISTS:
        matched = matchExistsFilter(entity, (TimelineExistsFilter)filter,
            entityFiltersType);
        break;
      case KEY_VALUE:
        matched = matchKeyValueFilter(entity, (TimelineKeyValueFilter)filter,
            entityFiltersType);
        break;
      case KEY_VALUES:
        matched = matchKeyValuesFilter(entity, (TimelineKeyValuesFilter)filter,
            entityFiltersType);
        break;
      default:
        throw new IOException("Unsupported filter " + filterType);
      }
      if (!matched) {
        if(operator == TimelineFilterList.Operator.AND) {
          return false;
        }
      } else {
        if(operator == TimelineFilterList.Operator.OR) {
          return true;
        }
      }
    }
    return operator == TimelineFilterList.Operator.AND;
  }

  /**
   * Checks if passed object is of integral type(Short/Integer/Long).
   *
   * @param obj Object to be checked.
   * @return true if object passed is of type Short or Integer or Long, false
   * otherwise.
   */
  public static boolean isIntegralValue(Object obj) {
    return (obj instanceof Short) || (obj instanceof Integer) ||
        (obj instanceof Long);
  }

  /**
   * creates a new cell based on the input cell but with the new value.
   *
   * @param origCell Original cell
   * @param newValue new cell value
   * @return cell
   * @throws IOException while creating new cell.
   */
  public static Cell createNewCell(Cell origCell, byte[] newValue)
      throws IOException {
    return CellUtil.createCell(CellUtil.cloneRow(origCell),
        CellUtil.cloneFamily(origCell), CellUtil.cloneQualifier(origCell),
        origCell.getTimestamp(), KeyValue.Type.Put.getCode(), newValue);
  }

  /**
   * creates a cell with the given inputs.
   *
   * @param row row of the cell to be created
   * @param family column family name of the new cell
   * @param qualifier qualifier for the new cell
   * @param ts timestamp of the new cell
   * @param newValue value of the new cell
   * @param tags tags in the new cell
   * @return cell
   * @throws IOException while creating the cell.
   */
  public static Cell createNewCell(byte[] row, byte[] family, byte[] qualifier,
      long ts, byte[] newValue, byte[] tags) throws IOException {
    return CellUtil.createCell(row, family, qualifier, ts, KeyValue.Type.Put,
        newValue, tags);
  }

  /**
   * returns app id from the list of tags.
   *
   * @param tags cell tags to be looked into
   * @return App Id as the AggregationCompactionDimension
   */
  public static String getAggregationCompactionDimension(List<Tag> tags) {
    String appId = null;
    for (Tag t : tags) {
      if (AggregationCompactionDimension.APPLICATION_ID.getTagType() == t
          .getType()) {
        appId = Bytes.toString(t.getValue());
        return appId;
      }
    }
    return appId;
  }

  /**
   * Helper method for reading relationship.
   *
   * @param <T> Describes the type of column prefix.
   * @param entity entity to fill.
   * @param result result from HBase.
   * @param prefix column prefix.
   * @param isRelatedTo if true, means relationship is to be added to
   *     isRelatedTo, otherwise its added to relatesTo.
   * @throws IOException if any problem is encountered while reading result.
   */
  public static <T> void readRelationship(
      TimelineEntity entity, Result result, ColumnPrefix<T> prefix,
      boolean isRelatedTo) throws IOException {
    // isRelatedTo and relatesTo are of type Map<String, Set<String>>
    Map<String, Object> columns =
        prefix.readResults(result, StringKeyConverter.getInstance());
    for (Map.Entry<String, Object> column : columns.entrySet()) {
      for (String id : Separator.VALUES.splitEncoded(
          column.getValue().toString())) {
        if (isRelatedTo) {
          entity.addIsRelatedToEntity(column.getKey(), id);
        } else {
          entity.addRelatesToEntity(column.getKey(), id);
        }
      }
    }
  }

  /**
   * Helper method for reading key-value pairs for either info or config.
   *
   * @param <T> Describes the type of column prefix.
   * @param entity entity to fill.
   * @param result result from HBase.
   * @param prefix column prefix.
   * @param isConfig if true, means we are reading configs, otherwise info.
   * @throws IOException if any problem is encountered while reading result.
   */
  public static <T> void readKeyValuePairs(
      TimelineEntity entity, Result result, ColumnPrefix<T> prefix,
      boolean isConfig) throws IOException {
    // info and configuration are of type Map<String, Object or String>
    Map<String, Object> columns =
        prefix.readResults(result, StringKeyConverter.getInstance());
    if (isConfig) {
      for (Map.Entry<String, Object> column : columns.entrySet()) {
        entity.addConfig(column.getKey(), column.getValue().toString());
      }
    } else {
      entity.addInfo(columns);
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
  public static <T> void readEvents(TimelineEntity entity, Result result,
      ColumnPrefix<T> prefix) throws IOException {
    Map<String, TimelineEvent> eventsMap = new HashMap<>();
    Map<EventColumnName, Object> eventsResult =
        prefix.readResults(result, EventColumnNameConverter.getInstance());
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

  public static boolean isFlowRunTable(HRegionInfo hRegionInfo,
      Configuration conf) {
    String regionTableName = hRegionInfo.getTable().getNameAsString();
    String flowRunTableName = conf.get(FlowRunTable.TABLE_NAME_CONF_NAME,
        FlowRunTable.DEFAULT_TABLE_NAME);
    if (LOG.isDebugEnabled()) {
      LOG.debug("regionTableName=" + regionTableName);
    }
    if (flowRunTableName.equalsIgnoreCase(regionTableName)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(" table is the flow run table!! " + flowRunTableName);
      }
      return true;
    }
    return false;
  }
}
