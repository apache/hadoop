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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationOperation;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * A bunch of utility functions used across TimelineReader and TimelineWriter.
 */
@Public
@Unstable
public final class TimelineStorageUtils {
  private TimelineStorageUtils() {
  }

  /** empty bytes. */
  public static final byte[] EMPTY_BYTES = new byte[0];

  /** indicator for no limits for splitting. */
  public static final int NO_LIMIT_SPLIT = -1;

  /** milliseconds in one day. */
  public static final long MILLIS_ONE_DAY = 86400000L;

  /**
   * Splits the source array into multiple array segments using the given
   * separator, up to a maximum of count items. This will naturally produce
   * copied byte arrays for each of the split segments. To identify the split
   * ranges without the array copies, see {@link #splitRanges(byte[], byte[])}.
   *
   * @param source Source array.
   * @param separator Separator represented as a byte array.
   * @return byte[][] after splitting the source
   */
  public static byte[][] split(byte[] source, byte[] separator) {
    return split(source, separator, NO_LIMIT_SPLIT);
  }

  /**
   * Splits the source array into multiple array segments using the given
   * separator, up to a maximum of count items. This will naturally produce
   * copied byte arrays for each of the split segments. To identify the split
   * ranges without the array copies, see {@link #splitRanges(byte[], byte[])}.
   *
   * @param source Source array.
   * @param separator Separator represented as a byte array.
   * @param limit a non-positive value indicates no limit on number of segments.
   * @return byte[][] after splitting the input source.
   */
  public static byte[][] split(byte[] source, byte[] separator, int limit) {
    List<Range> segments = splitRanges(source, separator, limit);

    byte[][] splits = new byte[segments.size()][];
    for (int i = 0; i < segments.size(); i++) {
      Range r = segments.get(i);
      byte[] tmp = new byte[r.length()];
      if (tmp.length > 0) {
        System.arraycopy(source, r.start(), tmp, 0, r.length());
      }
      splits[i] = tmp;
    }
    return splits;
  }

  /**
   * Returns a list of ranges identifying [start, end) -- closed, open --
   * positions within the source byte array that would be split using the
   * separator byte array.
   *
   * @param source Source array.
   * @param separator Separator represented as a byte array.
   * @return a list of ranges.
   */
  public static List<Range> splitRanges(byte[] source, byte[] separator) {
    return splitRanges(source, separator, NO_LIMIT_SPLIT);
  }

  /**
   * Returns a list of ranges identifying [start, end) -- closed, open --
   * positions within the source byte array that would be split using the
   * separator byte array.
   *
   * @param source the source data
   * @param separator the separator pattern to look for
   * @param limit the maximum number of splits to identify in the source
   * @return a list of ranges.
   */
  public static List<Range> splitRanges(byte[] source, byte[] separator,
      int limit) {
    List<Range> segments = new ArrayList<Range>();
    if ((source == null) || (separator == null)) {
      return segments;
    }
    int start = 0;
    itersource: for (int i = 0; i < source.length; i++) {
      for (int j = 0; j < separator.length; j++) {
        if (source[i + j] != separator[j]) {
          continue itersource;
        }
      }
      // all separator elements matched
      if (limit > 0 && segments.size() >= (limit - 1)) {
        // everything else goes in one final segment
        break;
      }
      segments.add(new Range(start, i));
      start = i + separator.length;
      // i will be incremented again in outer for loop
      i += separator.length - 1;
    }
    // add in remaining to a final range
    if (start <= source.length) {
      segments.add(new Range(start, source.length));
    }
    return segments;
  }

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
   * Converts/encodes a string app Id into a byte representation for (row) keys.
   * For conversion, we extract cluster timestamp and sequence id from the
   * string app id (calls {@link ConverterUtils#toApplicationId(String)} for
   * conversion) and then store it in a byte array of length 12 (8 bytes (long)
   * for cluster timestamp followed 4 bytes(int) for sequence id). Both cluster
   * timestamp and sequence id are inverted so that the most recent cluster
   * timestamp and highest sequence id appears first in the table (i.e.
   * application id appears in a descending order).
   *
   * @param appIdStr application id in string format i.e.
   * application_{cluster timestamp}_{sequence id with min 4 digits}
   *
   * @return encoded byte representation of app id.
   */
  public static byte[] encodeAppId(String appIdStr) {
    ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);
    byte[] appIdBytes = new byte[Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT];
    byte[] clusterTs = Bytes.toBytes(invertLong(appId.getClusterTimestamp()));
    System.arraycopy(clusterTs, 0, appIdBytes, 0, Bytes.SIZEOF_LONG);
    byte[] seqId = Bytes.toBytes(invertInt(appId.getId()));
    System.arraycopy(seqId, 0, appIdBytes, Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT);
    return appIdBytes;
  }

  /**
   * Converts/decodes a 12 byte representation of app id for (row) keys to an
   * app id in string format which can be returned back to client.
   * For decoding, 12 bytes are interpreted as 8 bytes of inverted cluster
   * timestamp(long) followed by 4 bytes of inverted sequence id(int). Calls
   * {@link ApplicationId#toString} to generate string representation of app id.
   *
   * @param appIdBytes application id in byte representation.
   *
   * @return decoded app id in string format.
   */
  public static String decodeAppId(byte[] appIdBytes) {
    if (appIdBytes.length != (Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT)) {
      throw new IllegalArgumentException("Invalid app id in byte format");
    }
    long clusterTs = invertLong(Bytes.toLong(appIdBytes, 0, Bytes.SIZEOF_LONG));
    int seqId =
        invertInt(Bytes.toInt(appIdBytes, Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT));
    return ApplicationId.newInstance(clusterTs, seqId).toString();
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
   *
   * @param entityRelations the relations of an entity
   * @param relationFilters the relations for filtering
   * @return a boolean flag to indicate if both match
   */
  public static boolean matchRelations(
      Map<String, Set<String>> entityRelations,
      Map<String, Set<String>> relationFilters) {
    for (Map.Entry<String, Set<String>> relation : relationFilters.entrySet()) {
      Set<String> ids = entityRelations.get(relation.getKey());
      if (ids == null) {
        return false;
      }
      for (String id : relation.getValue()) {
        if (!ids.contains(id)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   *
   * @param map the map of key/value pairs in an entity
   * @param filters the map of key/value pairs for filtering
   * @return a boolean flag to indicate if both match
   */
  public static boolean matchFilters(Map<String, ? extends Object> map,
      Map<String, ? extends Object> filters) {
    for (Map.Entry<String, ? extends Object> filter : filters.entrySet()) {
      Object value = map.get(filter.getKey());
      if (value == null) {
        return false;
      }
      if (!value.equals(filter.getValue())) {
        return false;
      }
    }
    return true;
  }

  /**
   *
   * @param entityEvents the set of event objects in an entity
   * @param eventFilters the set of event Ids for filtering
   * @return a boolean flag to indicate if both match
   */
  public static boolean matchEventFilters(Set<TimelineEvent> entityEvents,
      Set<String> eventFilters) {
    Set<String> eventIds = new HashSet<String>();
    for (TimelineEvent event : entityEvents) {
      eventIds.add(event.getId());
    }
    for (String eventFilter : eventFilters) {
      if (!eventIds.contains(eventFilter)) {
        return false;
      }
    }
    return true;
  }

  /**
   *
   * @param metrics the set of metric objects in an entity
   * @param metricFilters the set of metric Ids for filtering
   * @return a boolean flag to indicate if both match
   */
  public static boolean matchMetricFilters(Set<TimelineMetric> metrics,
      Set<String> metricFilters) {
    Set<String> metricIds = new HashSet<String>();
    for (TimelineMetric metric : metrics) {
      metricIds.add(metric.getId());
    }

    for (String metricFilter : metricFilters) {
      if (!metricIds.contains(metricFilter)) {
        return false;
      }
    }
    return true;
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
}
