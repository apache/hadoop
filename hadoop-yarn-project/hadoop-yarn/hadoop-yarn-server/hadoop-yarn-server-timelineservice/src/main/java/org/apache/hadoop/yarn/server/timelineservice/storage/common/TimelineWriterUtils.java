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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationOperation;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * bunch of utility functions used across TimelineWriter classes
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TimelineWriterUtils {

  /** empty bytes */
  public static final byte[] EMPTY_BYTES = new byte[0];

  /** indicator for no limits for splitting */
  public static final int NO_LIMIT_SPLIT = -1;

  /** milliseconds in one day */
  public static final long MILLIS_ONE_DAY = 86400000L;

  /**
   * Splits the source array into multiple array segments using the given
   * separator, up to a maximum of count items. This will naturally produce
   * copied byte arrays for each of the split segments. To identify the split
   * ranges without the array copies, see
   * {@link TimelineWriterUtils#splitRanges(byte[], byte[])}.
   *
   * @param source
   * @param separator
   * @return byte[] array after splitting the source
   */
  public static byte[][] split(byte[] source, byte[] separator) {
    return split(source, separator, NO_LIMIT_SPLIT);
  }

  /**
   * Splits the source array into multiple array segments using the given
   * separator, up to a maximum of count items. This will naturally produce
   * copied byte arrays for each of the split segments. To identify the split
   * ranges without the array copies, see
   * {@link TimelineWriterUtils#splitRanges(byte[], byte[])}.
   *
   * @param source
   * @param separator
   * @param limit a non-positive value indicates no limit on number of segments.
   * @return byte[][] after splitting the input source
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
  public static long invert(Long key) {
    return Long.MAX_VALUE - key;
  }

  /**
   * returns the timestamp of that day's start (which is midnight 00:00:00 AM)
   * for a given input timestamp
   *
   * @param ts
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
   * @param attributes
   * @param aggOp
   * @return array of combined attributes
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
   * @param attributes
   * @param aggOp
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
   * checks if an application has finished
   *
   * @param te
   * @return true if application has finished else false
   */
  public static boolean isApplicationFinished(TimelineEntity te) {
    SortedSet<TimelineEvent> allEvents = te.getEvents();
    if ((allEvents != null) && (allEvents.size() > 0)) {
      TimelineEvent event = allEvents.last();
      if (event.getId().equals(ApplicationMetricsConstants.FINISHED_EVENT_TYPE)) {
        return true;
      }
    }
    return false;
  }

  /**
   * get the time at which an app finished
   *
   * @param te
   * @return true if application has finished else false
   */
  public static long getApplicationFinishedTime(TimelineEntity te) {
    SortedSet<TimelineEvent> allEvents = te.getEvents();
    if ((allEvents != null) && (allEvents.size() > 0)) {
      TimelineEvent event = allEvents.last();
      if (event.getId().equals(ApplicationMetricsConstants.FINISHED_EVENT_TYPE)) {
        return event.getTimestamp();
      }
    }
    return 0l;
  }

  /**
   * Checks if the input TimelineEntity object is an ApplicationEntity.
   *
   * @param te
   * @return true if input is an ApplicationEntity, false otherwise
   */
  public static boolean isApplicationEntity(TimelineEntity te) {
    return te.getType().equals(TimelineEntityType.YARN_APPLICATION.toString());
  }

  /**
   * Checks for the APPLICATION_CREATED event.
   *
   * @param te
   * @return true is application event exists, false otherwise
   */
  public static boolean isApplicationCreated(TimelineEntity te) {
    if (isApplicationEntity(te)) {
      for (TimelineEvent event : te.getEvents()) {
        if (event.getId()
            .equals(ApplicationMetricsConstants.CREATED_EVENT_TYPE)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns the first seen aggregation operation as seen in the list of input
   * tags or null otherwise
   *
   * @param tags
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
   * @param attribute
   * @return Tag
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

    AggregationCompactionDimension aggCompactDim = AggregationCompactionDimension
        .getAggregationCompactionDimension(attribute.getKey());
    if (aggCompactDim != null) {
      Tag t = new Tag(aggCompactDim.getTagType(), attribute.getValue());
      return t;
    }
    return null;
  }

}