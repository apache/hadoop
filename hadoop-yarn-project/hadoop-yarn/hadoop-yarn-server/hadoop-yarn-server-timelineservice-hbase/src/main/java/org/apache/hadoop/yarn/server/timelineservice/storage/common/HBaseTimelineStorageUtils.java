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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationOperation;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;

/**
 * A bunch of utility functions used in HBase TimelineService backend.
 */
public final class HBaseTimelineStorageUtils {
  /** milliseconds in one day. */
  public static final long MILLIS_ONE_DAY = 86400000L;
  private static final Logger LOG =
      LoggerFactory.getLogger(HBaseTimelineStorageUtils.class);

  private HBaseTimelineStorageUtils() {
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
  public static Tag getTagFromAttribute(Map.Entry<String, byte[]> attribute) {
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

  private static final ThreadLocal<NumberFormat> APP_ID_FORMAT =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(4);
          return fmt;
        }
      };

  /**
   * A utility method that converts ApplicationId to string without using
   * FastNumberFormat in order to avoid the incompatibility issue caused
   * by mixing hadoop-common 2.5.1 and hadoop-yarn-api 3.0 in this module.
   * This is a work-around implementation as discussed in YARN-6905.
   *
   * @param appId application id
   * @return the string representation of the given application id
   *
   */
  public static String convertApplicationIdToString(ApplicationId appId) {
    StringBuilder sb = new StringBuilder(64);
    sb.append(ApplicationId.appIdStrPrefix);
    sb.append("_");
    sb.append(appId.getClusterTimestamp());
    sb.append('_');
    sb.append(APP_ID_FORMAT.get().format(appId.getId()));
    return sb.toString();
  }

  /**
   * @param conf Yarn configuration. Used to see if there is an explicit config
   *          pointing to the HBase config file to read. It should not be null
   *          or a NullPointerException will be thrown.
   * @return a configuration with the HBase configuration from the classpath,
   *         optionally overwritten by the timeline service configuration URL if
   *         specified.
   * @throws MalformedURLException if a timeline service HBase configuration URL
   *           is specified but is a malformed URL.
   */
  public static Configuration getTimelineServiceHBaseConf(Configuration conf)
      throws MalformedURLException {
    if (conf == null) {
      throw new NullPointerException();
    }

    Configuration hbaseConf;
    String timelineServiceHBaseConfFileURL =
        conf.get(YarnConfiguration.TIMELINE_SERVICE_HBASE_CONFIGURATION_FILE);
    if (timelineServiceHBaseConfFileURL != null
        && timelineServiceHBaseConfFileURL.length() > 0) {
      LOG.info("Using hbase configuration at " +
          timelineServiceHBaseConfFileURL);
      // create a clone so that we don't mess with out input one
      hbaseConf = new Configuration(conf);
      Configuration plainHBaseConf = new Configuration(false);
      URL hbaseSiteXML = new URL(timelineServiceHBaseConfFileURL);
      plainHBaseConf.addResource(hbaseSiteXML);
      HBaseConfiguration.merge(hbaseConf, plainHBaseConf);
    } else {
      // default to what is on the classpath
      hbaseConf = HBaseConfiguration.create(conf);
    }
    return hbaseConf;
  }

  /**
   * Given a row key prefix stored in a byte array, return a byte array for its
   * immediate next row key.
   *
   * @param rowKeyPrefix The provided row key prefix, represented in an array.
   * @return the closest next row key of the provided row key.
   */
  public static byte[] calculateTheClosestNextRowKeyForPrefix(
      byte[] rowKeyPrefix) {
    // Essentially we are treating it like an 'unsigned very very long' and
    // doing +1 manually.
    // Search for the place where the trailing 0xFFs start
    int offset = rowKeyPrefix.length;
    while (offset > 0) {
      if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
        break;
      }
      offset--;
    }

    if (offset == 0) {
      // We got an 0xFFFF... (only FFs) stopRow value which is
      // the last possible prefix before the end of the table.
      // So set it to stop at the 'end of the table'
      return HConstants.EMPTY_END_ROW;
    }

    // Copy the right length of the original
    byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
    // And increment the last one
    newStopRow[newStopRow.length - 1]++;
    return newStopRow;
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

  public static void setMetricsTimeRange(Query query, byte[] metricsCf,
      long tsBegin, long tsEnd) {
    if (tsBegin != 0 || tsEnd != Long.MAX_VALUE) {
      query.setColumnFamilyTimeRange(metricsCf,
          tsBegin, ((tsEnd == Long.MAX_VALUE) ? Long.MAX_VALUE : (tsEnd + 1)));
    }
  }
}
