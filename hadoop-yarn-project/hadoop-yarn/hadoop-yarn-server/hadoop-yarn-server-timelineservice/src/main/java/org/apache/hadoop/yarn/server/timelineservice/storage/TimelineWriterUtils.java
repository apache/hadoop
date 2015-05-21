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
package org.apache.hadoop.yarn.server.timelineservice.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timelineservice.storage.Range;

/**
 * bunch of utility functions used across TimelineWriter classes
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TimelineWriterUtils {

  /** empty bytes */
  public static final byte[] EMPTY_BYTES = new byte[0];
  private static final String SPACE = " ";
  private static final String UNDERSCORE = "_";
  private static final String EMPTY_STRING = "";

  /**
   * Returns a single byte array containing all of the individual component
   * arrays separated by the separator array.
   *
   * @param separator
   * @param components
   * @return byte array after joining the components
   */
  public static byte[] join(byte[] separator, byte[]... components) {
    if (components == null || components.length == 0) {
      return EMPTY_BYTES;
    }

    int finalSize = 0;
    if (separator != null) {
      finalSize = separator.length * (components.length - 1);
    }
    for (byte[] comp : components) {
      if (comp != null) {
        finalSize += comp.length;
      }
    }

    byte[] buf = new byte[finalSize];
    int offset = 0;
    for (int i = 0; i < components.length; i++) {
      if (components[i] != null) {
        System.arraycopy(components[i], 0, buf, offset, components[i].length);
        offset += components[i].length;
        if (i < (components.length - 1) && separator != null
            && separator.length > 0) {
          System.arraycopy(separator, 0, buf, offset, separator.length);
          offset += separator.length;
        }
      }
    }
    return buf;
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
   * @return byte[] array after splitting the source
   */
  public static byte[][] split(byte[] source, byte[] separator) {
    return split(source, separator, -1);
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
   * @param limit
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
    return splitRanges(source, separator, -1);
  }

  /**
   * Returns a list of ranges identifying [start, end) -- closed, open --
   * positions within the source byte array that would be split using the
   * separator byte array.
   * @param source the source data
   * @param separator the separator pattern to look for
   * @param limit the maximum number of splits to identify in the source
   */
  public static List<Range> splitRanges(byte[] source, byte[] separator, int limit) {
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
      if (limit > 0 && segments.size() >= (limit-1)) {
        // everything else goes in one final segment
        break;
      }

      segments.add(new Range(start, i));
      start = i + separator.length;
      // i will be incremented again in outer for loop
      i += separator.length-1;
    }
    // add in remaining to a final range
    if (start <= source.length) {
      segments.add(new Range(start, source.length));
    }
    return segments;
  }

  /**
   * converts run id into it's inverse timestamp
   * @param flowRunId
   * @return inverted long
   */
  public static long encodeRunId(Long flowRunId) {
    return Long.MAX_VALUE - flowRunId;
  }

  /**
   * return a value from the Map as a String
   * @param key
   * @param values
   * @return value as a String or ""
   * @throws IOException 
   */
  public static String getValueAsString(final byte[] key,
      final Map<byte[], byte[]> values) throws IOException {
    if( values == null ) {
      return EMPTY_STRING;
    }
    byte[] value = values.get(key);
    if (value != null) {
      return GenericObjectMapper.read(value).toString();
    } else {
      return EMPTY_STRING;
    }
  }

  /**
   * return a value from the Map as a long
   * @param key
   * @param values
   * @return value as Long or 0L
   * @throws IOException 
   */
  public static long getValueAsLong(final byte[] key,
      final Map<byte[], byte[]> values) throws IOException {
    if (values == null) {
      return 0;
    }
    byte[] value = values.get(key);
    if (value != null) {
      Number val = (Number) GenericObjectMapper.read(value);
      return val.longValue();
    } else {
      return 0L;
    }
  }

  /**
   * concates the values from a Set<Strings> to return a single delimited string value
   * @param rowKeySeparator
   * @param values
   * @return Value from the set of strings as a string
   */
  public static String getValueAsString(String rowKeySeparator,
      Set<String> values) {

    if (values == null) {
      return EMPTY_STRING;
    }
    StringBuilder concatStrings = new StringBuilder();
    for (String value : values) {
      concatStrings.append(value);
      concatStrings.append(rowKeySeparator);
    }
    // remove the last separator
    if(concatStrings.length() > 1) {
      concatStrings.deleteCharAt(concatStrings.lastIndexOf(rowKeySeparator));
    }
    return concatStrings.toString();
  }
  /**
   * Constructs a row key prefix for the entity table
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @param appId
   * @return byte array with the row key prefix
   */
  static byte[] getRowKeyPrefix(String clusterId, String userId, String flowId,
      Long flowRunId, String appId) {
    return TimelineWriterUtils.join(
        TimelineEntitySchemaConstants.ROW_KEY_SEPARATOR_BYTES,
        Bytes.toBytes(cleanse(userId)), Bytes.toBytes(cleanse(clusterId)),
        Bytes.toBytes(cleanse(flowId)),
        Bytes.toBytes(TimelineWriterUtils.encodeRunId(flowRunId)),
        Bytes.toBytes(cleanse(appId)));
 }

  /**
   * Takes a string token to be used as a key or qualifier and
   * cleanses out reserved tokens.
   * This operation is not symmetrical.
   * Logic is to replace all spaces and separator chars in input with
   * underscores.
   *
   * @param token token to cleanse.
   * @return String with no spaces and no separator chars
   */
  public static String cleanse(String token) {
    if (token == null || token.length() == 0) {
      return token;
    }

    String cleansed = token.replaceAll(SPACE, UNDERSCORE);
    cleansed = cleansed.replaceAll(
        TimelineEntitySchemaConstants.ROW_KEY_SEPARATOR, UNDERSCORE);

    return cleansed;
  }

  /**
   * stores the info to the table in hbase
   * 
   * @param rowKey
   * @param table
   * @param columnFamily
   * @param columnPrefix
   * @param columnQualifier
   * @param inputValue
   * @param cellTimeStamp
   * @throws IOException
   */
  public static void store(byte[] rowKey, BufferedMutator table, byte[] columnFamily,
      byte[] columnPrefix, byte[] columnQualifier, Object inputValue,
      Long cellTimeStamp) throws IOException {
    if ((rowKey == null) || (table == null) || (columnFamily == null)
        || (columnQualifier == null) || (inputValue == null)) {
      return;
    }

    Put p = null;
    if (cellTimeStamp == null) {
      if (columnPrefix != null) {
        // store with prefix
        p = new Put(rowKey);
        p.addColumn(
            columnFamily,
            join(TimelineEntitySchemaConstants.ROW_KEY_SEPARATOR_BYTES,
                columnPrefix, columnQualifier), GenericObjectMapper
                .write(inputValue));
      } else {
        // store without prefix
        p = new Put(rowKey);
        p.addColumn(columnFamily, columnQualifier,
            GenericObjectMapper.write(inputValue));
      }
    } else {
      // store with cell timestamp
      Cell cell = CellUtil.createCell(rowKey, columnFamily, columnQualifier,
          // set the cell timestamp
          cellTimeStamp,
          // KeyValue Type minimum
          TimelineEntitySchemaConstants.ZERO_BYTES,
          GenericObjectMapper.write(inputValue));
      p = new Put(rowKey);
      p.add(cell);
    }
    if (p != null) {
      table.mutate(p);
    }

  }

}