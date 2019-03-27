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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A set of utility functions that read or read to a column.
 * This class is meant to be used only by explicit Columns,
 * and not directly to write by clients.
 */
public final class ColumnRWHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(ColumnHelper.class);

  private ColumnRWHelper() {
  }

  /**
   * Figures out the cell timestamp used in the Put For storing.
   * Will supplement the timestamp if required. Typically done for flow run
   * table.If we supplement the timestamp, we left shift the timestamp and
   * supplement it with the AppId id so that there are no collisions in the flow
   * run table's cells.
   */
  private static long getPutTimestamp(
      Long timestamp, boolean supplementTs, Attribute[] attributes) {
    if (timestamp == null) {
      timestamp = System.currentTimeMillis();
    }
    if (!supplementTs) {
      return timestamp;
    } else {
      String appId = getAppIdFromAttributes(attributes);
      long supplementedTS = TimestampGenerator.getSupplementedTimestamp(
          timestamp, appId);
      return supplementedTS;
    }
  }

  private static String getAppIdFromAttributes(Attribute[] attributes) {
    if (attributes == null) {
      return null;
    }
    String appId = null;
    for (Attribute attribute : attributes) {
      if (AggregationCompactionDimension.APPLICATION_ID.toString().equals(
          attribute.getName())) {
        appId = Bytes.toString(attribute.getValue());
      }
    }
    return appId;
  }

  /**
   * Sends a Mutation to the table. The mutations will be buffered and sent over
   * the wire as part of a batch.
   *
   * @param rowKey
   *          identifying the row to write. Nothing gets written when null.
   * @param tableMutator
   *          used to modify the underlying HBase table
   * @param column the column that is to be modified
   * @param timestamp
   *          version timestamp. When null the current timestamp multiplied with
   *          TimestampGenerator.TS_MULTIPLIER and added with last 3 digits of
   *          app id will be used
   * @param inputValue
   *          the value to write to the rowKey and column qualifier. Nothing
   *          gets written when null.
   * @param attributes Attributes to be set for HBase Put.
   * @throws IOException if any problem occurs during store operation(sending
   *          mutation to table).
   */
  public static void store(byte[] rowKey, TypedBufferedMutator<?> tableMutator,
                           Column<?> column, Long timestamp,
                           Object inputValue, Attribute... attributes)
      throws IOException {
    store(rowKey, tableMutator, column.getColumnFamilyBytes(),
        column.getColumnQualifierBytes(), timestamp,
        column.supplementCellTimestamp(), inputValue,
        column.getValueConverter(),
        column.getCombinedAttrsWithAggr(attributes));
  }

  /**
   * Sends a Mutation to the table. The mutations will be buffered and sent over
   * the wire as part of a batch.
   *
   * @param rowKey
   *          identifying the row to write. Nothing gets written when null.
   * @param tableMutator
   *          used to modify the underlying HBase table
   * @param columnFamilyBytes
   * @param columnQualifier
   *          column qualifier. Nothing gets written when null.
   * @param timestamp
   *          version timestamp. When null the current timestamp multiplied with
   *          TimestampGenerator.TS_MULTIPLIER and added with last 3 digits of
   *          app id will be used
   * @param inputValue
   *          the value to write to the rowKey and column qualifier. Nothing
   *          gets written when null.
   * @param converter
   * @param attributes Attributes to be set for HBase Put.
   * @throws IOException if any problem occurs during store operation(sending
   *          mutation to table).
   */
  public static void store(byte[] rowKey, TypedBufferedMutator<?> tableMutator,
      byte[] columnFamilyBytes, byte[] columnQualifier, Long timestamp,
      boolean supplementTs, Object inputValue, ValueConverter converter,
      Attribute... attributes) throws IOException {
    if ((rowKey == null) || (columnQualifier == null) || (inputValue == null)) {
      return;
    }
    Put p = new Put(rowKey);
    timestamp = getPutTimestamp(timestamp, supplementTs, attributes);
    p.addColumn(columnFamilyBytes, columnQualifier, timestamp,
        converter.encodeValue(inputValue));
    if ((attributes != null) && (attributes.length > 0)) {
      for (Attribute attribute : attributes) {
        p.setAttribute(attribute.getName(), attribute.getValue());
      }
    }
    tableMutator.mutate(p);
  }

  /**
   * Get the latest version of this specified column. Note: this call clones the
   * value content of the hosting {@link org.apache.hadoop.hbase.Cell Cell}.
   *
   * @param result from which to read the value. Cannot be null
   * @param columnFamilyBytes
   * @param columnQualifierBytes referring to the column to be read.
   * @param converter
   * @return latest version of the specified column of whichever object was
   *         written.
   * @throws IOException if any problem occurs while reading result.
   */
  public static Object readResult(Result result, byte[] columnFamilyBytes,
      byte[] columnQualifierBytes, ValueConverter converter)
      throws IOException {
    if (result == null || columnQualifierBytes == null) {
      return null;
    }

    // Would have preferred to be able to use getValueAsByteBuffer and get a
    // ByteBuffer to avoid copy, but GenericObjectMapper doesn't seem to like
    // that.
    byte[] value = result.getValue(columnFamilyBytes, columnQualifierBytes);
    return converter.decodeValue(value);
  }

  /**
   * Get the latest version of this specified column. Note: this call clones the
   * value content of the hosting {@link org.apache.hadoop.hbase.Cell Cell}.
   *
   * @param result from which to read the value. Cannot be null
   * @param column the column that the result can be parsed to
   * @return latest version of the specified column of whichever object was
   *         written.
   * @throws IOException if any problem occurs while reading result.
   */
  public static Object readResult(Result result, Column<?> column)
      throws IOException {
    return readResult(result, column.getColumnFamilyBytes(),
        column.getColumnQualifierBytes(), column.getValueConverter());
  }

  /**
   * Get the latest version of this specified column. Note: this call clones the
   * value content of the hosting {@link org.apache.hadoop.hbase.Cell Cell}.
   *
   * @param result Cannot be null
   * @param columnPrefix column prefix to read from
   * @param qualifier column qualifier. Nothing gets read when null.
   * @return result object (can be cast to whatever object was written to) or
   *         null when specified column qualifier for this prefix doesn't exist
   *         in the result.
   * @throws IOException if there is any exception encountered while reading
   *     result.
   */
  public static Object readResult(Result result, ColumnPrefix<?> columnPrefix,
                                  String qualifier) throws IOException {
    byte[] columnQualifier = ColumnHelper.getColumnQualifier(
        columnPrefix.getColumnPrefixInBytes(), qualifier);

    return readResult(
        result, columnPrefix.getColumnFamilyBytes(),
        columnQualifier, columnPrefix.getValueConverter());
  }

  /**
   *
   * @param <K> identifies the type of key converter.
   * @param result from which to read columns.
   * @param keyConverter used to convert column bytes to the appropriate key
   *          type
   * @return the latest values of columns in the column family with this prefix
   *         (or all of them if the prefix value is null).
   * @throws IOException if there is any exception encountered while reading
   *           results.
   */
  public static <K> Map<K, Object> readResults(Result result,
      ColumnPrefix<?> columnPrefix, KeyConverter<K> keyConverter)
      throws IOException {
    return readResults(result,
        columnPrefix.getColumnFamilyBytes(),
        columnPrefix.getColumnPrefixInBytes(),
        keyConverter, columnPrefix.getValueConverter());
  }

  /**
   * @param result from which to reads data with timestamps.
   * @param <K> identifies the type of key converter.
   * @param <V> the type of the values. The values will be cast into that type.
   * @param keyConverter used to convert column bytes to the appropriate key
   *     type.
   * @return the cell values at each respective time in for form
   *         {@literal {idA={timestamp1->value1}, idA={timestamp2->value2},
   *         idB={timestamp3->value3}, idC={timestamp1->value4}}}
   * @throws IOException if there is any exception encountered while reading
   *     result.
   */
  public static <K, V> NavigableMap<K, NavigableMap<Long, V>>
      readResultsWithTimestamps(Result result, ColumnPrefix<?> columnPrefix,
      KeyConverter<K> keyConverter) throws IOException {
    return readResultsWithTimestamps(result,
        columnPrefix.getColumnFamilyBytes(),
        columnPrefix.getColumnPrefixInBytes(),
        keyConverter, columnPrefix.getValueConverter(),
        columnPrefix.supplementCellTimeStamp());
  }

  /**
   * @param result from which to reads data with timestamps
   * @param columnPrefixBytes optional prefix to limit columns. If null all
   *          columns are returned.
   * @param <K> identifies the type of column name(indicated by type of key
   *     converter).
   * @param <V> the type of the values. The values will be cast into that type.
   * @param keyConverter used to convert column bytes to the appropriate key
   *     type.
   * @return the cell values at each respective time in for form
   *         {@literal {idA={timestamp1->value1}, idA={timestamp2->value2},
   *         idB={timestamp3->value3}, idC={timestamp1->value4}}}
   * @throws IOException if any problem occurs while reading results.
   */
  @SuppressWarnings("unchecked")
  public static <K, V> NavigableMap<K, NavigableMap<Long, V>>
      readResultsWithTimestamps(Result result, byte[] columnFamilyBytes,
          byte[] columnPrefixBytes, KeyConverter<K> keyConverter,
          ValueConverter valueConverter, boolean supplementTs)
      throws IOException {

    NavigableMap<K, NavigableMap<Long, V>> results = new TreeMap<>();

    if (result != null) {
      NavigableMap<
          byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap =
          result.getMap();

      NavigableMap<byte[], NavigableMap<Long, byte[]>> columnCellMap =
          resultMap.get(columnFamilyBytes);
      // could be that there is no such column family.
      if (columnCellMap != null) {
        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entry : columnCellMap
            .entrySet()) {
          K converterColumnKey = null;
          if (columnPrefixBytes == null) {
            LOG.debug("null prefix was specified; returning all columns");
            try {
              converterColumnKey = keyConverter.decode(entry.getKey());
            } catch (IllegalArgumentException iae) {
              LOG.error("Illegal column found, skipping this column.", iae);
              continue;
            }
          } else {
            // A non-null prefix means columns are actually of the form
            // prefix!columnNameRemainder
            byte[][] columnNameParts =
                Separator.QUALIFIERS.split(entry.getKey(), 2);
            byte[] actualColumnPrefixBytes = columnNameParts[0];
            if (Bytes.equals(columnPrefixBytes, actualColumnPrefixBytes)
                && columnNameParts.length == 2) {
              try {
                // This is the prefix that we want
                converterColumnKey = keyConverter.decode(columnNameParts[1]);
              } catch (IllegalArgumentException iae) {
                LOG.error("Illegal column found, skipping this column.", iae);
                continue;
              }
            }
          }

          // If this column has the prefix we want
          if (converterColumnKey != null) {
            NavigableMap<Long, V> cellResults =
                new TreeMap<Long, V>();
            NavigableMap<Long, byte[]> cells = entry.getValue();
            if (cells != null) {
              for (Map.Entry<Long, byte[]> cell : cells.entrySet()) {
                V value =
                    (V) valueConverter.decodeValue(cell.getValue());
                Long ts = supplementTs ? TimestampGenerator.
                    getTruncatedTimestamp(cell.getKey()) : cell.getKey();
                cellResults.put(ts, value);
              }
            }
            results.put(converterColumnKey, cellResults);
          }
        } // for entry : columnCellMap
      } // if columnCellMap != null
    } // if result != null
    return results;
  }

  /**
   * @param <K> identifies the type of column name(indicated by type of key
   *     converter).
   * @param result from which to read columns
   * @param columnPrefixBytes optional prefix to limit columns. If null all
   *        columns are returned.
   * @param keyConverter used to convert column bytes to the appropriate key
   *          type.
   * @return the latest values of columns in the column family. If the column
   *         prefix is null, the column qualifier is returned as Strings. For a
   *         non-null column prefix bytes, the column qualifier is returned as
   *         a list of parts, each part a byte[]. This is to facilitate
   *         returning byte arrays of values that were not Strings.
   * @throws IOException if any problem occurs while reading results.
   */
  public static <K> Map<K, Object> readResults(Result result,
      byte[] columnFamilyBytes, byte[] columnPrefixBytes,
      KeyConverter<K> keyConverter, ValueConverter valueConverter)
      throws IOException {
    Map<K, Object> results = new HashMap<K, Object>();

    if (result != null) {
      Map<byte[], byte[]> columns = result.getFamilyMap(columnFamilyBytes);
      for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
        byte[] columnKey = entry.getKey();
        if (columnKey != null && columnKey.length > 0) {

          K converterColumnKey = null;
          if (columnPrefixBytes == null) {
            try {
              converterColumnKey = keyConverter.decode(columnKey);
            } catch (IllegalArgumentException iae) {
              LOG.error("Illegal column found, skipping this column.", iae);
              continue;
            }
          } else {
            // A non-null prefix means columns are actually of the form
            // prefix!columnNameRemainder
            byte[][] columnNameParts = Separator.QUALIFIERS.split(columnKey, 2);
            if (columnNameParts.length > 0) {
              byte[] actualColumnPrefixBytes = columnNameParts[0];
              // If this is the prefix that we want
              if (Bytes.equals(columnPrefixBytes, actualColumnPrefixBytes)
                  && columnNameParts.length == 2) {
                try {
                  converterColumnKey = keyConverter.decode(columnNameParts[1]);
                } catch (IllegalArgumentException iae) {
                  LOG.error("Illegal column found, skipping this column.", iae);
                  continue;
                }
              }
            }
          } // if-else

          // If the columnPrefix is null (we want all columns), or the actual
          // prefix matches the given prefix we want this column
          if (converterColumnKey != null) {
            Object value = valueConverter.decodeValue(entry.getValue());
            // we return the columnQualifier in parts since we don't know
            // which part is of which data type.
            results.put(converterColumnKey, value);
          }
        }
      } // for entry
    }
    return results;
  }

  /**
   * Sends a Mutation to the table. The mutations will be buffered and sent over
   * the wire as part of a batch.
   *
   * @param rowKey identifying the row to write. Nothing gets written when null.
   * @param tableMutator used to modify the underlying HBase table. Caller is
   *          responsible to pass a mutator for the table that actually has this
   *          column.
   * @param qualifier column qualifier. Nothing gets written when null.
   * @param timestamp version timestamp. When null the server timestamp will be
   *          used.
   * @param attributes attributes for the mutation that are used by the
   *          coprocessor to set/read the cell tags.
   * @param inputValue the value to write to the rowKey and column qualifier.
   *          Nothing gets written when null.
   * @throws IOException if there is any exception encountered while doing
   *     store operation(sending mutation to the table).
   */
  public static void store(byte[] rowKey, TypedBufferedMutator<?> tableMutator,
             ColumnPrefix<?> columnPrefix, byte[] qualifier, Long timestamp,
             Object inputValue, Attribute... attributes) throws IOException {
    // Null check
    if (qualifier == null) {
      throw new IOException("Cannot store column with null qualifier in "
          +tableMutator.getName().getNameAsString());
    }

    byte[] columnQualifier = columnPrefix.getColumnPrefixBytes(qualifier);
    Attribute[] combinedAttributes =
        columnPrefix.getCombinedAttrsWithAggr(attributes);

    store(rowKey, tableMutator, columnPrefix.getColumnFamilyBytes(),
        columnQualifier, timestamp, columnPrefix.supplementCellTimeStamp(),
        inputValue, columnPrefix.getValueConverter(), combinedAttributes);
  }

  /**
   * Sends a Mutation to the table. The mutations will be buffered and sent over
   * the wire as part of a batch.
   *
   * @param rowKey identifying the row to write. Nothing gets written when null.
   * @param tableMutator used to modify the underlying HBase table. Caller is
   *          responsible to pass a mutator for the table that actually has this
   *          column.
   * @param qualifier column qualifier. Nothing gets written when null.
   * @param timestamp version timestamp. When null the server timestamp will be
   *          used.
   * @param attributes attributes for the mutation that are used by the
   *          coprocessor to set/read the cell tags.
   * @param inputValue the value to write to the rowKey and column qualifier.
   *          Nothing gets written when null.
   * @throws IOException if there is any exception encountered while doing
   *     store operation(sending mutation to the table).
   */
  public static void store(byte[] rowKey, TypedBufferedMutator<?> tableMutator,
             ColumnPrefix<?> columnPrefix, String qualifier, Long timestamp,
             Object inputValue, Attribute... attributes) throws IOException {
    // Null check
    if (qualifier == null) {
      throw new IOException("Cannot store column with null qualifier in "
          + tableMutator.getName().getNameAsString());
    }

    byte[] columnQualifier = columnPrefix.getColumnPrefixBytes(qualifier);
    Attribute[] combinedAttributes =
        columnPrefix.getCombinedAttrsWithAggr(attributes);

    store(rowKey, tableMutator, columnPrefix.getColumnFamilyBytes(),
        columnQualifier, timestamp, columnPrefix.supplementCellTimeStamp(),
        inputValue, columnPrefix.getValueConverter(), combinedAttributes);
  }
}
