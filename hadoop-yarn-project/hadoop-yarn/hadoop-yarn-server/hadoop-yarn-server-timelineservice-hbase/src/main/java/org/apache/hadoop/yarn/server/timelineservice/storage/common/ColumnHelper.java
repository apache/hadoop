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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is meant to be used only by explicit Columns, and not directly to
 * write by clients.
 *
 * @param <T> refers to the table.
 */
public class ColumnHelper<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ColumnHelper.class);

  private final ColumnFamily<T> columnFamily;

  /**
   * Local copy of bytes representation of columnFamily so that we can avoid
   * cloning a new copy over and over.
   */
  private final byte[] columnFamilyBytes;

  private final ValueConverter converter;

  private final boolean supplementTs;

  public ColumnHelper(ColumnFamily<T> columnFamily) {
    this(columnFamily, GenericConverter.getInstance());
  }

  public ColumnHelper(ColumnFamily<T> columnFamily, ValueConverter converter) {
    this(columnFamily, converter, false);
  }

  /**
   * @param columnFamily column family implementation.
   * @param converter converter use to encode/decode values stored in the column
   *     or column prefix.
   * @param needSupplementTs flag to indicate if cell timestamp needs to be
   *     modified for this column by calling
   *     {@link TimestampGenerator#getSupplementedTimestamp(long, String)}. This
   *     would be required for columns(such as metrics in flow run table) where
   *     potential collisions can occur due to same timestamp.
   */
  public ColumnHelper(ColumnFamily<T> columnFamily, ValueConverter converter,
      boolean needSupplementTs) {
    this.columnFamily = columnFamily;
    columnFamilyBytes = columnFamily.getBytes();
    if (converter == null) {
      this.converter = GenericConverter.getInstance();
    } else {
      this.converter = converter;
    }
    this.supplementTs = needSupplementTs;
  }

  /**
   * Sends a Mutation to the table. The mutations will be buffered and sent over
   * the wire as part of a batch.
   *
   * @param rowKey
   *          identifying the row to write. Nothing gets written when null.
   * @param tableMutator
   *          used to modify the underlying HBase table
   * @param columnQualifier
   *          column qualifier. Nothing gets written when null.
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
  public void store(byte[] rowKey, TypedBufferedMutator<?> tableMutator,
      byte[] columnQualifier, Long timestamp, Object inputValue,
      Attribute... attributes) throws IOException {
    if ((rowKey == null) || (columnQualifier == null) || (inputValue == null)) {
      return;
    }
    Put p = new Put(rowKey);
    timestamp = getPutTimestamp(timestamp, attributes);
    p.addColumn(columnFamilyBytes, columnQualifier, timestamp,
        converter.encodeValue(inputValue));
    if ((attributes != null) && (attributes.length > 0)) {
      for (Attribute attribute : attributes) {
        p.setAttribute(attribute.getName(), attribute.getValue());
      }
    }
    tableMutator.mutate(p);
  }

  /*
   * Figures out the cell timestamp used in the Put For storing.
   * Will supplement the timestamp if required. Typically done for flow run
   * table.If we supplement the timestamp, we left shift the timestamp and
   * supplement it with the AppId id so that there are no collisions in the flow
   * run table's cells.
   */
  private long getPutTimestamp(Long timestamp, Attribute[] attributes) {
    if (timestamp == null) {
      timestamp = System.currentTimeMillis();
    }
    if (!this.supplementTs) {
      return timestamp;
    } else {
      String appId = getAppIdFromAttributes(attributes);
      long supplementedTS = TimestampGenerator.getSupplementedTimestamp(
          timestamp, appId);
      return supplementedTS;
    }
  }

  private String getAppIdFromAttributes(Attribute[] attributes) {
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
   * @return the column family for this column implementation.
   */
  public ColumnFamily<T> getColumnFamily() {
    return columnFamily;
  }

  /**
   * Get the latest version of this specified column. Note: this call clones the
   * value content of the hosting {@link org.apache.hadoop.hbase.Cell Cell}.
   *
   * @param result from which to read the value. Cannot be null
   * @param columnQualifierBytes referring to the column to be read.
   * @return latest version of the specified column of whichever object was
   *         written.
   * @throws IOException if any problem occurs while reading result.
   */
  public Object readResult(Result result, byte[] columnQualifierBytes)
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
  public <K, V> NavigableMap<K, NavigableMap<Long, V>>
      readResultsWithTimestamps(Result result, byte[] columnPrefixBytes,
          KeyConverter<K> keyConverter) throws IOException {

    NavigableMap<K, NavigableMap<Long, V>> results = new TreeMap<>();

    if (result != null) {
      NavigableMap<
          byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap =
              result.getMap();

      NavigableMap<byte[], NavigableMap<Long, byte[]>> columnCellMap =
          resultMap.get(columnFamilyBytes);
      // could be that there is no such column family.
      if (columnCellMap != null) {
        for (Entry<byte[], NavigableMap<Long, byte[]>> entry : columnCellMap
            .entrySet()) {
          K converterColumnKey = null;
          if (columnPrefixBytes == null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("null prefix was specified; returning all columns");
            }
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
              for (Entry<Long, byte[]> cell : cells.entrySet()) {
                V value =
                    (V) converter.decodeValue(cell.getValue());
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
  public <K> Map<K, Object> readResults(Result result,
      byte[] columnPrefixBytes, KeyConverter<K> keyConverter)
      throws IOException {
    Map<K, Object> results = new HashMap<K, Object>();

    if (result != null) {
      Map<byte[], byte[]> columns = result.getFamilyMap(columnFamilyBytes);
      for (Entry<byte[], byte[]> entry : columns.entrySet()) {
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
            Object value = converter.decodeValue(entry.getValue());
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
   * @param columnPrefixBytes The byte representation for the column prefix.
   *          Should not contain {@link Separator#QUALIFIERS}.
   * @param qualifier for the remainder of the column.
   *          {@link Separator#QUALIFIERS} is permissible in the qualifier
   *          as it is joined only with the column prefix bytes.
   * @return fully sanitized column qualifier that is a combination of prefix
   *         and qualifier. If prefix is null, the result is simply the encoded
   *         qualifier without any separator.
   */
  public static byte[] getColumnQualifier(byte[] columnPrefixBytes,
      String qualifier) {

    // We don't want column names to have spaces / tabs.
    byte[] encodedQualifier =
        Separator.encode(qualifier, Separator.SPACE, Separator.TAB);
    if (columnPrefixBytes == null) {
      return encodedQualifier;
    }

    // Convert qualifier to lower case, strip of separators and tag on column
    // prefix.
    byte[] columnQualifier =
        Separator.QUALIFIERS.join(columnPrefixBytes, encodedQualifier);
    return columnQualifier;
  }

  /**
   * @param columnPrefixBytes The byte representation for the column prefix.
   *          Should not contain {@link Separator#QUALIFIERS}.
   * @param qualifier for the remainder of the column.
   * @return fully sanitized column qualifier that is a combination of prefix
   *         and qualifier. If prefix is null, the result is simply the encoded
   *         qualifier without any separator.
   */
  public static byte[] getColumnQualifier(byte[] columnPrefixBytes,
      long qualifier) {

    if (columnPrefixBytes == null) {
      return Bytes.toBytes(qualifier);
    }

    // Convert qualifier to lower case, strip of separators and tag on column
    // prefix.
    byte[] columnQualifier =
        Separator.QUALIFIERS.join(columnPrefixBytes, Bytes.toBytes(qualifier));
    return columnQualifier;
  }

  public ValueConverter getValueConverter() {
    return converter;
  }

  /**
   * @param columnPrefixBytes The byte representation for the column prefix.
   *          Should not contain {@link Separator#QUALIFIERS}.
   * @param qualifier the byte representation for the remainder of the column.
   * @return fully sanitized column qualifier that is a combination of prefix
   *         and qualifier. If prefix is null, the result is simply the encoded
   *         qualifier without any separator.
   */
  public static byte[] getColumnQualifier(byte[] columnPrefixBytes,
      byte[] qualifier) {

    if (columnPrefixBytes == null) {
      return qualifier;
    }

    byte[] columnQualifier =
        Separator.QUALIFIERS.join(columnPrefixBytes, qualifier);
    return columnQualifier;
  }

}
