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
package org.apache.hadoop.yarn.server.timelineservice.storage.entity;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TypedBufferedMutator;

/**
 * Identifies partially qualified columns for the entity table.
 */
public enum EntityColumnPrefix implements ColumnPrefix<EntityTable> {

  /**
   * To store TimelineEntity getIsRelatedToEntities values.
   */
  IS_RELATED_TO(EntityColumnFamily.INFO, "s"),

  /**
   * To store TimelineEntity getRelatesToEntities values.
   */
  RELATES_TO(EntityColumnFamily.INFO, "r"),

  /**
   * To store TimelineEntity info values.
   */
  INFO(EntityColumnFamily.INFO, "i"),

  /**
   * Lifecycle events for an entity
   */
  EVENT(EntityColumnFamily.INFO, "e"),

  /**
   * Config column stores configuration with config key as the column name.
   */
  CONFIG(EntityColumnFamily.CONFIGS, null),

  /**
   * Metrics are stored with the metric name as the column name.
   */
  METRIC(EntityColumnFamily.METRICS, null);

  private final ColumnHelper<EntityTable> column;
  private final ColumnFamily<EntityTable> columnFamily;

  /**
   * Can be null for those cases where the provided column qualifier is the
   * entire column name.
   */
  private final String columnPrefix;
  private final byte[] columnPrefixBytes;

  /**
   * Private constructor, meant to be used by the enum definition.
   *
   * @param columnFamily that this column is stored in.
   * @param columnPrefix for this column.
   */
  EntityColumnPrefix(ColumnFamily<EntityTable> columnFamily,
      String columnPrefix) {
    column = new ColumnHelper<EntityTable>(columnFamily);
    this.columnFamily = columnFamily;
    this.columnPrefix = columnPrefix;
    if (columnPrefix == null) {
      this.columnPrefixBytes = null;
    } else {
      // Future-proof by ensuring the right column prefix hygiene.
      this.columnPrefixBytes =
          Bytes.toBytes(Separator.SPACE.encode(columnPrefix));
    }
  }

  /**
   * @return the column name value
   */
  public String getColumnPrefix() {
    return columnPrefix;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix
   * #store(byte[],
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.
   * TypedBufferedMutator, java.lang.String, java.lang.Long, java.lang.Object)
   */
  public void store(byte[] rowKey,
      TypedBufferedMutator<EntityTable> tableMutator, String qualifier,
      Long timestamp, Object inputValue) throws IOException {

    // Null check
    if (qualifier == null) {
      throw new IOException("Cannot store column with null qualifier in "
          + tableMutator.getName().getNameAsString());
    }

    byte[] columnQualifier =
        ColumnHelper.getColumnQualifier(this.columnPrefixBytes, qualifier);

    column.store(rowKey, tableMutator, columnQualifier, timestamp, inputValue);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix
   * #store(byte[],
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.
   * TypedBufferedMutator, java.lang.String, java.lang.Long, java.lang.Object)
   */
  public void store(byte[] rowKey,
      TypedBufferedMutator<EntityTable> tableMutator, byte[] qualifier,
      Long timestamp, Object inputValue) throws IOException {

    // Null check
    if (qualifier == null) {
      throw new IOException("Cannot store column with null qualifier in "
          + tableMutator.getName().getNameAsString());
    }

    byte[] columnQualifier =
        ColumnHelper.getColumnQualifier(this.columnPrefixBytes, qualifier);

    column.store(rowKey, tableMutator, columnQualifier, timestamp, inputValue);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix
   * #readResult(org.apache.hadoop.hbase.client.Result, java.lang.String)
   */
  public Object readResult(Result result, String qualifier) throws IOException {
    byte[] columnQualifier =
        ColumnHelper.getColumnQualifier(this.columnPrefixBytes, qualifier);
    return column.readResult(result, columnQualifier);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix
   * #readResults(org.apache.hadoop.hbase.client.Result)
   */
  public Map<String, Object> readResults(Result result) throws IOException {
    return column.readResults(result, columnPrefixBytes);
  }

  /**
   * @param result from which to read columns
   * @return the latest values of columns in the column family. The column
   *         qualifier is returned as a list of parts, each part a byte[]. This
   *         is to facilitate returning byte arrays of values that were not
   *         Strings. If they can be treated as Strings, you should use
   *         {@link #readResults(Result)} instead.
   * @throws IOException
   */
  public Map<?, Object> readResultsHavingCompoundColumnQualifiers(Result result)
          throws IOException {
    return column.readResultsHavingCompoundColumnQualifiers(result,
        columnPrefixBytes);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix
   * #readResultsWithTimestamps(org.apache.hadoop.hbase.client.Result)
   */
  public <V> NavigableMap<String, NavigableMap<Long, V>>
      readResultsWithTimestamps(Result result) throws IOException {
    return column.readResultsWithTimestamps(result, columnPrefixBytes);
  }

  /**
   * Retrieve an {@link EntityColumnPrefix} given a name, or null if there is no
   * match. The following holds true: {@code columnFor(x) == columnFor(y)} if
   * and only if {@code x.equals(y)} or {@code (x == y == null)}
   *
   * @param columnPrefix Name of the column to retrieve
   * @return the corresponding {@link EntityColumnPrefix} or null
   */
  public static final EntityColumnPrefix columnFor(String columnPrefix) {

    // Match column based on value, assume column family matches.
    for (EntityColumnPrefix ecp : EntityColumnPrefix.values()) {
      // Find a match based only on name.
      if (ecp.getColumnPrefix().equals(columnPrefix)) {
        return ecp;
      }
    }

    // Default to null
    return null;
  }

  /**
   * Retrieve an {@link EntityColumnPrefix} given a name, or null if there is no
   * match. The following holds true: {@code columnFor(a,x) == columnFor(b,y)}
   * if and only if {@code (x == y == null)} or
   * {@code a.equals(b) & x.equals(y)}
   *
   * @param columnFamily The columnFamily for which to retrieve the column.
   * @param columnPrefix Name of the column to retrieve
   * @return the corresponding {@link EntityColumnPrefix} or null if both
   *         arguments don't match.
   */
  public static final EntityColumnPrefix columnFor(
      EntityColumnFamily columnFamily, String columnPrefix) {

    // TODO: needs unit test to confirm and need to update javadoc to explain
    // null prefix case.

    for (EntityColumnPrefix ecp : EntityColumnPrefix.values()) {
      // Find a match based column family and on name.
      if (ecp.columnFamily.equals(columnFamily)
          && (((columnPrefix == null) && (ecp.getColumnPrefix() == null)) || (ecp
              .getColumnPrefix().equals(columnPrefix)))) {
        return ecp;
      }
    }

    // Default to null
    return null;
  }

}
