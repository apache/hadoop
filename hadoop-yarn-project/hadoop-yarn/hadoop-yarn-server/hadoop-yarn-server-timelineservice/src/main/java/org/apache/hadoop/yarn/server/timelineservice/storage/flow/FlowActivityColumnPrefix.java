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
package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TypedBufferedMutator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;

/**
 * Identifies partially qualified columns for the {@link FlowActivityTable}.
 */
public enum FlowActivityColumnPrefix
    implements ColumnPrefix<FlowActivityTable> {

  /**
   * To store run ids of the flows.
   */
  RUN_ID(FlowActivityColumnFamily.INFO, "r", null);

  private final ColumnHelper<FlowActivityTable> column;
  private final ColumnFamily<FlowActivityTable> columnFamily;

  /**
   * Can be null for those cases where the provided column qualifier is the
   * entire column name.
   */
  private final String columnPrefix;
  private final byte[] columnPrefixBytes;

  private final AggregationOperation aggOp;

  /**
   * Private constructor, meant to be used by the enum definition.
   *
   * @param columnFamily
   *          that this column is stored in.
   * @param columnPrefix
   *          for this column.
   */
  private FlowActivityColumnPrefix(
      ColumnFamily<FlowActivityTable> columnFamily, String columnPrefix,
      AggregationOperation aggOp) {
    this(columnFamily, columnPrefix, aggOp, false);
  }

  private FlowActivityColumnPrefix(
      ColumnFamily<FlowActivityTable> columnFamily, String columnPrefix,
      AggregationOperation aggOp, boolean compoundColQual) {
    column = new ColumnHelper<FlowActivityTable>(columnFamily);
    this.columnFamily = columnFamily;
    this.columnPrefix = columnPrefix;
    if (columnPrefix == null) {
      this.columnPrefixBytes = null;
    } else {
      // Future-proof by ensuring the right column prefix hygiene.
      this.columnPrefixBytes = Bytes.toBytes(Separator.SPACE
          .encode(columnPrefix));
    }
    this.aggOp = aggOp;
  }

  /**
   * @return the column name value
   */
  public String getColumnPrefix() {
    return columnPrefix;
  }

  @Override
  public byte[] getColumnPrefixBytes(byte[] qualifierPrefix) {
    return ColumnHelper.getColumnQualifier(
        this.columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixBytes(String qualifierPrefix) {
    return ColumnHelper.getColumnQualifier(
        this.columnPrefixBytes, qualifierPrefix);
  }

  public byte[] getColumnPrefixBytes() {
    return columnPrefixBytes.clone();
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    return columnFamily.getBytes();
  }

  @Override
  public ValueConverter getValueConverter() {
    return column.getValueConverter();
  }

  public AggregationOperation getAttribute() {
    return aggOp;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix
   * #store(byte[],
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.
   * TypedBufferedMutator, byte[], java.lang.Long, java.lang.Object,
   * org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute[])
   */
  @Override
  public void store(byte[] rowKey,
      TypedBufferedMutator<FlowActivityTable> tableMutator, byte[] qualifier,
      Long timestamp, Object inputValue, Attribute... attributes)
      throws IOException {
    // Null check
    if (qualifier == null) {
      throw new IOException("Cannot store column with null qualifier in "
          + tableMutator.getName().getNameAsString());
    }

    byte[] columnQualifier = getColumnPrefixBytes(qualifier);
    Attribute[] combinedAttributes =
        HBaseTimelineStorageUtils.combineAttributes(attributes, this.aggOp);
    column.store(rowKey, tableMutator, columnQualifier, timestamp, inputValue,
        combinedAttributes);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix
   * #readResult(org.apache.hadoop.hbase.client.Result, java.lang.String)
   */
  public Object readResult(Result result, String qualifier) throws IOException {
    byte[] columnQualifier = ColumnHelper.getColumnQualifier(
        this.columnPrefixBytes, qualifier);
    return column.readResult(result, columnQualifier);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix
   * #readResults(org.apache.hadoop.hbase.client.Result,
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter)
   */
  public <K> Map<K, Object> readResults(Result result,
      KeyConverter<K> keyConverter) throws IOException {
    return column.readResults(result, columnPrefixBytes, keyConverter);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix
   * #readResultsWithTimestamps(org.apache.hadoop.hbase.client.Result,
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter)
   */
  public <K, V> NavigableMap<K, NavigableMap<Long, V>>
      readResultsWithTimestamps(Result result, KeyConverter<K> keyConverter)
      throws IOException {
    return column.readResultsWithTimestamps(result, columnPrefixBytes,
        keyConverter);
  }

  /**
   * Retrieve an {@link FlowActivityColumnPrefix} given a name, or null if there
   * is no match. The following holds true: {@code columnFor(x) == columnFor(y)}
   * if and only if {@code x.equals(y)} or {@code (x == y == null)}
   *
   * @param columnPrefix
   *          Name of the column to retrieve
   * @return the corresponding {@link FlowActivityColumnPrefix} or null
   */
  public static final FlowActivityColumnPrefix columnFor(String columnPrefix) {

    // Match column based on value, assume column family matches.
    for (FlowActivityColumnPrefix flowActivityColPrefix :
        FlowActivityColumnPrefix.values()) {
      // Find a match based only on name.
      if (flowActivityColPrefix.getColumnPrefix().equals(columnPrefix)) {
        return flowActivityColPrefix;
      }
    }
    // Default to null
    return null;
  }

  /**
   * Retrieve an {@link FlowActivityColumnPrefix} given a name, or null if there
   * is no match. The following holds true:
   * {@code columnFor(a,x) == columnFor(b,y)} if and only if
   * {@code (x == y == null)} or {@code a.equals(b) & x.equals(y)}
   *
   * @param columnFamily
   *          The columnFamily for which to retrieve the column.
   * @param columnPrefix
   *          Name of the column to retrieve
   * @return the corresponding {@link FlowActivityColumnPrefix} or null if both
   *         arguments don't match.
   */
  public static final FlowActivityColumnPrefix columnFor(
      FlowActivityColumnFamily columnFamily, String columnPrefix) {

    // TODO: needs unit test to confirm and need to update javadoc to explain
    // null prefix case.

    for (FlowActivityColumnPrefix flowActivityColumnPrefix :
        FlowActivityColumnPrefix.values()) {
      // Find a match based column family and on name.
      if (flowActivityColumnPrefix.columnFamily.equals(columnFamily)
          && (((columnPrefix == null) && (flowActivityColumnPrefix
              .getColumnPrefix() == null)) || (flowActivityColumnPrefix
              .getColumnPrefix().equals(columnPrefix)))) {
        return flowActivityColumnPrefix;
      }
    }
    // Default to null
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix
   * #store(byte[],
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.
   * TypedBufferedMutator, java.lang.String, java.lang.Long, java.lang.Object,
   * org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute[])
   */
  @Override
  public void store(byte[] rowKey,
      TypedBufferedMutator<FlowActivityTable> tableMutator, String qualifier,
      Long timestamp, Object inputValue, Attribute... attributes)
      throws IOException {
    // Null check
    if (qualifier == null) {
      throw new IOException("Cannot store column with null qualifier in "
          + tableMutator.getName().getNameAsString());
    }

    byte[] columnQualifier = getColumnPrefixBytes(qualifier);
    Attribute[] combinedAttributes =
        HBaseTimelineStorageUtils.combineAttributes(attributes, this.aggOp);
    column.store(rowKey, tableMutator, columnQualifier, null, inputValue,
        combinedAttributes);
  }
}