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

package org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TypedBufferedMutator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * Identifies partially qualified columns for the app-to-flow table.
 */
public enum AppToFlowColumnPrefix implements ColumnPrefix<AppToFlowTable> {

  /**
   * The flow name.
   */
  FLOW_NAME(AppToFlowColumnFamily.MAPPING, "flow_name"),

  /**
   * The flow run ID.
   */
  FLOW_RUN_ID(AppToFlowColumnFamily.MAPPING, "flow_run_id"),

  /**
   * The user.
   */
  USER_ID(AppToFlowColumnFamily.MAPPING, "user_id");

  private final ColumnHelper<AppToFlowTable> column;
  private final ColumnFamily<AppToFlowTable> columnFamily;
  private final String columnPrefix;
  private final byte[] columnPrefixBytes;

  AppToFlowColumnPrefix(ColumnFamily<AppToFlowTable> columnFamily,
      String columnPrefix) {
    this.columnFamily = columnFamily;
    this.columnPrefix = columnPrefix;
    if (columnPrefix == null) {
      this.columnPrefixBytes = null;
    } else {
      // Future-proof by ensuring the right column prefix hygiene.
      this.columnPrefixBytes =
          Bytes.toBytes(Separator.SPACE.encode(columnPrefix));
    }
    this.column = new ColumnHelper<AppToFlowTable>(columnFamily);
  }

  @Override
  public byte[] getColumnPrefixBytes(String qualifierPrefix) {
    return ColumnHelper.getColumnQualifier(
        columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixBytes(byte[] qualifierPrefix) {
    return ColumnHelper.getColumnQualifier(
        columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    return columnFamily.getBytes();
  }

  @Override
  public void store(byte[] rowKey,
      TypedBufferedMutator<AppToFlowTable> tableMutator, byte[] qualifier,
      Long timestamp, Object inputValue, Attribute... attributes)
      throws IOException {

    // Null check
    if (qualifier == null) {
      throw new IOException("Cannot store column with null qualifier in "
          + tableMutator.getName().getNameAsString());
    }

    byte[] columnQualifier = getColumnPrefixBytes(qualifier);

    column.store(rowKey, tableMutator, columnQualifier, timestamp, inputValue,
        attributes);
  }

  @Override
  public void store(byte[] rowKey,
      TypedBufferedMutator<AppToFlowTable> tableMutator, String qualifier,
      Long timestamp, Object inputValue, Attribute... attributes)
      throws IOException {

    // Null check
    if (qualifier == null) {
      throw new IOException("Cannot store column with null qualifier in "
          + tableMutator.getName().getNameAsString());
    }

    byte[] columnQualifier = getColumnPrefixBytes(qualifier);

    column.store(rowKey, tableMutator, columnQualifier, timestamp, inputValue,
        attributes);
  }

  @Override
  public ValueConverter getValueConverter() {
    return column.getValueConverter();
  }

  @Override
  public Object readResult(Result result, String qualifier) throws IOException {
    byte[] columnQualifier =
        ColumnHelper.getColumnQualifier(columnPrefixBytes, qualifier);
    return column.readResult(result, columnQualifier);
  }

  @Override
  public <K> Map<K, Object> readResults(Result result,
      KeyConverter<K> keyConverter)
      throws IOException {
    return column.readResults(result, columnPrefixBytes, keyConverter);
  }

  @Override
  public <K, V> NavigableMap<K, NavigableMap<Long, V>>
      readResultsWithTimestamps(Result result,
      KeyConverter<K> keyConverter) throws IOException {
    return column.readResultsWithTimestamps(result, columnPrefixBytes,
        keyConverter);
  }

  /**
   * Retrieve an {@link AppToFlowColumnPrefix} given a name, or null if there
   * is no match. The following holds true: {@code columnFor(x) == columnFor(y)}
   * if and only if {@code x.equals(y)} or {@code (x == y == null)}
   *
   * @param columnPrefix Name of the column to retrieve
   * @return the corresponding {@link AppToFlowColumnPrefix} or null
   */
  public static final AppToFlowColumnPrefix columnFor(String columnPrefix) {

    // Match column based on value, assume column family matches.
    for (AppToFlowColumnPrefix afcp : AppToFlowColumnPrefix.values()) {
      // Find a match based only on name.
      if (afcp.columnPrefix.equals(columnPrefix)) {
        return afcp;
      }
    }

    // Default to null
    return null;
  }

  /**
   * Retrieve an {@link AppToFlowColumnPrefix} given a name, or null if there
   * is no match. The following holds true:
   * {@code columnFor(a,x) == columnFor(b,y)} if and only if
   * {@code (x == y == null)} or {@code a.equals(b) & x.equals(y)}
   *
   * @param columnFamily The columnFamily for which to retrieve the column.
   * @param columnPrefix Name of the column to retrieve
   * @return the corresponding {@link AppToFlowColumnPrefix} or null if both
   *         arguments don't match.
   */
  public static final AppToFlowColumnPrefix columnFor(
      AppToFlowColumnFamily columnFamily, String columnPrefix) {

    // TODO: needs unit test to confirm and need to update javadoc to explain
    // null prefix case.

    for (AppToFlowColumnPrefix afcp : AppToFlowColumnPrefix.values()) {
      // Find a match based column family and on name.
      if (afcp.columnFamily.equals(columnFamily)
          && (((columnPrefix == null) && (afcp.columnPrefix == null)) ||
          (afcp.columnPrefix.equals(columnPrefix)))) {
        return afcp;
      }
    }

    // Default to null
    return null;
  }
}
