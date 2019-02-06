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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineSchemaUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
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

  private final ColumnFamily<FlowActivityTable> columnFamily;
  private final ValueConverter valueConverter;

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
    this.valueConverter = GenericConverter.getInstance();
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
  public byte[] getColumnPrefixInBytes() {
    return columnPrefixBytes != null ? columnPrefixBytes.clone() : null;
  }

  @Override
  public ValueConverter getValueConverter() {
    return valueConverter;
  }

  @Override
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    return HBaseTimelineSchemaUtils.combineAttributes(attributes, aggOp);
  }

  @Override
  public boolean supplementCellTimeStamp() {
    return false;
  }

  public AggregationOperation getAttribute() {
    return aggOp;
  }
}