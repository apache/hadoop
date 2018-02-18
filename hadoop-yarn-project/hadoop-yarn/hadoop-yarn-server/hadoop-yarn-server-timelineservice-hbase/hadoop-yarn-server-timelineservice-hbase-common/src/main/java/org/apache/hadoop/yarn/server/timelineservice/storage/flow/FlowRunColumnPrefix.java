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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineSchemaUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;

/**
 * Identifies partially qualified columns for the {@link FlowRunTable}.
 */
public enum FlowRunColumnPrefix implements ColumnPrefix<FlowRunTable> {

  /**
   * To store flow run info values.
   */
  METRIC(FlowRunColumnFamily.INFO, "m", null, new LongConverter());

  private final ColumnFamily<FlowRunTable> columnFamily;

  /**
   * Can be null for those cases where the provided column qualifier is the
   * entire column name.
   */
  private final String columnPrefix;
  private final byte[] columnPrefixBytes;
  private final ValueConverter valueConverter;

  private final AggregationOperation aggOp;

  /**
   * Private constructor, meant to be used by the enum definition.
   *
   * @param columnFamily that this column is stored in.
   * @param columnPrefix for this column.
   */
  private FlowRunColumnPrefix(ColumnFamily<FlowRunTable> columnFamily,
      String columnPrefix, AggregationOperation fra, ValueConverter converter) {
    this(columnFamily, columnPrefix, fra, converter, false);
  }

  private FlowRunColumnPrefix(ColumnFamily<FlowRunTable> columnFamily,
      String columnPrefix, AggregationOperation fra, ValueConverter converter,
      boolean compoundColQual) {
    this.valueConverter = converter;
    this.columnFamily = columnFamily;
    this.columnPrefix = columnPrefix;
    if (columnPrefix == null) {
      this.columnPrefixBytes = null;
    } else {
      // Future-proof by ensuring the right column prefix hygiene.
      this.columnPrefixBytes =
          Bytes.toBytes(Separator.SPACE.encode(columnPrefix));
    }
    this.aggOp = fra;
  }

  /**
   * @return the column name value
   */
  public String getColumnPrefix() {
    return columnPrefix;
  }

  public byte[] getColumnPrefixBytes() {
    return columnPrefixBytes.clone();
  }

  @Override
  public byte[] getColumnPrefixBytes(byte[] qualifierPrefix) {
    return ColumnHelper.getColumnQualifier(this.columnPrefixBytes,
        qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixBytes(String qualifierPrefix) {
    return ColumnHelper.getColumnQualifier(this.columnPrefixBytes,
        qualifierPrefix);
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
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    return HBaseTimelineSchemaUtils.combineAttributes(attributes, aggOp);
  }

  @Override
  public boolean supplementCellTimeStamp() {
    return true;
  }

  public AggregationOperation getAttribute() {
    return aggOp;
  }

  @Override
  public ValueConverter getValueConverter() {
    return valueConverter;
  }
}
