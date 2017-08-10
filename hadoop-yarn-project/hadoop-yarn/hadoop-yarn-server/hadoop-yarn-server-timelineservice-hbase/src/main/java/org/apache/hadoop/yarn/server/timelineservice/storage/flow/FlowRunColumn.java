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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Column;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TypedBufferedMutator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;

/**
 * Identifies fully qualified columns for the {@link FlowRunTable}.
 */
public enum FlowRunColumn implements Column<FlowRunTable> {

  /**
   * When the flow was started. This is the minimum of currently known
   * application start times.
   */
  MIN_START_TIME(FlowRunColumnFamily.INFO, "min_start_time",
      AggregationOperation.GLOBAL_MIN, new LongConverter()),

  /**
   * When the flow ended. This is the maximum of currently known application end
   * times.
   */
  MAX_END_TIME(FlowRunColumnFamily.INFO, "max_end_time",
      AggregationOperation.GLOBAL_MAX, new LongConverter()),

  /**
   * The version of the flow that this flow belongs to.
   */
  FLOW_VERSION(FlowRunColumnFamily.INFO, "flow_version", null);

  private final ColumnHelper<FlowRunTable> column;
  private final ColumnFamily<FlowRunTable> columnFamily;
  private final String columnQualifier;
  private final byte[] columnQualifierBytes;
  private final AggregationOperation aggOp;

  private FlowRunColumn(ColumnFamily<FlowRunTable> columnFamily,
      String columnQualifier, AggregationOperation aggOp) {
    this(columnFamily, columnQualifier, aggOp,
        GenericConverter.getInstance());
  }

  private FlowRunColumn(ColumnFamily<FlowRunTable> columnFamily,
      String columnQualifier, AggregationOperation aggOp,
      ValueConverter converter) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    this.aggOp = aggOp;
    // Future-proof by ensuring the right column prefix hygiene.
    this.columnQualifierBytes = Bytes.toBytes(Separator.SPACE
        .encode(columnQualifier));
    this.column = new ColumnHelper<FlowRunTable>(columnFamily, converter, true);
  }

  /**
   * @return the column name value
   */
  private String getColumnQualifier() {
    return columnQualifier;
  }

  @Override
  public byte[] getColumnQualifierBytes() {
    return columnQualifierBytes.clone();
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    return columnFamily.getBytes();
  }

  public AggregationOperation getAggregationOperation() {
    return aggOp;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.Column#store
   * (byte[], org.apache.hadoop.yarn.server.timelineservice.storage.common.
   * TypedBufferedMutator, java.lang.Long, java.lang.Object,
   * org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute[])
   */
  public void store(byte[] rowKey,
      TypedBufferedMutator<FlowRunTable> tableMutator, Long timestamp,
      Object inputValue, Attribute... attributes) throws IOException {

    Attribute[] combinedAttributes =
        HBaseTimelineStorageUtils.combineAttributes(attributes, aggOp);
    column.store(rowKey, tableMutator, columnQualifierBytes, timestamp,
        inputValue, combinedAttributes);
  }

  public Object readResult(Result result) throws IOException {
    return column.readResult(result, columnQualifierBytes);
  }

  @Override
  public ValueConverter getValueConverter() {
    return column.getValueConverter();
  }

}
