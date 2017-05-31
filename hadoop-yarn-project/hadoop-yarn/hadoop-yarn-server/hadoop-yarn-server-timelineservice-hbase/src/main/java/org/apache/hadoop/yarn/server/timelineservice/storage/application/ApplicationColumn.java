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
package org.apache.hadoop.yarn.server.timelineservice.storage.application;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Column;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TypedBufferedMutator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * Identifies fully qualified columns for the {@link ApplicationTable}.
 */
public enum ApplicationColumn implements Column<ApplicationTable> {

  /**
   * App id.
   */
  ID(ApplicationColumnFamily.INFO, "id"),

  /**
   * When the application was created.
   */
  CREATED_TIME(ApplicationColumnFamily.INFO, "created_time",
      new LongConverter()),

  /**
   * The version of the flow that this app belongs to.
   */
  FLOW_VERSION(ApplicationColumnFamily.INFO, "flow_version");

  private final ColumnHelper<ApplicationTable> column;
  private final ColumnFamily<ApplicationTable> columnFamily;
  private final String columnQualifier;
  private final byte[] columnQualifierBytes;

  private ApplicationColumn(ColumnFamily<ApplicationTable> columnFamily,
      String columnQualifier) {
    this(columnFamily, columnQualifier, GenericConverter.getInstance());
  }

  private ApplicationColumn(ColumnFamily<ApplicationTable> columnFamily,
      String columnQualifier, ValueConverter converter) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    // Future-proof by ensuring the right column prefix hygiene.
    this.columnQualifierBytes =
        Bytes.toBytes(Separator.SPACE.encode(columnQualifier));
    this.column = new ColumnHelper<ApplicationTable>(columnFamily, converter);
  }

  /**
   * @return the column name value
   */
  private String getColumnQualifier() {
    return columnQualifier;
  }

  public void store(byte[] rowKey,
      TypedBufferedMutator<ApplicationTable> tableMutator, Long timestamp,
      Object inputValue, Attribute... attributes) throws IOException {
    column.store(rowKey, tableMutator, columnQualifierBytes, timestamp,
        inputValue, attributes);
  }

  public Object readResult(Result result) throws IOException {
    return column.readResult(result, columnQualifierBytes);
  }

  @Override
  public byte[] getColumnQualifierBytes() {
    return columnQualifierBytes.clone();
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    return columnFamily.getBytes();
  }

  @Override
  public ValueConverter getValueConverter() {
    return column.getValueConverter();
  }

}
