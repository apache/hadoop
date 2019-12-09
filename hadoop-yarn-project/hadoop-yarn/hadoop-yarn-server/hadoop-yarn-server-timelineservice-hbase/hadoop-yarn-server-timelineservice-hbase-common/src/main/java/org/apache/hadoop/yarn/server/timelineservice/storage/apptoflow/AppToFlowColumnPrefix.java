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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
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

  private final ColumnFamily<AppToFlowTable> columnFamily;
  private final String columnPrefix;
  private final byte[] columnPrefixBytes;
  private final ValueConverter valueConverter;

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
    this.valueConverter = GenericConverter.getInstance();
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
  public byte[] getColumnPrefixInBytes() {
    return columnPrefixBytes != null ? columnPrefixBytes.clone() : null;
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    return columnFamily.getBytes();
  }

  @Override
  public ValueConverter getValueConverter() {
    return valueConverter;
  }

  @Override
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    return attributes;
  }

  @Override
  public boolean supplementCellTimeStamp() {
    return false;
  }
}
