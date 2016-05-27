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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;

/**
 * Encodes and decodes row key for flow activity table.
 * The row key is of the form : clusterId!dayTimestamp!user!flowName.
 * dayTimestamp(top of the day timestamp) is a long and rest are strings.
 */
public final class FlowActivityRowKeyConverter implements
    KeyConverter<FlowActivityRowKey> {
  private static final FlowActivityRowKeyConverter INSTANCE =
      new FlowActivityRowKeyConverter();

  public static FlowActivityRowKeyConverter getInstance() {
    return INSTANCE;
  }

  private FlowActivityRowKeyConverter() {
  }

  // Flow activity row key is of the form clusterId!dayTimestamp!user!flowName
  // with each segment separated by !. The sizes below indicate sizes of each
  // one of these segements in sequence. clusterId, user and flowName are
  // strings. Top of the day timestamp is a long hence 8 bytes in size.
  // Strings are variable in size (i.e. end whenever separator is encountered).
  // This is used while decoding and helps in determining where to split.
  private static final int[] SEGMENT_SIZES = {
      Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG, Separator.VARIABLE_SIZE,
      Separator.VARIABLE_SIZE };

  /*
   * (non-Javadoc)
   *
   * Encodes FlowActivityRowKey object into a byte array with each
   * component/field in FlowActivityRowKey separated by Separator#QUALIFIERS.
   * This leads to an flow activity table row key of the form
   * clusterId!dayTimestamp!user!flowName
   * If dayTimestamp in passed FlowActivityRowKey object is null and clusterId
   * is not null, this returns a row key prefix as clusterId! and if userId in
   * FlowActivityRowKey is null (and the fields preceding it i.e. clusterId and
   * dayTimestamp are not null), this returns a row key prefix as
   * clusterId!dayTimeStamp!
   * dayTimestamp is inverted while encoding as it helps maintain a descending
   * order for row keys in flow activity table.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #encode(java.lang.Object)
   */

  @Override
  public byte[] encode(FlowActivityRowKey rowKey) {
    if (rowKey.getDayTimestamp() == null) {
      return Separator.QUALIFIERS.join(Separator.encode(rowKey.getClusterId(),
          Separator.SPACE, Separator.TAB, Separator.QUALIFIERS),
              Separator.EMPTY_BYTES);
    }
    if (rowKey.getUserId() == null) {
      return Separator.QUALIFIERS.join(Separator.encode(rowKey.getClusterId(),
          Separator.SPACE, Separator.TAB, Separator.QUALIFIERS),
          Bytes.toBytes(TimelineStorageUtils.invertLong(
              rowKey.getDayTimestamp())), Separator.EMPTY_BYTES);
    }
    return Separator.QUALIFIERS.join(
        Separator.encode(rowKey.getClusterId(), Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS),
        Bytes.toBytes(
            TimelineStorageUtils.invertLong(rowKey.getDayTimestamp())),
        Separator.encode(rowKey.getUserId(), Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS),
        Separator.encode(rowKey.getFlowName(), Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS));
  }

  @Override
  public FlowActivityRowKey decode(byte[] rowKey) {
    byte[][] rowKeyComponents =
        Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
    if (rowKeyComponents.length != 4) {
      throw new IllegalArgumentException("the row key is not valid for "
          + "a flow activity");
    }
    String clusterId = Separator.decode(Bytes.toString(rowKeyComponents[0]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    Long dayTs =
        TimelineStorageUtils.invertLong(Bytes.toLong(rowKeyComponents[1]));
    String userId = Separator.decode(Bytes.toString(rowKeyComponents[2]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    String flowName = Separator.decode(Bytes.toString(rowKeyComponents[3]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    return new FlowActivityRowKey(clusterId, dayTs, userId, flowName);
  }
}
