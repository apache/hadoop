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
 * Encodes and decodes row key for flow run table.
 * The row key is of the form : clusterId!userId!flowName!flowrunId.
 * flowrunId is a long and rest are strings.
 */
public final class FlowRunRowKeyConverter implements
    KeyConverter<FlowRunRowKey> {
  private static final FlowRunRowKeyConverter INSTANCE =
      new FlowRunRowKeyConverter();

  public static FlowRunRowKeyConverter getInstance() {
    return INSTANCE;
  }

  private FlowRunRowKeyConverter() {
  }

  // Flow run row key is of the form
  // clusterId!userId!flowName!flowrunId with each segment separated by !.
  // The sizes below indicate sizes of each one of these segments in sequence.
  // clusterId, userId and flowName are strings. flowrunId is a long hence 8
  // bytes in size. Strings are variable in size (i.e. end whenever separator is
  // encountered). This is used while decoding and helps in determining where to
  // split.
  private static final int[] SEGMENT_SIZES = {
      Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
      Bytes.SIZEOF_LONG };

  /*
   * (non-Javadoc)
   *
   * Encodes FlowRunRowKey object into a byte array with each component/field in
   * FlowRunRowKey separated by Separator#QUALIFIERS. This leads to an
   * flow run row key of the form clusterId!userId!flowName!flowrunId
   * If flowRunId in passed FlowRunRowKey object is null (and the fields
   * preceding it i.e. clusterId, userId and flowName are not null), this
   * returns a row key prefix of the form clusterId!userName!flowName!
   * flowRunId is inverted while encoding as it helps maintain a descending
   * order for flow keys in flow run table.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #encode(java.lang.Object)
   */
  @Override
  public byte[] encode(FlowRunRowKey rowKey) {
    byte[] first = Separator.QUALIFIERS.join(
        Separator.encode(rowKey.getClusterId(), Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS),
        Separator.encode(rowKey.getUserId(), Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS),
        Separator.encode(rowKey.getFlowName(), Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS));
    if (rowKey.getFlowRunId() == null) {
      return Separator.QUALIFIERS.join(first, Separator.EMPTY_BYTES);
    } else {
      // Note that flowRunId is a long, so we can't encode them all at the same
      // time.
      byte[] second = Bytes.toBytes(TimelineStorageUtils.invertLong(
          rowKey.getFlowRunId()));
      return Separator.QUALIFIERS.join(first, second);
    }
  }

  /*
   * (non-Javadoc)
   *
   * Decodes an flow run row key of the form
   * clusterId!userId!flowName!flowrunId represented in byte format and converts
   * it into an FlowRunRowKey object. flowRunId is inverted while decoding as
   * it was inverted while encoding.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #decode(byte[])
   */
  @Override
  public FlowRunRowKey decode(byte[] rowKey) {
    byte[][] rowKeyComponents =
        Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
    if (rowKeyComponents.length != 4) {
      throw new IllegalArgumentException("the row key is not valid for " +
          "a flow run");
    }
    String clusterId = Separator.decode(Bytes.toString(rowKeyComponents[0]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    String userId = Separator.decode(Bytes.toString(rowKeyComponents[1]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    String flowName = Separator.decode(Bytes.toString(rowKeyComponents[2]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    Long flowRunId =
        TimelineStorageUtils.invertLong(Bytes.toLong(rowKeyComponents[3]));
    return new FlowRunRowKey(clusterId, userId, flowName, flowRunId);
  }
}
