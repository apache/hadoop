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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.AppIdKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;

/**
 * Encodes and decodes row key for application table.
 * The row key is of the form : clusterId!userName!flowName!flowRunId!appId.
 * flowRunId is a long, appId is encoded and decoded using
 * {@link AppIdKeyConverter} and rest are strings.
 */
public final class ApplicationRowKeyConverter implements
    KeyConverter<ApplicationRowKey> {
  private static final ApplicationRowKeyConverter INSTANCE =
      new ApplicationRowKeyConverter();

  public static ApplicationRowKeyConverter getInstance() {
    return INSTANCE;
  }

  private ApplicationRowKeyConverter() {
  }

  // Application row key is of the form
  // clusterId!userName!flowName!flowRunId!appId with each segment separated
  // by !. The sizes below indicate sizes of each one of these segements in
  // sequence. clusterId, userName and flowName are strings. flowrunId is a long
  // hence 8 bytes in size. app id is represented as 12 bytes with cluster
  // timestamp part of appid being 8 bytes(long) and seq id being 4 bytes(int).
  // Strings are variable in size (i.e. end whenever separator is encountered).
  // This is used while decoding and helps in determining where to split.
  private static final int[] SEGMENT_SIZES = {
      Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
      Bytes.SIZEOF_LONG, AppIdKeyConverter.getKeySize() };

  /*
   * (non-Javadoc)
   *
   * Encodes ApplicationRowKey object into a byte array with each
   * component/field in ApplicationRowKey separated by Separator#QUALIFIERS.
   * This leads to an application table row key of the form
   * clusterId!userName!flowName!flowRunId!appId
   * If flowRunId in passed ApplicationRowKey object is null (and the fields
   * preceding it i.e. clusterId, userId and flowName are not null), this
   * returns a row key prefix of the form clusterId!userName!flowName! and if
   * appId in ApplicationRowKey is null (other 4 components are not null), this
   * returns a row key prefix of the form clusterId!userName!flowName!flowRunId!
   * flowRunId is inverted while encoding as it helps maintain a descending
   * order for row keys in application table.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #encode(java.lang.Object)
   */
  @Override
  public byte[] encode(ApplicationRowKey rowKey) {
    byte[] cluster = Separator.encode(rowKey.getClusterId(),
        Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
    byte[] user = Separator.encode(rowKey.getUserId(),
        Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
    byte[] flow = Separator.encode(rowKey.getFlowName(),
        Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
    byte[] first = Separator.QUALIFIERS.join(cluster, user, flow);
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    if (rowKey.getFlowRunId() == null) {
      return Separator.QUALIFIERS.join(first, Separator.EMPTY_BYTES);
    }
    byte[] second = Bytes.toBytes(
        TimelineStorageUtils.invertLong(rowKey.getFlowRunId()));
    if (rowKey.getAppId() == null || rowKey.getAppId().isEmpty()) {
      return Separator.QUALIFIERS.join(first, second, Separator.EMPTY_BYTES);
    }
    byte[] third = AppIdKeyConverter.getInstance().encode(rowKey.getAppId());
    return Separator.QUALIFIERS.join(first, second, third);
  }

  /*
   * (non-Javadoc)
   *
   * Decodes an application row key of the form
   * clusterId!userName!flowName!flowRunId!appId represented in byte format and
   * converts it into an ApplicationRowKey object.flowRunId is inverted while
   * decoding as it was inverted while encoding.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #decode(byte[])
   */
  @Override
  public ApplicationRowKey decode(byte[] rowKey) {
    byte[][] rowKeyComponents =
        Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
    if (rowKeyComponents.length != 5) {
      throw new IllegalArgumentException("the row key is not valid for " +
          "an application");
    }
    String clusterId = Separator.decode(Bytes.toString(rowKeyComponents[0]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    String userId = Separator.decode(Bytes.toString(rowKeyComponents[1]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    String flowName = Separator.decode(Bytes.toString(rowKeyComponents[2]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    Long flowRunId =
        TimelineStorageUtils.invertLong(Bytes.toLong(rowKeyComponents[3]));
    String appId = AppIdKeyConverter.getInstance().decode(rowKeyComponents[4]);
    return new ApplicationRowKey(clusterId, userId, flowName, flowRunId, appId);
  }
}
