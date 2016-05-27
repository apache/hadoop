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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.AppIdKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * Encodes and decodes row key for app_flow table.
 * The row key is of the form : clusterId!appId.
 * clusterId is a string and appId is encoded/decoded using
 * {@link AppIdKeyConverter}.
 */
public final class AppToFlowRowKeyConverter
    implements KeyConverter<AppToFlowRowKey> {
  private static final AppToFlowRowKeyConverter INSTANCE =
      new AppToFlowRowKeyConverter();

  public static AppToFlowRowKeyConverter getInstance() {
    return INSTANCE;
  }

  private AppToFlowRowKeyConverter() {
  }

  // App to flow row key is of the form clusterId!appId with the 2 segments
  // separated by !. The sizes below indicate sizes of both of these segments
  // in sequence. clusterId is a string. appId is represented as 12 bytes with
  // cluster Timestamp part of appid being 8 bytes(long) and seq id being 4
  // bytes(int).
  // Strings are variable in size (i.e. end whenever separator is encountered).
  // This is used while decoding and helps in determining where to split.
  private static final int[] SEGMENT_SIZES = {
      Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT };

  /*
   * (non-Javadoc)
   *
   * Encodes AppToFlowRowKey object into a byte array with each component/field
   * in AppToFlowRowKey separated by Separator#QUALIFIERS. This leads to an
   * app to flow table row key of the form clusterId!appId
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #encode(java.lang.Object)
   */
  @Override
  public byte[] encode(AppToFlowRowKey rowKey) {
    byte[] first = Separator.encode(rowKey.getClusterId(),
        Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
    byte[] second = AppIdKeyConverter.getInstance().encode(rowKey.getAppId());
    return Separator.QUALIFIERS.join(first, second);
  }

  /*
   * (non-Javadoc)
   *
   * Decodes an app to flow row key of the form clusterId!appId represented in
   * byte format and converts it into an AppToFlowRowKey object.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #decode(byte[])
   */
  @Override
  public AppToFlowRowKey decode(byte[] rowKey) {
    byte[][] rowKeyComponents =
        Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
    if (rowKeyComponents.length != 2) {
      throw new IllegalArgumentException("the row key is not valid for " +
          "the app-to-flow table");
    }
    String clusterId = Separator.decode(Bytes.toString(rowKeyComponents[0]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    String appId = AppIdKeyConverter.getInstance().decode(rowKeyComponents[1]);
    return new AppToFlowRowKey(clusterId, appId);
  }
}
