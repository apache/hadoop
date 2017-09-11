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

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.AppIdKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverterToString;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * Represents a rowkey for the application table.
 */
public class ApplicationRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowName;
  private final Long flowRunId;
  private final String appId;
  private final ApplicationRowKeyConverter appRowKeyConverter =
      new ApplicationRowKeyConverter();

  public ApplicationRowKey(String clusterId, String userId, String flowName,
      Long flowRunId, String appId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = flowName;
    this.flowRunId = flowRunId;
    this.appId = appId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getUserId() {
    return userId;
  }

  public String getFlowName() {
    return flowName;
  }

  public Long getFlowRunId() {
    return flowRunId;
  }

  public String getAppId() {
    return appId;
  }

  /**
   * Constructs a row key for the application table as follows:
   * {@code clusterId!userName!flowName!flowRunId!AppId}.
   *
   * @return byte array with the row key
   */
  public byte[] getRowKey() {
    return appRowKeyConverter.encode(this);
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   *
   * @param rowKey Byte representation of row key.
   * @return An <cite>ApplicationRowKey</cite> object.
   */
  public static ApplicationRowKey parseRowKey(byte[] rowKey) {
    return new ApplicationRowKeyConverter().decode(rowKey);
  }

  /**
   * Constructs a row key for the application table as follows:
   * {@code clusterId!userName!flowName!flowRunId!AppId}.
   * @return String representation of row key.
   */
  public String getRowKeyAsString() {
    return appRowKeyConverter.encodeAsString(this);
  }

  /**
   * Given the encoded row key as string, returns the row key as an object.
   * @param encodedRowKey String representation of row key.
   * @return A <cite>ApplicationRowKey</cite> object.
   */
  public static ApplicationRowKey parseRowKeyFromString(String encodedRowKey) {
    return new ApplicationRowKeyConverter().decodeFromString(encodedRowKey);
  }

  /**
   * Encodes and decodes row key for application table. The row key is of the
   * form: clusterId!userName!flowName!flowRunId!appId. flowRunId is a long,
   * appId is encoded and decoded using {@link AppIdKeyConverter} and rest are
   * strings.
   * <p>
   */
  final private static class ApplicationRowKeyConverter implements
      KeyConverter<ApplicationRowKey>, KeyConverterToString<ApplicationRowKey> {

    private final KeyConverter<String> appIDKeyConverter =
        new AppIdKeyConverter();

    /**
     * Intended for use in ApplicationRowKey only.
     */
    private ApplicationRowKeyConverter() {
    }

    /**
     * Application row key is of the form
     * clusterId!userName!flowName!flowRunId!appId with each segment separated
     * by !. The sizes below indicate sizes of each one of these segements in
     * sequence. clusterId, userName and flowName are strings. flowrunId is a
     * long hence 8 bytes in size. app id is represented as 12 bytes with
     * cluster timestamp part of appid takes 8 bytes(long) and seq id takes 4
     * bytes(int). Strings are variable in size (i.e. end whenever separator is
     * encountered). This is used while decoding and helps in determining where
     * to split.
     */
    private static final int[] SEGMENT_SIZES = {Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
        AppIdKeyConverter.getKeySize() };

    /*
     * (non-Javadoc)
     *
     * Encodes ApplicationRowKey object into a byte array with each
     * component/field in ApplicationRowKey separated by Separator#QUALIFIERS.
     * This leads to an application table row key of the form
     * clusterId!userName!flowName!flowRunId!appId If flowRunId in passed
     * ApplicationRowKey object is null (and the fields preceding it i.e.
     * clusterId, userId and flowName are not null), this returns a row key
     * prefix of the form clusterId!userName!flowName! and if appId in
     * ApplicationRowKey is null (other 4 components all are not null), this
     * returns a row key prefix of the form
     * clusterId!userName!flowName!flowRunId! flowRunId is inverted while
     * encoding as it helps maintain a descending order for row keys in the
     * application table.
     *
     * @see
     * org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(ApplicationRowKey rowKey) {
      byte[] cluster =
          Separator.encode(rowKey.getClusterId(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);
      byte[] user =
          Separator.encode(rowKey.getUserId(), Separator.SPACE, Separator.TAB,
              Separator.QUALIFIERS);
      byte[] flow =
          Separator.encode(rowKey.getFlowName(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);
      byte[] first = Separator.QUALIFIERS.join(cluster, user, flow);
      // Note that flowRunId is a long, so we can't encode them all at the same
      // time.
      if (rowKey.getFlowRunId() == null) {
        return Separator.QUALIFIERS.join(first, Separator.EMPTY_BYTES);
      }
      byte[] second =
          Bytes.toBytes(LongConverter.invertLong(
              rowKey.getFlowRunId()));
      if (rowKey.getAppId() == null || rowKey.getAppId().isEmpty()) {
        return Separator.QUALIFIERS.join(first, second, Separator.EMPTY_BYTES);
      }
      byte[] third = appIDKeyConverter.encode(rowKey.getAppId());
      return Separator.QUALIFIERS.join(first, second, third);
    }

    /*
     * (non-Javadoc)
     *
     * Decodes an application row key of the form
     * clusterId!userName!flowName!flowRunId!appId represented in byte format
     * and converts it into an ApplicationRowKey object.flowRunId is inverted
     * while decoding as it was inverted while encoding.
     *
     * @see
     * org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#decode(byte[])
     */
    @Override
    public ApplicationRowKey decode(byte[] rowKey) {
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      if (rowKeyComponents.length != 5) {
        throw new IllegalArgumentException("the row key is not valid for "
            + "an application");
      }
      String clusterId =
          Separator.decode(Bytes.toString(rowKeyComponents[0]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      String userId =
          Separator.decode(Bytes.toString(rowKeyComponents[1]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      String flowName =
          Separator.decode(Bytes.toString(rowKeyComponents[2]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      Long flowRunId =
          LongConverter.invertLong(Bytes.toLong(rowKeyComponents[3]));
      String appId = appIDKeyConverter.decode(rowKeyComponents[4]);
      return new ApplicationRowKey(clusterId, userId, flowName, flowRunId,
          appId);
    }

    @Override
    public String encodeAsString(ApplicationRowKey key) {
      if (key.clusterId == null || key.userId == null || key.flowName == null
          || key.flowRunId == null || key.appId == null) {
        throw new IllegalArgumentException();
      }
      return TimelineReaderUtils
          .joinAndEscapeStrings(new String[] {key.clusterId, key.userId,
              key.flowName, key.flowRunId.toString(), key.appId});
    }

    @Override
    public ApplicationRowKey decodeFromString(String encodedRowKey) {
      List<String> split = TimelineReaderUtils.split(encodedRowKey);
      if (split == null || split.size() != 5) {
        throw new IllegalArgumentException(
            "Invalid row key for application table.");
      }
      Long flowRunId = Long.valueOf(split.get(3));
      return new ApplicationRowKey(split.get(0), split.get(1), split.get(2),
          flowRunId, split.get(4));
    }
  }

}
