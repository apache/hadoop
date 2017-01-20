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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * Represents a rowkey for the flow activity table.
 */
public class FlowActivityRowKey {

  private final String clusterId;
  private final Long dayTs;
  private final String userId;
  private final String flowName;
  private final KeyConverter<FlowActivityRowKey> flowActivityRowKeyConverter =
      new FlowActivityRowKeyConverter();

  /**
   * @param clusterId identifying the cluster
   * @param dayTs to be converted to the top of the day timestamp
   * @param userId identifying user
   * @param flowName identifying the flow
   */
  public FlowActivityRowKey(String clusterId, Long dayTs, String userId,
      String flowName) {
    this(clusterId, dayTs, userId, flowName, true);
  }

  /**
   * @param clusterId identifying the cluster
   * @param timestamp when the flow activity happened. May be converted to the
   *          top of the day depending on the convertDayTsToTopOfDay argument.
   * @param userId identifying user
   * @param flowName identifying the flow
   * @param convertDayTsToTopOfDay if true and timestamp isn't null, then
   *          timestamp will be converted to the top-of-the day timestamp
   */
  protected FlowActivityRowKey(String clusterId, Long timestamp, String userId,
      String flowName, boolean convertDayTsToTopOfDay) {
    this.clusterId = clusterId;
    if (convertDayTsToTopOfDay && (timestamp != null)) {
      this.dayTs = HBaseTimelineStorageUtils.getTopOfTheDayTimestamp(timestamp);
    } else {
      this.dayTs = timestamp;
    }
    this.userId = userId;
    this.flowName = flowName;
  }

  public String getClusterId() {
    return clusterId;
  }

  public Long getDayTimestamp() {
    return dayTs;
  }

  public String getUserId() {
    return userId;
  }

  public String getFlowName() {
    return flowName;
  }

  /**
   * Constructs a row key for the flow activity table as follows:
   * {@code clusterId!dayTimestamp!user!flowName}.
   *
   * @return byte array for the row key
   */
  public byte[] getRowKey() {
    return flowActivityRowKeyConverter.encode(this);
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   *
   * @param rowKey Byte representation of row key.
   * @return A <cite>FlowActivityRowKey</cite> object.
   */
  public static FlowActivityRowKey parseRowKey(byte[] rowKey) {
    return new FlowActivityRowKeyConverter().decode(rowKey);
  }

  /**
   * Encodes and decodes row key for flow activity table. The row key is of the
   * form : clusterId!dayTimestamp!user!flowName. dayTimestamp(top of the day
   * timestamp) is a long and rest are strings.
   * <p>
   */
  final private static class FlowActivityRowKeyConverter implements
      KeyConverter<FlowActivityRowKey> {

    private FlowActivityRowKeyConverter() {
    }

    /**
     * The flow activity row key is of the form
     * clusterId!dayTimestamp!user!flowName with each segment separated by !.
     * The sizes below indicate sizes of each one of these segements in
     * sequence. clusterId, user and flowName are strings. Top of the day
     * timestamp is a long hence 8 bytes in size. Strings are variable in size
     * (i.e. they end whenever separator is encountered). This is used while
     * decoding and helps in determining where to split.
     */
    private static final int[] SEGMENT_SIZES = {Separator.VARIABLE_SIZE,
        Bytes.SIZEOF_LONG, Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE };

    /*
     * (non-Javadoc)
     *
     * Encodes FlowActivityRowKey object into a byte array with each
     * component/field in FlowActivityRowKey separated by Separator#QUALIFIERS.
     * This leads to an flow activity table row key of the form
     * clusterId!dayTimestamp!user!flowName. If dayTimestamp in passed
     * FlowActivityRowKey object is null and clusterId is not null, then this
     * returns a row key prefix as clusterId! and if userId in
     * FlowActivityRowKey is null (and the fields preceding it i.e. clusterId
     * and dayTimestamp are not null), this returns a row key prefix as
     * clusterId!dayTimeStamp! dayTimestamp is inverted while encoding as it
     * helps maintain a descending order for row keys in flow activity table.
     *
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(FlowActivityRowKey rowKey) {
      if (rowKey.getDayTimestamp() == null) {
        return Separator.QUALIFIERS.join(Separator.encode(
            rowKey.getClusterId(), Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS), Separator.EMPTY_BYTES);
      }
      if (rowKey.getUserId() == null) {
        return Separator.QUALIFIERS.join(Separator.encode(
            rowKey.getClusterId(), Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS), Bytes.toBytes(LongConverter
            .invertLong(rowKey.getDayTimestamp())), Separator.EMPTY_BYTES);
      }
      return Separator.QUALIFIERS.join(Separator.encode(rowKey.getClusterId(),
          Separator.SPACE, Separator.TAB, Separator.QUALIFIERS), Bytes
          .toBytes(LongConverter.invertLong(rowKey.getDayTimestamp())),
          Separator.encode(rowKey.getUserId(), Separator.SPACE, Separator.TAB,
              Separator.QUALIFIERS), Separator.encode(rowKey.getFlowName(),
              Separator.SPACE, Separator.TAB, Separator.QUALIFIERS));
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#decode(byte[])
     */
    @Override
    public FlowActivityRowKey decode(byte[] rowKey) {
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      if (rowKeyComponents.length != 4) {
        throw new IllegalArgumentException("the row key is not valid for "
            + "a flow activity");
      }
      String clusterId =
          Separator.decode(Bytes.toString(rowKeyComponents[0]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      Long dayTs = LongConverter.invertLong(Bytes.toLong(rowKeyComponents[1]));
      String userId =
          Separator.decode(Bytes.toString(rowKeyComponents[2]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      String flowName =
          Separator.decode(Bytes.toString(rowKeyComponents[3]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      return new FlowActivityRowKey(clusterId, dayTs, userId, flowName);
    }
  }
}
