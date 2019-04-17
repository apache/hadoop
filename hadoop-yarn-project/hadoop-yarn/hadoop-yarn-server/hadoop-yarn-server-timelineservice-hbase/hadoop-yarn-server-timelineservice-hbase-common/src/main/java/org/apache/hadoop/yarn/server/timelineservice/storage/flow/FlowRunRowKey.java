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

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverterToString;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * Represents a rowkey for the flow run table.
 */
public class FlowRunRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowName;
  private final Long flowRunId;
  private final FlowRunRowKeyConverter flowRunRowKeyConverter =
      new FlowRunRowKeyConverter();

  public FlowRunRowKey(String clusterId, String userId, String flowName,
      Long flowRunId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = flowName;
    this.flowRunId = flowRunId;
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

  /**
   * Constructs a row key for the entity table as follows: {
   * clusterId!userId!flowName!Inverted Flow Run Id}.
   *
   * @return byte array with the row key
   */
  public byte[] getRowKey() {
    return flowRunRowKeyConverter.encode(this);
  }


  /**
   * Given the raw row key as bytes, returns the row key as an object.
   * @param rowKey Byte representation of row key.
   * @return A <cite>FlowRunRowKey</cite> object.
   */
  public static FlowRunRowKey parseRowKey(byte[] rowKey) {
    return new FlowRunRowKeyConverter().decode(rowKey);
  }

  /**
   * Constructs a row key for the flow run table as follows:
   * {@code clusterId!userId!flowName!Flow Run Id}.
   * @return String representation of row key
   */
  public String getRowKeyAsString() {
    return flowRunRowKeyConverter.encodeAsString(this);
  }

  /**
   * Given the encoded row key as string, returns the row key as an object.
   * @param encodedRowKey String representation of row key.
   * @return A <cite>FlowRunRowKey</cite> object.
   */
  public static FlowRunRowKey parseRowKeyFromString(String encodedRowKey) {
    return new FlowRunRowKeyConverter().decodeFromString(encodedRowKey);
  }

  /**
   * returns the Flow Key as a verbose String output.
   * @return String
   */
  @Override
  public String toString() {
    StringBuilder flowKeyStr = new StringBuilder();
    flowKeyStr.append("{clusterId=" + clusterId)
        .append(" userId=" + userId)
        .append(" flowName=" + flowName)
        .append(" flowRunId=")
        .append(flowRunId)
        .append("}");
    return flowKeyStr.toString();
  }

  /**
   * Encodes and decodes row key for flow run table.
   * The row key is of the form : clusterId!userId!flowName!flowrunId.
   * flowrunId is a long and rest are strings.
   * <p>
   */
  final private static class FlowRunRowKeyConverter implements
      KeyConverter<FlowRunRowKey>, KeyConverterToString<FlowRunRowKey> {

    private FlowRunRowKeyConverter() {
    }

    /**
     * The flow run row key is of the form clusterId!userId!flowName!flowrunId
     * with each segment separated by !. The sizes below indicate sizes of each
     * one of these segments in sequence. clusterId, userId and flowName are
     * strings. flowrunId is a long hence 8 bytes in size. Strings are variable
     * in size (i.e. end whenever separator is encountered). This is used while
     * decoding and helps in determining where to split.
     */
    private static final int[] SEGMENT_SIZES = {Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG };

    /*
     * (non-Javadoc)
     *
     * Encodes FlowRunRowKey object into a byte array with each component/field
     * in FlowRunRowKey separated by Separator#QUALIFIERS. This leads to an flow
     * run row key of the form clusterId!userId!flowName!flowrunId If flowRunId
     * in passed FlowRunRowKey object is null (and the fields preceding it i.e.
     * clusterId, userId and flowName are not null), this returns a row key
     * prefix of the form clusterId!userName!flowName! flowRunId is inverted
     * while encoding as it helps maintain a descending order for flow keys in
     * flow run table.
     *
     * @see
     * org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(FlowRunRowKey rowKey) {
      byte[] first =
          Separator.QUALIFIERS.join(Separator.encode(rowKey.getClusterId(),
              Separator.SPACE, Separator.TAB, Separator.QUALIFIERS), Separator
              .encode(rowKey.getUserId(), Separator.SPACE, Separator.TAB,
                  Separator.QUALIFIERS), Separator.encode(rowKey.getFlowName(),
              Separator.SPACE, Separator.TAB, Separator.QUALIFIERS));
      if (rowKey.getFlowRunId() == null) {
        return Separator.QUALIFIERS.join(first, Separator.EMPTY_BYTES);
      } else {
        // Note that flowRunId is a long, so we can't encode them all at the
        // same
        // time.
        byte[] second =
            Bytes.toBytes(LongConverter.invertLong(rowKey.getFlowRunId()));
        return Separator.QUALIFIERS.join(first, second);
      }
    }

    /*
     * (non-Javadoc)
     *
     * Decodes an flow run row key of the form
     * clusterId!userId!flowName!flowrunId represented in byte format and
     * converts it into an FlowRunRowKey object. flowRunId is inverted while
     * decoding as it was inverted while encoding.
     *
     * @see
     * org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#decode(byte[])
     */
    @Override
    public FlowRunRowKey decode(byte[] rowKey) {
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      if (rowKeyComponents.length != 4) {
        throw new IllegalArgumentException("the row key is not valid for "
            + "a flow run");
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
      return new FlowRunRowKey(clusterId, userId, flowName, flowRunId);
    }

    @Override
    public String encodeAsString(FlowRunRowKey key) {
      if (key.clusterId == null || key.userId == null || key.flowName == null
          || key.flowRunId == null) {
        throw new IllegalArgumentException();
      }
      return TimelineReaderUtils.joinAndEscapeStrings(new String[] {
          key.clusterId, key.userId, key.flowName, key.flowRunId.toString()});
    }

    @Override
    public FlowRunRowKey decodeFromString(String encodedRowKey) {
      List<String> split = TimelineReaderUtils.split(encodedRowKey);
      if (split == null || split.size() != 4) {
        throw new IllegalArgumentException(
            "Invalid row key for flow run table.");
      }
      Long flowRunId = Long.valueOf(split.get(3));
      return new FlowRunRowKey(split.get(0), split.get(1), split.get(2),
          flowRunId);
    }
  }
}
