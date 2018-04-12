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
package org.apache.hadoop.yarn.server.timelineservice.storage.domain;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverterToString;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;

/**
 * Represents a row key for the domain table, which is the
 * cluster ! domain id.
 */
public class DomainRowKey {
  private final String clusterId;
  private final String domainId;
  private final DomainRowKeyConverter domainIdKeyConverter =
      new DomainRowKeyConverter();

  public DomainRowKey(String clusterId, String domainId) {
    this.clusterId = clusterId;
    this.domainId = domainId;
  }


  public String getClusterId() {
    return clusterId;
  }

  public String getDomainId() {
    return domainId;
  }

  /**
   * Constructs a row key prefix for the domain table.
   *
   * @return byte array with the row key
   */
  public  byte[] getRowKey() {

    return domainIdKeyConverter.encode(this);
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   *
   * @param rowKey a rowkey represented as a byte array.
   * @return an <cite>DomainRowKey</cite> object.
   */
  public static DomainRowKey parseRowKey(byte[] rowKey) {
    return new DomainRowKeyConverter().decode(rowKey);
  }

  /**
   * Constructs a row key for the domain table as follows:
   * <p>
   * {@code clusterId!domainId}.
   * </p>
   * @return String representation of row key.
   */
  public String getRowKeyAsString() {
    return domainIdKeyConverter.encodeAsString(this);
  }

  /**
   * Given the encoded row key as string, returns the row key as an object.
   * @param encodedRowKey String representation of row key.
   * @return A <cite>DomainRowKey</cite> object.
   */
  public static DomainRowKey parseRowKeyFromString(String encodedRowKey) {
    return new DomainRowKeyConverter().decodeFromString(encodedRowKey);
  }

  /**
   * Encodes and decodes row key for the domain table.
   * The row key is of the
   * form : domainId
   * <p>
   */
  final private static class DomainRowKeyConverter
      implements KeyConverter<DomainRowKey>,
      KeyConverterToString<DomainRowKey> {

    private DomainRowKeyConverter() {
    }

    /**
     * The domain row key is of the form
     * clusterId!domainId with each segment separated by !.
     * The sizes below indicate sizes of each one of
     * these segements in sequence.
     * clusterId and domainId are strings.
     * Strings are variable in size
     * (i.e. they end whenever separator is encountered).
     * This is used while
     * decoding and helps in determining where to split.
     */
    private static final int[] SEGMENT_SIZES = {
        Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE};

    /*
     * (non-Javadoc)
     *
     * Encodes DomainRowKey object into a byte array
     *
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(DomainRowKey rowKey) {
      if (rowKey == null) {
        return Separator.EMPTY_BYTES;
      }

      byte[] cluster =
          Separator.encode(rowKey.getClusterId(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);
      byte[] domainIdBytes =
          Separator.encode(rowKey.getDomainId(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);

      return Separator.QUALIFIERS.join(cluster, domainIdBytes);
    }

    @Override
    public DomainRowKey decode(byte[] rowKey) {
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      if (rowKeyComponents.length != 2) {
        throw new IllegalArgumentException("the row key is not valid for "
            + "a domain id");
      }
      String clusterId =
          Separator.decode(Bytes.toString(rowKeyComponents[0]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);

      String domainId =
          Separator.decode(Bytes.toString(rowKeyComponents[1]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);

      return new DomainRowKey(clusterId, domainId);
    }

    @Override
    public String encodeAsString(DomainRowKey key) {
      return TimelineReaderUtils.joinAndEscapeStrings(
          new String[] {key.clusterId, key.domainId});
    }

    @Override
    public DomainRowKey decodeFromString(String encodedRowKey) {
      List<String> split = TimelineReaderUtils.split(encodedRowKey);
      if (split == null || split.size() != 2) {
        throw new IllegalArgumentException(
            "Invalid row key for domain id.");
      }
      return new DomainRowKey(split.get(0), split.get(1));
    }
  }
}
