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
package org.apache.hadoop.yarn.server.timelineservice.storage.subapplication;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverterToString;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * Represents a rowkey for the sub app table.
 */
public class SubApplicationRowKey {
  private final String subAppUserId;
  private final String clusterId;
  private final String entityType;
  private final Long entityIdPrefix;
  private final String entityId;
  private final String userId;
  private final SubApplicationRowKeyConverter subAppRowKeyConverter =
      new SubApplicationRowKeyConverter();

  public SubApplicationRowKey(String subAppUserId, String clusterId,
      String entityType, Long entityIdPrefix, String entityId, String userId) {
    this.subAppUserId = subAppUserId;
    this.clusterId = clusterId;
    this.entityType = entityType;
    this.entityIdPrefix = entityIdPrefix;
    this.entityId = entityId;
    this.userId = userId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getSubAppUserId() {
    return subAppUserId;
  }

  public String getEntityType() {
    return entityType;
  }

  public String getEntityId() {
    return entityId;
  }

  public Long getEntityIdPrefix() {
    return entityIdPrefix;
  }

  public String getUserId() {
    return userId;
  }

  /**
   * Constructs a row key for the sub app table as follows:
   * {@code subAppUserId!clusterId!entityType
   * !entityPrefix!entityId!userId}.
   * Typically used while querying a specific sub app.
   *
   * subAppUserId is usually the doAsUser.
   * userId is the yarn user that the AM runs as.
   *
   * @return byte array with the row key.
   */
  public byte[] getRowKey() {
    return subAppRowKeyConverter.encode(this);
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   *
   * @param rowKey byte representation of row key.
   * @return An <cite>SubApplicationRowKey</cite> object.
   */
  public static SubApplicationRowKey parseRowKey(byte[] rowKey) {
    return new SubApplicationRowKeyConverter().decode(rowKey);
  }

  /**
   * Constructs a row key for the sub app table as follows:
   * <p>
   * {@code subAppUserId!clusterId!
   * entityType!entityIdPrefix!entityId!userId}.
   *
   * subAppUserId is usually the doAsUser.
   * userId is the yarn user that that the AM runs as.
   *
   * </p>
   *
   * @return String representation of row key.
   */
  public String getRowKeyAsString() {
    return subAppRowKeyConverter.encodeAsString(this);
  }

  /**
   * Given the encoded row key as string, returns the row key as an object.
   *
   * @param encodedRowKey String representation of row key.
   * @return A <cite>SubApplicationRowKey</cite> object.
   */
  public static SubApplicationRowKey parseRowKeyFromString(
      String encodedRowKey) {
    return new SubApplicationRowKeyConverter().decodeFromString(encodedRowKey);
  }

  /**
   * Encodes and decodes row key for sub app table.
   * The row key is of the form :
   * subAppUserId!clusterId!flowRunId!appId!entityType!entityId!userId
   *
   * subAppUserId is usually the doAsUser.
   * userId is the yarn user that the AM runs as.
   *
   * <p>
   */
  final private static class SubApplicationRowKeyConverter
      implements KeyConverter<SubApplicationRowKey>,
      KeyConverterToString<SubApplicationRowKey> {

    private SubApplicationRowKeyConverter() {
    }

    /**
     * sub app row key is of the form
     * subAppUserId!clusterId!entityType!entityPrefix!entityId!userId
     * w. each segment separated by !.
     *
     * subAppUserId is usually the doAsUser.
     * userId is the yarn user that the AM runs as.
     *
     * The sizes below indicate sizes of each one of these
     * segments in sequence. clusterId, subAppUserId, entityType,
     * entityId and userId are strings.
     * entity prefix is a long hence 8 bytes in size. Strings are
     * variable in size (i.e. end whenever separator is encountered).
     * This is used while decoding and helps in determining where to split.
     */
    private static final int[] SEGMENT_SIZES = {Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE};

    /*
     * (non-Javadoc)
     *
     * Encodes SubApplicationRowKey object into a byte array with each
     * component/field in SubApplicationRowKey separated by
     * Separator#QUALIFIERS.
     * This leads to an sub app table row key of the form
     * subAppUserId!clusterId!entityType!entityPrefix!entityId!userId
     *
     * subAppUserId is usually the doAsUser.
     * userId is the yarn user that the AM runs as.
     *
     * If entityType in passed SubApplicationRowKey object is null (and the
     * fields preceding it are not null i.e. clusterId, subAppUserId), this
     * returns a row key prefix of the form subAppUserId!clusterId!
     * If entityId in SubApplicationRowKey is null
     * (other components are not null), this returns a row key prefix
     * of the form subAppUserId!clusterId!entityType!
     *
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(SubApplicationRowKey rowKey) {
      byte[] subAppUser = Separator.encode(rowKey.getSubAppUserId(),
          Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
      byte[] cluster = Separator.encode(rowKey.getClusterId(), Separator.SPACE,
          Separator.TAB, Separator.QUALIFIERS);
      byte[] first = Separator.QUALIFIERS.join(subAppUser, cluster);
      if (rowKey.getEntityType() == null) {
        return first;
      }
      byte[] entityType = Separator.encode(rowKey.getEntityType(),
          Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);

      if (rowKey.getEntityIdPrefix() == null) {
        return Separator.QUALIFIERS.join(first, entityType,
            Separator.EMPTY_BYTES);
      }

      byte[] entityIdPrefix = Bytes.toBytes(rowKey.getEntityIdPrefix());

      if (rowKey.getEntityId() == null) {
        return Separator.QUALIFIERS.join(first, entityType, entityIdPrefix,
            Separator.EMPTY_BYTES);
      }

      byte[] entityId = Separator.encode(rowKey.getEntityId(), Separator.SPACE,
          Separator.TAB, Separator.QUALIFIERS);

      byte[] userId = Separator.encode(rowKey.getUserId(),
          Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);

      byte[] second = Separator.QUALIFIERS.join(entityType, entityIdPrefix,
          entityId, userId);

      return Separator.QUALIFIERS.join(first, second);
    }

    /*
     * (non-Javadoc)
     *
     * Decodes a sub application row key of the form
     * subAppUserId!clusterId!entityType!entityPrefix!entityId!userId
     *
     * subAppUserId is usually the doAsUser.
     * userId is the yarn user that the AM runs as.
     *
     * represented in byte format
     * and converts it into an SubApplicationRowKey object.
     *
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#decode(byte[])
     */
    @Override
    public SubApplicationRowKey decode(byte[] rowKey) {
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      if (rowKeyComponents.length != 6) {
        throw new IllegalArgumentException(
            "the row key is not valid for " + "a sub app");
      }
      String subAppUserId =
          Separator.decode(Bytes.toString(rowKeyComponents[0]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      String clusterId = Separator.decode(Bytes.toString(rowKeyComponents[1]),
          Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      String entityType = Separator.decode(Bytes.toString(rowKeyComponents[2]),
          Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);

      Long entityPrefixId = Bytes.toLong(rowKeyComponents[3]);

      String entityId = Separator.decode(Bytes.toString(rowKeyComponents[4]),
          Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      String userId =
          Separator.decode(Bytes.toString(rowKeyComponents[5]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);

      return new SubApplicationRowKey(subAppUserId, clusterId, entityType,
          entityPrefixId, entityId, userId);
    }

    @Override
    public String encodeAsString(SubApplicationRowKey key) {
      if (key.subAppUserId == null || key.clusterId == null
          || key.entityType == null || key.entityIdPrefix == null
          || key.entityId == null || key.userId == null) {
        throw new IllegalArgumentException();
      }
      return TimelineReaderUtils.joinAndEscapeStrings(
          new String[] {key.subAppUserId, key.clusterId, key.entityType,
              key.entityIdPrefix.toString(), key.entityId, key.userId});
    }

    @Override
    public SubApplicationRowKey decodeFromString(String encodedRowKey) {
      List<String> split = TimelineReaderUtils.split(encodedRowKey);
      if (split == null || split.size() != 6) {
        throw new IllegalArgumentException(
            "Invalid row key for sub app table.");
      }
      Long entityIdPrefix = Long.valueOf(split.get(3));
      return new SubApplicationRowKey(split.get(0), split.get(1),
          split.get(2), entityIdPrefix, split.get(4), split.get(5));
    }
  }
}
