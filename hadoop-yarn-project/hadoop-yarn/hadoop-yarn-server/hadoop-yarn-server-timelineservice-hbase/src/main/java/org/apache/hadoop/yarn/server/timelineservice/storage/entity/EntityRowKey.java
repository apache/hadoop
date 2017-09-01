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
package org.apache.hadoop.yarn.server.timelineservice.storage.entity;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.AppIdKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverterToString;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * Represents a rowkey for the entity table.
 */
public class EntityRowKey {
  private final String clusterId;
  private final String userId;
  private final String flowName;
  private final Long flowRunId;
  private final String appId;
  private final String entityType;
  private final Long entityIdPrefix;
  private final String entityId;
  private final EntityRowKeyConverter entityRowKeyConverter =
      new EntityRowKeyConverter();

  public EntityRowKey(String clusterId, String userId, String flowName,
      Long flowRunId, String appId, String entityType, Long entityIdPrefix,
      String entityId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = flowName;
    this.flowRunId = flowRunId;
    this.appId = appId;
    this.entityType = entityType;
    this.entityIdPrefix = entityIdPrefix;
    this.entityId = entityId;
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

  public String getEntityType() {
    return entityType;
  }

  public String getEntityId() {
    return entityId;
  }

  public Long getEntityIdPrefix() {
    return entityIdPrefix;
  }

  /**
   * Constructs a row key for the entity table as follows:
   * {@code userName!clusterId!flowName!flowRunId!AppId!entityType!entityId}.
   * Typically used while querying a specific entity.
   *
   * @return byte array with the row key.
   */
  public byte[] getRowKey() {
    return entityRowKeyConverter.encode(this);
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   * @param rowKey byte representation of row key.
   * @return An <cite>EntityRowKey</cite> object.
   */
  public static EntityRowKey parseRowKey(byte[] rowKey) {
    return new EntityRowKeyConverter().decode(rowKey);
  }

  /**
   * Constructs a row key for the entity table as follows:
   * <p>
   * {@code userName!clusterId!flowName!flowRunId!AppId!
   * entityType!entityIdPrefix!entityId}.
   * </p>
   * @return String representation of row key.
   */
  public String getRowKeyAsString() {
    return entityRowKeyConverter.encodeAsString(this);
  }

  /**
   * Given the encoded row key as string, returns the row key as an object.
   * @param encodedRowKey String representation of row key.
   * @return A <cite>EntityRowKey</cite> object.
   */
  public static EntityRowKey parseRowKeyFromString(String encodedRowKey) {
    return new EntityRowKeyConverter().decodeFromString(encodedRowKey);
  }

  /**
   * Encodes and decodes row key for entity table. The row key is of the form :
   * userName!clusterId!flowName!flowRunId!appId!entityType!entityId. flowRunId
   * is a long, appId is encoded/decoded using {@link AppIdKeyConverter} and
   * rest are strings.
   * <p>
   */
  final private static class EntityRowKeyConverter implements
      KeyConverter<EntityRowKey>, KeyConverterToString<EntityRowKey> {

    private final AppIdKeyConverter appIDKeyConverter = new AppIdKeyConverter();

    private EntityRowKeyConverter() {
    }

    /**
     * Entity row key is of the form
     * userName!clusterId!flowName!flowRunId!appId!entityType!entityId w. each
     * segment separated by !. The sizes below indicate sizes of each one of
     * these segments in sequence. clusterId, userName, flowName, entityType and
     * entityId are strings. flowrunId is a long hence 8 bytes in size. app id
     * is represented as 12 bytes with cluster timestamp part of appid being 8
     * bytes (long) and seq id being 4 bytes(int). Strings are variable in size
     * (i.e. end whenever separator is encountered). This is used while decoding
     * and helps in determining where to split.
     */
    private static final int[] SEGMENT_SIZES = {Separator.VARIABLE_SIZE,
        Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
        AppIdKeyConverter.getKeySize(), Separator.VARIABLE_SIZE,
        Bytes.SIZEOF_LONG, Separator.VARIABLE_SIZE };

    /*
     * (non-Javadoc)
     *
     * Encodes EntityRowKey object into a byte array with each component/field
     * in EntityRowKey separated by Separator#QUALIFIERS. This leads to an
     * entity table row key of the form
     * userName!clusterId!flowName!flowRunId!appId!entityType!entityId If
     * entityType in passed EntityRowKey object is null (and the fields
     * preceding it i.e. clusterId, userId and flowName, flowRunId and appId
     * are not null), this returns a row key prefix of the form
     * userName!clusterId!flowName!flowRunId!appId! and if entityId in
     * EntityRowKey is null (other 6 components are not null), this returns a
     * row key prefix of the form
     * userName!clusterId!flowName!flowRunId!appId!entityType! flowRunId is
     * inverted while encoding as it helps maintain a descending order for row
     * keys in entity table.
     *
     * @see org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#encode(java.lang.Object)
     */
    @Override
    public byte[] encode(EntityRowKey rowKey) {
      byte[] user =
          Separator.encode(rowKey.getUserId(), Separator.SPACE, Separator.TAB,
              Separator.QUALIFIERS);
      byte[] cluster =
          Separator.encode(rowKey.getClusterId(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);
      byte[] flow =
          Separator.encode(rowKey.getFlowName(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);
      byte[] first = Separator.QUALIFIERS.join(user, cluster, flow);
      // Note that flowRunId is a long, so we can't encode them all at the same
      // time.
      byte[] second =
          Bytes.toBytes(LongConverter.invertLong(rowKey.getFlowRunId()));
      byte[] third = appIDKeyConverter.encode(rowKey.getAppId());
      if (rowKey.getEntityType() == null) {
        return Separator.QUALIFIERS.join(first, second, third,
            Separator.EMPTY_BYTES);
      }
      byte[] entityType =
          Separator.encode(rowKey.getEntityType(), Separator.SPACE,
              Separator.TAB, Separator.QUALIFIERS);

      if (rowKey.getEntityIdPrefix() == null) {
        return Separator.QUALIFIERS.join(first, second, third, entityType,
            Separator.EMPTY_BYTES);
      }

      byte[] entityIdPrefix = Bytes.toBytes(rowKey.getEntityIdPrefix());

      if (rowKey.getEntityId() == null) {
        return Separator.QUALIFIERS.join(first, second, third, entityType,
            entityIdPrefix, Separator.EMPTY_BYTES);
      }

      byte[] entityId = Separator.encode(rowKey.getEntityId(), Separator.SPACE,
          Separator.TAB, Separator.QUALIFIERS);

      byte[] fourth =
          Separator.QUALIFIERS.join(entityType, entityIdPrefix, entityId);

      return Separator.QUALIFIERS.join(first, second, third, fourth);
    }

    /*
     * (non-Javadoc)
     *
     * Decodes an application row key of the form
     * userName!clusterId!flowName!flowRunId!appId!entityType!entityId
     * represented in byte format and converts it into an EntityRowKey object.
     * flowRunId is inverted while decoding as it was inverted while encoding.
     *
     * @see
     * org.apache.hadoop.yarn.server.timelineservice.storage.common
     * .KeyConverter#decode(byte[])
     */
    @Override
    public EntityRowKey decode(byte[] rowKey) {
      byte[][] rowKeyComponents =
          Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
      if (rowKeyComponents.length != 8) {
        throw new IllegalArgumentException("the row key is not valid for "
            + "an entity");
      }
      String userId =
          Separator.decode(Bytes.toString(rowKeyComponents[0]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      String clusterId =
          Separator.decode(Bytes.toString(rowKeyComponents[1]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      String flowName =
          Separator.decode(Bytes.toString(rowKeyComponents[2]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      Long flowRunId =
          LongConverter.invertLong(Bytes.toLong(rowKeyComponents[3]));
      String appId = appIDKeyConverter.decode(rowKeyComponents[4]);
      String entityType =
          Separator.decode(Bytes.toString(rowKeyComponents[5]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);

      Long entityPrefixId = Bytes.toLong(rowKeyComponents[6]);

      String entityId =
          Separator.decode(Bytes.toString(rowKeyComponents[7]),
              Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
      return new EntityRowKey(clusterId, userId, flowName, flowRunId, appId,
          entityType, entityPrefixId, entityId);
    }

    @Override
    public String encodeAsString(EntityRowKey key) {
      if (key.clusterId == null || key.userId == null || key.flowName == null
          || key.flowRunId == null || key.appId == null
          || key.entityType == null || key.entityIdPrefix == null
          || key.entityId == null) {
        throw new IllegalArgumentException();
      }
      return TimelineReaderUtils
          .joinAndEscapeStrings(new String[] {key.clusterId, key.userId,
              key.flowName, key.flowRunId.toString(), key.appId, key.entityType,
              key.entityIdPrefix.toString(), key.entityId});
    }

    @Override
    public EntityRowKey decodeFromString(String encodedRowKey) {
      List<String> split = TimelineReaderUtils.split(encodedRowKey);
      if (split == null || split.size() != 8) {
        throw new IllegalArgumentException("Invalid row key for entity table.");
      }
      Long flowRunId = Long.valueOf(split.get(3));
      Long entityIdPrefix = Long.valueOf(split.get(6));
      return new EntityRowKey(split.get(0), split.get(1), split.get(2),
          flowRunId, split.get(4), split.get(5), entityIdPrefix, split.get(7));
    }
  }
}
