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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.AppIdKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;

/**
 * Encodes and decodes row key for entity table.
 * The row key is of the form :
 * userName!clusterId!flowName!flowRunId!appId!entityType!entityId.
 * flowRunId is a long, appId is encoded/decoded using
 * {@link AppIdKeyConverter} and rest are strings.
 */
public final class EntityRowKeyConverter implements KeyConverter<EntityRowKey> {
  private static final EntityRowKeyConverter INSTANCE =
      new EntityRowKeyConverter();

  public static EntityRowKeyConverter getInstance() {
    return INSTANCE;
  }

  private EntityRowKeyConverter() {
  }

  // Entity row key is of the form
  // userName!clusterId!flowName!flowRunId!appId!entityType!entityId with each
  // segment separated by !. The sizes below indicate sizes of each one of these
  // segements in sequence. clusterId, userName, flowName, entityType and
  // entityId are strings. flowrunId is a long hence 8 bytes in size. app id is
  // represented as 12 bytes with cluster timestamp part of appid being 8 bytes
  // (long) and seq id being 4 bytes(int).
  // Strings are variable in size (i.e. end whenever separator is encountered).
  // This is used while decoding and helps in determining where to split.
  private static final int[] SEGMENT_SIZES = {
      Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
      Bytes.SIZEOF_LONG, AppIdKeyConverter.getKeySize(),
      Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE };

  /*
   * (non-Javadoc)
   *
   * Encodes EntityRowKey object into a byte array with each component/field in
   * EntityRowKey separated by Separator#QUALIFIERS. This leads to an entity
   * table row key of the form
   * userName!clusterId!flowName!flowRunId!appId!entityType!entityId
   * If entityType in passed EntityRowKey object is null (and the fields
   * preceding it i.e. clusterId, userId and flowName, flowRunId and appId are
   * not null), this returns a row key prefix of the form
   * userName!clusterId!flowName!flowRunId!appId! and if entityId in
   * EntityRowKey is null (other 6 components are not null), this returns a row
   * key prefix of the form
   * userName!clusterId!flowName!flowRunId!appId!entityType!
   * flowRunId is inverted while encoding as it helps maintain a descending
   * order for row keys in entity table.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #encode(java.lang.Object)
   */
  @Override
  public byte[] encode(EntityRowKey rowKey) {
    byte[] user = Separator.encode(rowKey.getUserId(),
        Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
    byte[] cluster = Separator.encode(rowKey.getClusterId(),
        Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
    byte[] flow = Separator.encode(rowKey.getFlowName(),
        Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
    byte[] first = Separator.QUALIFIERS.join(user, cluster, flow);
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(TimelineStorageUtils.invertLong(
        rowKey.getFlowRunId()));
    byte[] third = AppIdKeyConverter.getInstance().encode(rowKey.getAppId());
    if (rowKey.getEntityType() == null) {
      return Separator.QUALIFIERS.join(
          first, second, third, Separator.EMPTY_BYTES);
    }
    byte[] entityType = Separator.encode(rowKey.getEntityType(),
        Separator.SPACE, Separator.TAB, Separator.QUALIFIERS);
    byte[] entityId = rowKey.getEntityId() == null ? Separator.EMPTY_BYTES :
        Separator.encode(rowKey.getEntityId(), Separator.SPACE, Separator.TAB,
        Separator.QUALIFIERS);
    byte[] fourth = Separator.QUALIFIERS.join(entityType, entityId);
    return Separator.QUALIFIERS.join(first, second, third, fourth);
  }

  /*
   * (non-Javadoc)
   *
   * Decodes an application row key of the form
   * userName!clusterId!flowName!flowRunId!appId!entityType!entityId represented
   * in byte format and converts it into an EntityRowKey object. flowRunId is
   * inverted while decoding as it was inverted while encoding.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #decode(byte[])
   */
  @Override
  public EntityRowKey decode(byte[] rowKey) {
    byte[][] rowKeyComponents =
        Separator.QUALIFIERS.split(rowKey, SEGMENT_SIZES);
    if (rowKeyComponents.length != 7) {
      throw new IllegalArgumentException("the row key is not valid for " +
          "an entity");
    }
    String userId = Separator.decode(Bytes.toString(rowKeyComponents[0]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    String clusterId = Separator.decode(Bytes.toString(rowKeyComponents[1]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    String flowName = Separator.decode(Bytes.toString(rowKeyComponents[2]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    Long flowRunId =
        TimelineStorageUtils.invertLong(Bytes.toLong(rowKeyComponents[3]));
    String appId = AppIdKeyConverter.getInstance().decode(rowKeyComponents[4]);
    String entityType = Separator.decode(Bytes.toString(rowKeyComponents[5]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    String entityId =Separator.decode(Bytes.toString(rowKeyComponents[6]),
        Separator.QUALIFIERS, Separator.TAB, Separator.SPACE);
    return new EntityRowKey(clusterId, userId, flowName, flowRunId, appId,
        entityType, entityId);
  }
}
