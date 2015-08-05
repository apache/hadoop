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
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineWriterUtils;

/**
 * Represents a rowkey for the entity table.
 */
public class EntityRowKey {
  // TODO: more methods are needed for this class.

  // TODO: API needs to be cleaned up.

  /**
   * Constructs a row key prefix for the entity table as follows:
   * {@code userName!clusterId!flowId!flowRunId!AppId}
   *
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @param appId
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKeyPrefix(String clusterId, String userId,
      String flowId, Long flowRunId, String appId) {
    byte[] first =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(userId, clusterId,
            flowId));
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(TimelineWriterUtils.invert(flowRunId));
    byte[] third = Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(appId));
    return Separator.QUALIFIERS.join(first, second, third);
  }

  /**
   * Constructs a row key prefix for the entity table as follows:
   * {@code userName!clusterId!flowId!flowRunId!AppId}
   *
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @param appId
   * @return byte array with the row key prefix
   */
  public static byte[] getRowKey(String clusterId, String userId,
      String flowId, Long flowRunId, String appId, TimelineEntity te) {
    byte[] first =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(userId, clusterId,
            flowId));
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(TimelineWriterUtils.invert(flowRunId));
    byte[] third =
        Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(appId, te.getType(),
            te.getId()));
    return Separator.QUALIFIERS.join(first, second, third);
  }

}
