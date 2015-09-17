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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineWriterUtils;

/**
 * Represents a rowkey for the flow run table.
 */
public class FlowRunRowKey {
  // TODO: more methods are needed for this class like parse row key

  /**
   * Constructs a row key for the entity table as follows: {
   * clusterId!userI!flowId!Inverted Flow Run Id}
   *
   * @param clusterId
   * @param userId
   * @param flowId
   * @param flowRunId
   * @return byte array with the row key
   */
  public static byte[] getRowKey(String clusterId, String userId,
      String flowId, Long flowRunId) {
    byte[] first = Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(clusterId,
        userId, flowId));
    // Note that flowRunId is a long, so we can't encode them all at the same
    // time.
    byte[] second = Bytes.toBytes(TimelineWriterUtils.invert(flowRunId));
    return Separator.QUALIFIERS.join(first, second);
  }

}
