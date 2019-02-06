/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.util.List;

/**
 * Used for decoding FROM_ID
 */
enum TimelineFromIdConverter {

  APPLICATION_FROMID {
    @Override TimelineReaderContext decodeUID(String fromId) throws Exception {
      if (fromId == null) {
        return null;
      }

      List<String> appTupleList = TimelineReaderUtils.split(fromId);
      if (appTupleList == null || appTupleList.size() != 5) {
        throw new IllegalArgumentException(
            "Invalid row key for application table.");
      }

      return new TimelineReaderContext(appTupleList.get(0), appTupleList.get(1),
          appTupleList.get(2), Long.parseLong(appTupleList.get(3)),
          appTupleList.get(4), null, null);
    }
  },

  SUB_APPLICATION_ENTITY_FROMID {
    @Override TimelineReaderContext decodeUID(String fromId) throws Exception {
      if (fromId == null) {
        return null;
      }
      List<String> split = TimelineReaderUtils.split(fromId);
      if (split == null || split.size() != 6) {
        throw new IllegalArgumentException(
            "Invalid row key for sub app table.");
      }

      String subAppUserId = split.get(0);
      String clusterId = split.get(1);
      String entityType = split.get(2);
      Long entityIdPrefix = Long.valueOf(split.get(3));
      String entityId = split.get(4);
      String userId = split.get(5);
      return new TimelineReaderContext(clusterId, userId, null, null, null,
          entityType, entityIdPrefix, entityId, subAppUserId);
    }
  },

  GENERIC_ENTITY_FROMID {
    @Override TimelineReaderContext decodeUID(String fromId) throws Exception {
      if (fromId == null) {
        return null;
      }
      List<String> split = TimelineReaderUtils.split(fromId);
      if (split == null || split.size() != 8) {
        throw new IllegalArgumentException("Invalid row key for entity table.");
      }
      Long flowRunId = Long.valueOf(split.get(3));
      Long entityIdPrefix = Long.valueOf(split.get(6));
      return new TimelineReaderContext(split.get(0), split.get(1), split.get(2),
          flowRunId, split.get(4), split.get(5), entityIdPrefix, split.get(7));
    }
  };

  /**
   * Decodes FROM_ID depending on FROM_ID implementation.
   *
   * @param fromId FROM_ID to be decoded.
   * @return a {@link TimelineReaderContext} object if FROM_ID passed can be
   * decoded, null otherwise.
   * @throws Exception if any problem occurs while decoding.
   */
  abstract TimelineReaderContext decodeUID(String fromId) throws Exception;
}
