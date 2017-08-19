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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.util.List;

/**
 * Used for encoding/decoding UID which will be used for query by UI.
 */
enum TimelineUIDConverter {
  // Flow UID should contain cluster, user and flow name.
  FLOW_UID {
    @Override
    String encodeUID(TimelineReaderContext context) {
      if (context == null) {
        return null;
      }
      if (context.getClusterId() == null || context.getUserId() == null ||
          context.getFlowName() == null) {
        return null;
      }
      String[] flowNameTupleArr = {context.getClusterId(), context.getUserId(),
          context.getFlowName()};
      return joinAndEscapeUIDParts(flowNameTupleArr);
    }

    @Override
    TimelineReaderContext decodeUID(String uId) throws Exception {
      if (uId == null) {
        return null;
      }
      List<String> flowNameTupleList = splitUID(uId);
      // Should have 3 parts i.e. cluster, user and flow name.
      if (flowNameTupleList.size() != 3) {
        return null;
      }
      return new TimelineReaderContext(flowNameTupleList.get(0),
          flowNameTupleList.get(1), flowNameTupleList.get(2), null,
          null, null, null);
    }
  },

  // Flowrun UID should contain cluster, user, flow name and flowrun id.
  FLOWRUN_UID{
    @Override
    String encodeUID(TimelineReaderContext context) {
      if (context == null) {
        return null;
      }
      if (context.getClusterId() == null || context.getUserId() == null ||
          context.getFlowName() == null || context.getFlowRunId() == null) {
        return null;
      }
      String[] flowRunTupleArr = {context.getClusterId(), context.getUserId(),
          context.getFlowName(), context.getFlowRunId().toString()};
      return joinAndEscapeUIDParts(flowRunTupleArr);
    }

    @Override
    TimelineReaderContext decodeUID(String uId) throws Exception {
      if (uId == null) {
        return null;
      }
      List<String> flowRunTupleList = splitUID(uId);
      // Should have 4 parts i.e. cluster, user, flow name and flowrun id.
      if (flowRunTupleList.size() != 4) {
        return null;
      }
      return new TimelineReaderContext(flowRunTupleList.get(0),
          flowRunTupleList.get(1), flowRunTupleList.get(2),
          Long.parseLong(flowRunTupleList.get(3)), null, null, null);
    }
  },

  // Application UID should contain cluster, user, flow name, flowrun id
  // and app id OR cluster and app id(i.e.without flow context info).
  APPLICATION_UID{
    @Override
    String encodeUID(TimelineReaderContext context) {
      if (context == null) {
        return null;
      }
      if (context.getClusterId() == null || context.getAppId() == null) {
        return null;
      }
      if (context.getUserId() != null && context.getFlowName() != null &&
          context.getFlowRunId() != null) {
        // Flow information exists.
        String[] appTupleArr = {context.getClusterId(), context.getUserId(),
            context.getFlowName(), context.getFlowRunId().toString(),
            context.getAppId()};
        return joinAndEscapeUIDParts(appTupleArr);
      } else {
        // Only cluster and app information exists. Flow info does not exist.
        String[] appTupleArr = {context.getClusterId(), context.getAppId()};
        return joinAndEscapeUIDParts(appTupleArr);
      }
    }

    @Override
    TimelineReaderContext decodeUID(String uId) throws Exception {
      if (uId == null) {
        return null;
      }
      List<String> appTupleList = splitUID(uId);
      // Should have 5 parts i.e. cluster, user, flow name, flowrun id
      // and app id OR should have 2 parts i.e. cluster and app id.
      if (appTupleList.size() == 5) {
        // Flow information exists.
        return new TimelineReaderContext(appTupleList.get(0),
            appTupleList.get(1), appTupleList.get(2),
            Long.parseLong(appTupleList.get(3)), appTupleList.get(4),
            null, null);
      } else if (appTupleList.size() == 2) {
        // Flow information does not exist.
        return new TimelineReaderContext(appTupleList.get(0), null, null, null,
            appTupleList.get(1), null, null);
      } else {
        return null;
      }
    }
  },

  // Sub Application Entity UID should contain cluster, user, entity type and
  // entity id
  SUB_APPLICATION_ENTITY_UID {
    @Override
    String encodeUID(TimelineReaderContext context) {
      if (context == null) {
        return null;
      }
      if (context.getClusterId() == null || context.getDoAsUser() == null
          || context.getEntityType() == null || context.getEntityId() == null) {
        return null;
      }
      String[] entityTupleArr = {context.getClusterId(), context.getDoAsUser(),
          context.getEntityType(), context.getEntityIdPrefix().toString(),
          context.getEntityId()};
      return joinAndEscapeUIDParts(entityTupleArr);
    }

    @Override
    TimelineReaderContext decodeUID(String uId) throws Exception {
      if (uId == null) {
        return null;
      }
      List<String> entityTupleList = splitUID(uId);
      if (entityTupleList.size() == 5) {
        // Flow information exists.
        return new TimelineReaderContext(entityTupleList.get(0), null, null,
            null, null, entityTupleList.get(2),
            Long.parseLong(entityTupleList.get(3)), entityTupleList.get(4),
            entityTupleList.get(1));
      }
      return null;
    }
  },

  // Generic Entity UID should contain cluster, user, flow name, flowrun id,
  // app id, entity type and entity id OR should contain cluster, appid, entity
  // type and entity id(i.e.without flow context info).
  GENERIC_ENTITY_UID {
    @Override
    String encodeUID(TimelineReaderContext context) {
      if (context == null) {
        return null;
      }
      if (context.getClusterId() == null || context.getAppId() == null ||
          context.getEntityType() == null || context.getEntityId() == null) {
        return null;
      }
      if (context.getUserId() != null && context.getFlowName() != null &&
          context.getFlowRunId() != null) {
        // Flow information exists.
        String[] entityTupleArr = {context.getClusterId(), context.getUserId(),
            context.getFlowName(), context.getFlowRunId().toString(),
            context.getAppId(), context.getEntityType(),
            context.getEntityIdPrefix().toString(), context.getEntityId() };
        return joinAndEscapeUIDParts(entityTupleArr);
      } else {
        // Only entity and app information exists. Flow info does not exist.
        String[] entityTupleArr = {context.getClusterId(), context.getAppId(),
            context.getEntityType(), context.getEntityIdPrefix().toString(),
            context.getEntityId() };
        return joinAndEscapeUIDParts(entityTupleArr);
      }
    }

    @Override
    TimelineReaderContext decodeUID(String uId) throws Exception {
      if (uId == null) {
        return null;
      }
      List<String> entityTupleList = splitUID(uId);
      // Should have 8 parts i.e. cluster, user, flow name, flowrun id, app id,
      // entity type and entity id OR should have 5 parts i.e. cluster, app id,
      // entity type and entity id.
      if (entityTupleList.size() == 8) {
        // Flow information exists.
        return new TimelineReaderContext(entityTupleList.get(0),
            entityTupleList.get(1), entityTupleList.get(2),
            Long.parseLong(entityTupleList.get(3)), entityTupleList.get(4),
            entityTupleList.get(5), Long.parseLong(entityTupleList.get(6)),
            entityTupleList.get(7));
      } else if (entityTupleList.size() == 5) {
        // Flow information does not exist.
        return new TimelineReaderContext(entityTupleList.get(0), null, null,
            null, entityTupleList.get(1), entityTupleList.get(2),
            Long.parseLong(entityTupleList.get(3)), entityTupleList.get(4));
      } else {
        return null;
      }
    }
  };

  /**
   * Split UID using {@link TimelineReaderUtils#DEFAULT_DELIMITER_CHAR} and
   * {@link TimelineReaderUtils#DEFAULT_ESCAPE_CHAR}.
   * @param uid UID to be splitted.
   * @return a list of different parts of UID split across delimiter.
   * @throws IllegalArgumentException if UID is not properly escaped.
   */
  private static List<String> splitUID(String uid)
      throws IllegalArgumentException {
    return TimelineReaderUtils.split(uid);
  }

  /**
   * Join different parts of UID delimited by
   * {@link TimelineReaderUtils#DEFAULT_DELIMITER_CHAR} with delimiter and
   * escape character escaped using
   * {@link TimelineReaderUtils#DEFAULT_ESCAPE_CHAR} if UID parts contain them.
   * @param parts an array of UID parts to be joined.
   * @return a string joined using the delimiter with escape and delimiter
   *         characters escaped if they are part of the string parts to be
   *         joined. Returns null if one of the parts is null.
   */
  private static String joinAndEscapeUIDParts(String[] parts) {
    return TimelineReaderUtils.joinAndEscapeStrings(parts);
  }

  /**
   * Encodes UID depending on UID implementation.
   *
   * @param context Reader context.
   * @return UID represented as a string.
   */
  abstract String encodeUID(TimelineReaderContext context);

  /**
   * Decodes UID depending on UID implementation.
   *
   * @param uId UID to be decoded.
   * @return a {@link TimelineReaderContext} object if UID passed can be
   * decoded, null otherwise.
   * @throws Exception if any problem occurs while decoding.
   */
  abstract TimelineReaderContext decodeUID(String uId) throws Exception;
}
