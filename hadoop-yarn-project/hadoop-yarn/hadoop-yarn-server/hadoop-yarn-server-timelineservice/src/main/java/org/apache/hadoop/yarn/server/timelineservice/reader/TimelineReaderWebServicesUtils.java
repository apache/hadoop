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

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;

/**
 * Set of utility methods to be used by timeline reader web services.
 */
final class TimelineReaderWebServicesUtils {
  private static final String COMMA_DELIMITER = ",";
  private static final String COLON_DELIMITER = ":";

  private TimelineReaderWebServicesUtils() {
  }

  /**
   * Parse the passed context information represented as strings and convert
   * into a {@link TimelineReaderContext} object.
   * @param clusterId Cluster Id.
   * @param userId User Id.
   * @param flowName Flow Name.
   * @param flowRunId Run id for the flow.
   * @param appId App Id.
   * @param entityType Entity Type.
   * @param entityId Entity Id.
   * @return a {@link TimelineReaderContext} object.
   * @throws Exception if any problem occurs during parsing.
   */
  static TimelineReaderContext createTimelineReaderContext(String clusterId,
      String userId, String flowName, String flowRunId, String appId,
      String entityType, String entityId) throws Exception {
    return new TimelineReaderContext(parseStr(clusterId), parseStr(userId),
        parseStr(flowName), parseLongStr(flowRunId), parseStr(appId),
        parseStr(entityType), parseStr(entityId));
  }

  /**
   * Parse the passed filters represented as strings and convert them into a
   * {@link TimelineEntityFilters} object.
   * @param limit Limit to number of entities to return.
   * @param createdTimeStart Created time start for the entities to return.
   * @param createdTimeEnd Created time end for the entities to return.
   * @param relatesTo Entities to return must match relatesTo.
   * @param isRelatedTo Entities to return must match isRelatedTo.
   * @param infofilters Entities to return must match these info filters.
   * @param conffilters Entities to return must match these metric filters.
   * @param metricfilters Entities to return must match these metric filters.
   * @param eventfilters Entities to return must match these event filters.
   * @return a {@link TimelineEntityFilters} object.
   * @throws Exception if any problem occurs during parsing.
   */
  static TimelineEntityFilters createTimelineEntityFilters(String limit,
      String createdTimeStart, String createdTimeEnd, String relatesTo,
      String isRelatedTo, String infofilters, String conffilters,
      String metricfilters, String eventfilters) throws Exception {
    return new TimelineEntityFilters(parseLongStr(limit),
        parseLongStr(createdTimeStart), parseLongStr(createdTimeEnd),
        parseKeyStrValuesStr(relatesTo, COMMA_DELIMITER, COLON_DELIMITER),
        parseKeyStrValuesStr(isRelatedTo, COMMA_DELIMITER, COLON_DELIMITER),
        parseKeyStrValueObj(infofilters, COMMA_DELIMITER, COLON_DELIMITER),
        parseKeyStrValueStr(conffilters, COMMA_DELIMITER, COLON_DELIMITER),
        parseValuesStr(metricfilters, COMMA_DELIMITER),
        parseValuesStr(eventfilters, COMMA_DELIMITER));
  }

  /**
   * Parse the passed fields represented as strings and convert them into a
   * {@link TimelineDataToRetrieve} object.
   * @param confs confs to retrieve.
   * @param metrics metrics to retrieve.
   * @param fields fields to retrieve.
   * @return a {@link TimelineDataToRetrieve} object.
   * @throws Exception if any problem occurs during parsing.
   */
  static TimelineDataToRetrieve createTimelineDataToRetrieve(String confs,
      String metrics, String fields) throws Exception {
    return new TimelineDataToRetrieve(
        null, null, parseFieldsStr(fields, COMMA_DELIMITER));
  }

  /**
   * Parse a delimited string and convert it into a set of strings. For
   * instance, if delimiter is ",", then the string should be represented as
   * "value1,value2,value3".
   * @param str delimited string.
   * @param delimiter string is delimited by this delimiter.
   * @return set of strings.
   */
  static Set<String> parseValuesStr(String str, String delimiter) {
    if (str == null || str.isEmpty()) {
      return null;
    }
    Set<String> strSet = new HashSet<String>();
    String[] strs = str.split(delimiter);
    for (String aStr : strs) {
      strSet.add(aStr.trim());
    }
    return strSet;
  }

  @SuppressWarnings("unchecked")
  private static <T> void parseKeyValues(Map<String, T> map, String str,
      String pairsDelim, String keyValuesDelim, boolean stringValue,
      boolean multipleValues) {
    String[] pairs = str.split(pairsDelim);
    for (String pair : pairs) {
      if (pair == null || pair.trim().isEmpty()) {
        continue;
      }
      String[] pairStrs = pair.split(keyValuesDelim);
      if (pairStrs.length < 2) {
        continue;
      }
      if (!stringValue) {
        try {
          Object value =
              GenericObjectMapper.OBJECT_READER.readValue(pairStrs[1].trim());
          map.put(pairStrs[0].trim(), (T) value);
        } catch (IOException e) {
          map.put(pairStrs[0].trim(), (T) pairStrs[1].trim());
        }
      } else {
        String key = pairStrs[0].trim();
        if (multipleValues) {
          Set<String> values = new HashSet<String>();
          for (int i = 1; i < pairStrs.length; i++) {
            values.add(pairStrs[i].trim());
          }
          map.put(key, (T) values);
        } else {
          map.put(key, (T) pairStrs[1].trim());
        }
      }
    }
  }

  /**
   * Parse a delimited string and convert it into a map of key-values with each
   * key having a set of values. Both the key and values are interpreted as
   * strings.
   * For instance, if pairsDelim is "," and keyValuesDelim is ":", then the
   * string should be represented as
   * "key1:value11:value12:value13,key2:value21,key3:value31:value32".
   * @param str delimited string represented as multiple keys having multiple
   *     values.
   * @param pairsDelim key-values pairs are delimited by this delimiter.
   * @param keyValuesDelim values for a key are delimited by this delimiter.
   * @return a map of key-values with each key having a set of values.
   */
  static Map<String, Set<String>> parseKeyStrValuesStr(String str,
      String pairsDelim, String keyValuesDelim) {
    if (str == null) {
      return null;
    }
    Map<String, Set<String>> map = new HashMap<String, Set<String>>();
    parseKeyValues(map, str, pairsDelim, keyValuesDelim, true, true);
    return map;
  }

  /**
   * Parse a delimited string and convert it into a map of key-value pairs with
   * both the key and value interpreted as strings.
   * For instance, if pairsDelim is "," and keyValDelim is ":", then the string
   * should be represented as "key1:value1,key2:value2,key3:value3".
   * @param str delimited string represented as key-value pairs.
   * @param pairsDelim key-value pairs are delimited by this delimiter.
   * @param keyValDelim key and value are delimited by this delimiter.
   * @return a map of key-value pairs with both key and value being strings.
   */
  static Map<String, String> parseKeyStrValueStr(String str,
      String pairsDelim, String keyValDelim) {
    if (str == null) {
      return null;
    }
    Map<String, String> map = new HashMap<String, String>();
    parseKeyValues(map, str, pairsDelim, keyValDelim, true, false);
    return map;
  }

  /**
   * Parse a delimited string and convert it into a map of key-value pairs with
   * key being a string and value interpreted as any object.
   * For instance, if pairsDelim is "," and keyValDelim is ":", then the string
   * should be represented as "key1:value1,key2:value2,key3:value3".
   * @param str delimited string represented as key-value pairs.
   * @param pairsDelim key-value pairs are delimited by this delimiter.
   * @param keyValDelim key and value are delimited by this delimiter.
   * @return a map of key-value pairs with key being a string and value, any
   *     object.
   */
  static Map<String, Object> parseKeyStrValueObj(String str,
      String pairsDelim, String keyValDelim) {
    if (str == null) {
      return null;
    }
    Map<String, Object> map = new HashMap<String, Object>();
    parseKeyValues(map, str, pairsDelim, keyValDelim, false, false);
    return map;
  }

  /**
   * Interprets passed string as set of fields delimited by passed delimiter.
   * For instance, if delimiter is ",", then the passed string should be
   * represented as "METRICS,CONFIGS" where the delimited parts of the string
   * present in {@link Field}.
   * @param str passed string.
   * @param delimiter string delimiter.
   * @return a set of {@link Field}.
   */
  static EnumSet<Field> parseFieldsStr(String str, String delimiter) {
    if (str == null) {
      return null;
    }
    String[] strs = str.split(delimiter);
    EnumSet<Field> fieldList = EnumSet.noneOf(Field.class);
    for (String s : strs) {
      fieldList.add(Field.valueOf(s.trim().toUpperCase()));
    }
    return fieldList;
  }

  /**
   * Interpret passed string as a long.
   * @param str Passed string.
   * @return long representation if string is not null, null otherwise.
   */
  static Long parseLongStr(String str) {
    return str == null ? null : Long.parseLong(str.trim());
  }

  /**
   * Trims the passed string if its not null.
   * @param str Passed string.
   * @return trimmed string if string is not null, null otherwise.
   */
  static String parseStr(String str) {
    return str == null ? null : str.trim();
  }

  /**
   * Get UGI from HTTP request.
   * @param req HTTP request.
   * @return UGI.
   */
  static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUGI;
  }

  /**
   * Get username from caller UGI.
   * @param callerUGI caller UGI.
   * @return username.
   */
  static String getUserName(UserGroupInformation callerUGI) {
    return ((callerUGI != null) ? callerUGI.getUserName().trim() : "");
  }
}