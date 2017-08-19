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

import java.security.Principal;
import java.util.EnumSet;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;

/**
 * Set of utility methods to be used by timeline reader web services.
 */
public final class TimelineReaderWebServicesUtils {

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
   */
  static TimelineReaderContext createTimelineReaderContext(String clusterId,
      String userId, String flowName, String flowRunId, String appId,
      String entityType, String entityIdPrefix, String entityId) {
    return new TimelineReaderContext(parseStr(clusterId), parseStr(userId),
        parseStr(flowName), parseLongStr(flowRunId), parseStr(appId),
        parseStr(entityType), parseLongStr(entityIdPrefix), parseStr(entityId));
  }

  static TimelineReaderContext createTimelineReaderContext(String clusterId,
      String userId, String flowName, String flowRunId, String appId,
      String entityType, String entityIdPrefix, String entityId,
      String doAsUser) {
    return new TimelineReaderContext(parseStr(clusterId), parseStr(userId),
        parseStr(flowName), parseLongStr(flowRunId), parseStr(appId),
        parseStr(entityType), parseLongStr(entityIdPrefix), parseStr(entityId),
        parseStr(doAsUser));
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
   * @throws TimelineParseException if any problem occurs during parsing.
   */
  static TimelineEntityFilters createTimelineEntityFilters(String limit,
      String createdTimeStart, String createdTimeEnd, String relatesTo,
      String isRelatedTo, String infofilters, String conffilters,
      String metricfilters, String eventfilters,
      String fromid) throws TimelineParseException {
    return createTimelineEntityFilters(
        limit, parseLongStr(createdTimeStart),
        parseLongStr(createdTimeEnd),
        relatesTo, isRelatedTo, infofilters,
        conffilters, metricfilters, eventfilters, fromid);
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
   * @throws TimelineParseException if any problem occurs during parsing.
   */
  static TimelineEntityFilters createTimelineEntityFilters(String limit,
      Long createdTimeStart, Long createdTimeEnd, String relatesTo,
      String isRelatedTo, String infofilters, String conffilters,
      String metricfilters, String eventfilters,
      String fromid) throws TimelineParseException {
    return new TimelineEntityFilters.Builder()
        .entityLimit(parseLongStr(limit))
        .createdTimeBegin(createdTimeStart)
        .createTimeEnd(createdTimeEnd)
        .relatesTo(parseRelationFilters(relatesTo))
        .isRelatedTo(parseRelationFilters(isRelatedTo))
        .infoFilters(parseKVFilters(infofilters, false))
        .configFilters(parseKVFilters(conffilters, true))
        .metricFilters(parseMetricFilters(metricfilters))
        .eventFilters(parseEventFilters(eventfilters))
        .fromId(parseStr(fromid)).build();
  }

  /**
   * Parse the passed fields represented as strings and convert them into a
   * {@link TimelineDataToRetrieve} object.
   * @param confs confs to retrieve.
   * @param metrics metrics to retrieve.
   * @param fields fields to retrieve.
   * @param metricsLimit upper limit on number of metrics to return.
   * @return a {@link TimelineDataToRetrieve} object.
   * @throws TimelineParseException if any problem occurs during parsing.
   */
  static TimelineDataToRetrieve createTimelineDataToRetrieve(String confs,
      String metrics, String fields, String metricsLimit,
      String metricsTimeBegin, String metricsTimeEnd)
      throws TimelineParseException {
    return new TimelineDataToRetrieve(parseDataToRetrieve(confs),
        parseDataToRetrieve(metrics), parseFieldsStr(fields,
        TimelineParseConstants.COMMA_DELIMITER), parseIntStr(metricsLimit),
        parseLongStr(metricsTimeBegin), parseLongStr(metricsTimeEnd));
  }

  /**
   * Parse a delimited string and convert it into a set of strings. For
   * instance, if delimiter is ",", then the string should be represented as
   * "value1,value2,value3".
   * @param str delimited string.
   * @param delimiter string is delimited by this delimiter.
   * @return set of strings.
   */
  static TimelineFilterList parseEventFilters(String expr)
      throws TimelineParseException {
    return parseFilters(new TimelineParserForExistFilters(expr,
        TimelineParseConstants.COMMA_CHAR));
  }

  /**
   * Parse relation filters.
   * @param expr Relation filter expression
   * @return a {@link TimelineFilterList} object.
   *
   * @throws Exception if any problem occurs.
   */
  static TimelineFilterList parseRelationFilters(String expr)
      throws TimelineParseException {
    return parseFilters(new TimelineParserForRelationFilters(expr,
        TimelineParseConstants.COMMA_CHAR,
        TimelineParseConstants.COLON_DELIMITER));
  }

  private static TimelineFilterList parseFilters(TimelineParser parser)
      throws TimelineParseException {
    try {
      return parser.parse();
    } finally {
      IOUtils.closeQuietly(parser);
    }
  }

  /**
   * Parses config and info filters.
   *
   * @param expr Expression to be parsed.
   * @param valueAsString true, if value has to be interpreted as string, false
   *     otherwise. It is true for config filters and false for info filters.
   * @return a {@link TimelineFilterList} object.
   * @throws TimelineParseException if any problem occurs during parsing.
   */
  static TimelineFilterList parseKVFilters(String expr, boolean valueAsString)
      throws TimelineParseException {
    return parseFilters(new TimelineParserForKVFilters(expr, valueAsString));
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
   * Parses metric filters.
   *
   * @param expr Metric filter expression to be parsed.
   * @return a {@link TimelineFilterList} object.
   * @throws TimelineParseException if any problem occurs during parsing.
   */
  static TimelineFilterList parseMetricFilters(String expr)
      throws TimelineParseException {
    return parseFilters(new TimelineParserForNumericFilters(expr));
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
   * Interpret passed string as a integer.
   * @param str Passed string.
   * @return integer representation if string is not null, null otherwise.
   */
  static Integer parseIntStr(String str) {
    return str == null ? null : Integer.parseInt(str.trim());
  }

  /**
   * Trims the passed string if its not null.
   * @param str Passed string.
   * @return trimmed string if string is not null, null otherwise.
   */
  static String parseStr(String str) {
    return StringUtils.trimToNull(str);
  }

  /**
   * Get UGI based on the remote user in the HTTP request.
   *
   * @param req HTTP request.
   * @return UGI.
   */
  public static UserGroupInformation getUser(HttpServletRequest req) {
    return getCallerUserGroupInformation(req, false);
  }

  /**
   * Get UGI from the HTTP request.
   *
   * @param hsr HTTP request.
   * @param usePrincipal if true, use principal name else use remote user name
   * @return UGI.
   */
  public static UserGroupInformation getCallerUserGroupInformation(
      HttpServletRequest hsr, boolean usePrincipal) {

    String remoteUser = hsr.getRemoteUser();
    if (usePrincipal) {
      Principal princ = hsr.getUserPrincipal();
      remoteUser = princ == null ? null : princ.getName();
    }

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

  /**
   * Parses confstoretrieve and metricstoretrieve.
   * @param str String representing confs/metrics to retrieve expression.
   *
   * @return a {@link TimelineFilterList} object.
   * @throws TimelineParseException if any problem occurs during parsing.
   */
  static TimelineFilterList parseDataToRetrieve(String expr)
        throws TimelineParseException {
    return parseFilters(new TimelineParserForDataToRetrieve(expr));
  }
}