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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Singleton;

/** REST end point for Timeline Reader */
@Private
@Unstable
@Singleton
@Path("/ws/v2/timeline")
public class TimelineReaderWebServices {
  private static final Log LOG =
      LogFactory.getLog(TimelineReaderWebServices.class);

  @Context private ServletContext ctxt;

  private static final String COMMA_DELIMITER = ",";
  private static final String COLON_DELIMITER = ":";
  private static final String QUERY_STRING_SEP = "?";
  private static final String RANGE_DELIMITER = "-";
  private static final String DATE_PATTERN = "yyyyMMdd";

  @VisibleForTesting
  static ThreadLocal<DateFormat> DATE_FORMAT = new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      SimpleDateFormat format =
          new SimpleDateFormat(DATE_PATTERN, Locale.ENGLISH);
      format.setTimeZone(TimeZone.getTimeZone("GMT"));
      format.setLenient(false);
      return format;
    }
  };

  private void init(HttpServletResponse response) {
    response.setContentType(null);
  }

  private static class DateRange {
    Long dateStart;
    Long dateEnd;
    private DateRange(Long start, Long end) {
      this.dateStart = start;
      this.dateEnd = end;
    }
  }

  private static long parseDate(String strDate) throws ParseException {
    Date date = DATE_FORMAT.get().parse(strDate);
    return date.getTime();
  }

  /**
   * Parses date range which can be a single date or in the format
   * "[startdate]-[enddate]" where either of start or end date may not exist.
   * @param dateRange
   * @return a {@link DateRange} object.
   * @throws IllegalArgumentException
   */
  private static DateRange parseDateRange(String dateRange)
      throws IllegalArgumentException {
    if (dateRange == null || dateRange.isEmpty()) {
      return new DateRange(null, null);
    }
    // Split date range around "-" fetching two components indicating start and
    // end date.
    String[] dates = dateRange.split(RANGE_DELIMITER, 2);
    Long start = null;
    Long end = null;
    try {
      String startDate = dates[0].trim();
      if (!startDate.isEmpty()) {
        // Start date is not in yyyyMMdd format.
        if (startDate.length() != DATE_PATTERN.length()) {
          throw new IllegalArgumentException("Invalid date range " + dateRange);
        }
        // Parse start date which exists before "-" in date range.
        // If "-" does not exist in date range, this effectively
        // gives single date.
        start = parseDate(startDate);
      }
      if (dates.length > 1) {
        String endDate = dates[1].trim();
        if (!endDate.isEmpty()) {
          // End date is not in yyyyMMdd format.
          if (endDate.length() != DATE_PATTERN.length()) {
            throw new IllegalArgumentException(
                "Invalid date range " + dateRange);
          }
          // Parse end date which exists after "-" in date range.
          end = parseDate(endDate);
        }
      } else {
        // Its a single date(without "-" in date range), so set
        // end equal to start.
        end = start;
      }
      if (start != null && end != null) {
        if (start > end) {
          throw new IllegalArgumentException("Invalid date range " + dateRange);
        }
      }
      return new DateRange(start, end);
    } catch (ParseException e) {
      // Date could not be parsed.
      throw new IllegalArgumentException("Invalid date range " + dateRange);
    }
  }

  private static Set<String> parseValuesStr(String str, String delimiter) {
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
  private static <T> void parseKeyValues(Map<String,T> map, String str,
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

  private static Map<String, Set<String>> parseKeyStrValuesStr(String str,
      String pairsDelim, String keyValuesDelim) {
    if (str == null) {
      return null;
    }
    Map<String, Set<String>> map = new HashMap<String, Set<String>>();
    parseKeyValues(map, str,pairsDelim, keyValuesDelim, true, true);
    return map;
  }

  private static Map<String, String> parseKeyStrValueStr(String str,
      String pairsDelim, String keyValDelim) {
    if (str == null) {
      return null;
    }
    Map<String, String> map = new HashMap<String, String>();
    parseKeyValues(map, str, pairsDelim, keyValDelim, true, false);
    return map;
  }

  private static Map<String, Object> parseKeyStrValueObj(String str,
      String pairsDelim, String keyValDelim) {
    if (str == null) {
      return null;
    }
    Map<String, Object> map = new HashMap<String, Object>();
    parseKeyValues(map, str, pairsDelim, keyValDelim, false, false);
    return map;
  }

  private static EnumSet<Field> parseFieldsStr(String str, String delimiter) {
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

  private static Long parseLongStr(String str) {
    return str == null ? null : Long.parseLong(str.trim());
  }

  private static String parseStr(String str) {
    return str == null ? null : str.trim();
  }

  private static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUGI;
  }

  private static String getUserName(UserGroupInformation callerUGI) {
    return ((callerUGI != null) ? callerUGI.getUserName().trim() : "");
  }

  private TimelineReaderManager getTimelineReaderManager() {
    return (TimelineReaderManager)
        ctxt.getAttribute(TimelineReaderServer.TIMELINE_READER_MANAGER_ATTR);
  }

  private static void handleException(Exception e, String url, long startTime,
      String invalidNumMsg) throws BadRequestException,
      WebApplicationException {
    long endTime = Time.monotonicNow();
    LOG.info("Processed URL " + url + " but encountered exception (Took " +
        (endTime - startTime) + " ms.)");
    if (e instanceof NumberFormatException) {
      throw new BadRequestException(invalidNumMsg + " is not a numeric value.");
    } else if (e instanceof IllegalArgumentException) {
      throw new BadRequestException(e.getMessage() == null ?
          "Requested Invalid Field." : e.getMessage());
    } else {
      LOG.error("Error while processing REST request", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Return the description of the timeline reader web services.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineAbout about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    init(res);
    return TimelineUtils.createTimelineAbout("Timeline Reader API");
  }

  /**
   * Return a set of entities that match the given parameters. Cluster ID is not
   * provided by client so default cluster ID has to be taken.
   */
  @GET
  @Path("/entities/{appid}/{entitytype}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("appid") String appId,
      @PathParam("entitytype") String entityType,
      @QueryParam("userid") String userId,
      @QueryParam("flowid") String flowId,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("modifiedtimestart") String modifiedTimeStart,
      @QueryParam("modifiedtimeend") String modifiedTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
       @QueryParam("fields") String fields) {
    return getEntities(req, res, null, appId, entityType, userId, flowId,
        flowRunId, limit, createdTimeStart, createdTimeEnd, modifiedTimeStart,
        modifiedTimeEnd, relatesTo, isRelatedTo, infofilters, conffilters,
        metricfilters, eventfilters, fields);
  }

  /**
   * Return a set of entities that match the given parameters.
   */
  @GET
  @Path("/entities/{clusterid}/{appid}/{entitytype}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId,
      @PathParam("entitytype") String entityType,
      @QueryParam("userid") String userId,
      @QueryParam("flowid") String flowId,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("modifiedtimestart") String modifiedTimeStart,
      @QueryParam("modifiedtimeend") String modifiedTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI = getUser(req);
    LOG.info("Received URL " + url + " from user " + getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      entities = timelineReaderManager.getEntities(
          parseStr(userId), parseStr(clusterId), parseStr(flowId),
          parseLongStr(flowRunId), parseStr(appId), parseStr(entityType),
          parseLongStr(limit), parseLongStr(createdTimeStart),
          parseLongStr(createdTimeEnd), parseLongStr(modifiedTimeStart),
          parseLongStr(modifiedTimeEnd),
          parseKeyStrValuesStr(relatesTo, COMMA_DELIMITER, COLON_DELIMITER),
          parseKeyStrValuesStr(isRelatedTo, COMMA_DELIMITER, COLON_DELIMITER),
          parseKeyStrValueObj(infofilters, COMMA_DELIMITER, COLON_DELIMITER),
          parseKeyStrValueStr(conffilters, COMMA_DELIMITER, COLON_DELIMITER),
          parseValuesStr(metricfilters, COMMA_DELIMITER),
          parseValuesStr(eventfilters, COMMA_DELIMITER),
          parseFieldsStr(fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime,
          "createdTime or modifiedTime start/end or limit or flowrunid");
    }
    long endTime = Time.monotonicNow();
    if (entities == null) {
      entities = Collections.emptySet();
    }
    LOG.info("Processed URL " + url +
        " (Took " + (endTime - startTime) + " ms.)");
    return entities;
  }

  /**
   * Return a single entity of the given entity type and Id. Cluster ID is not
   * provided by client so default cluster ID has to be taken.
   */
  @GET
  @Path("/entity/{appid}/{entitytype}/{entityid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("appid") String appId,
      @PathParam("entitytype") String entityType,
      @PathParam("entityid") String entityId,
      @QueryParam("userid") String userId,
      @QueryParam("flowid") String flowId,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("fields") String fields) {
    return getEntity(req, res, null, appId, entityType, entityId, userId,
        flowId, flowRunId, fields);
  }

  /**
   * Return a single entity of the given entity type and Id.
   */
  @GET
  @Path("/entity/{clusterid}/{appid}/{entitytype}/{entityid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId,
      @PathParam("entitytype") String entityType,
      @PathParam("entityid") String entityId,
      @QueryParam("userid") String userId,
      @QueryParam("flowid") String flowId,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI = getUser(req);
    LOG.info("Received URL " + url + " from user " + getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(
          parseStr(userId), parseStr(clusterId), parseStr(flowId),
          parseLongStr(flowRunId), parseStr(appId), parseStr(entityType),
          parseStr(entityId), parseFieldsStr(fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime, "flowrunid");
    }
    long endTime = Time.monotonicNow();
    if (entity == null) {
      LOG.info("Processed URL " + url + " but entity not found" + " (Took " +
          (endTime - startTime) + " ms.)");
      throw new NotFoundException("Timeline entity {id: " + parseStr(entityId) +
          ", type: " + parseStr(entityType) + " } is not found");
    }
    LOG.info("Processed URL " + url +
        " (Took " + (endTime - startTime) + " ms.)");
    return entity;
  }

  /**
   * Return a single flow run for the given user, flow id and run id.
   * Cluster ID is not provided by client so default cluster ID has to be taken.
   */
  @GET
  @Path("/flowrun/{userid}/{flowid}/{flowrunid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getFlowRun(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("flowid") String flowId,
      @PathParam("flowrunid") String flowRunId,
      @QueryParam("fields") String fields) {
    return getFlowRun(req, res, userId, null, flowId, flowRunId, fields);
  }

  /**
   * Return a single flow run for the given user, cluster, flow id and run id.
   */
  @GET
  @Path("/flowrun/{userid}/{clusterid}/{flowid}/{flowrunid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getFlowRun(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("clusterid") String clusterId,
      @PathParam("flowid") String flowId,
      @PathParam("flowrunid") String flowRunId,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI = getUser(req);
    LOG.info("Received URL " + url + " from user " + getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(parseStr(userId),
          parseStr(clusterId), parseStr(flowId), parseLongStr(flowRunId), null,
          TimelineEntityType.YARN_FLOW_RUN.toString(), null,
          parseFieldsStr(fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime, "flowrunid");
    }
    long endTime = Time.monotonicNow();
    if (entity == null) {
      LOG.info("Processed URL " + url + " but flowrun not found (Took " +
          (endTime - startTime) + " ms.)");
      throw new NotFoundException("Flow run {flow id: " + parseStr(flowId) +
          ", run id: " + parseLongStr(flowRunId) + " } is not found");
    }
    LOG.info("Processed URL " + url +
        " (Took " + (endTime - startTime) + " ms.)");
    return entity;
  }

  /**
   * Return a set of flows runs for the given user and flow id.
   * Cluster ID is not provided by client so default cluster ID has to be taken.
   */
  @GET
  @Path("/flowruns/{userid}/{flowid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowRuns(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("flowid") String flowId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("fields") String fields) {
    return getFlowRuns(req, res, userId, null, flowId, limit, createdTimeStart,
        createdTimeEnd, fields);
  }

  /**
   * Return a set of flow runs for the given user, cluster and flow id.
   */
  @GET
  @Path("/flowruns/{userid}/{clusterid}/{flowid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowRuns(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("clusterid") String clusterId,
      @PathParam("flowid") String flowId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI = getUser(req);
    LOG.info("Received URL " + url + " from user " + getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      entities = timelineReaderManager.getEntities(
          parseStr(userId), parseStr(clusterId), parseStr(flowId), null, null,
          TimelineEntityType.YARN_FLOW_RUN.toString(), parseLongStr(limit),
          parseLongStr(createdTimeStart), parseLongStr(createdTimeEnd), null,
          null, null, null, null, null, null, null,
          parseFieldsStr(fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime, "createdTime start/end or limit");
    }
    long endTime = Time.monotonicNow();
    if (entities == null) {
      entities = Collections.emptySet();
    }
    LOG.info("Processed URL " + url +
        " (Took " + (endTime - startTime) + " ms.)");
    return entities;
  }

  /**
   * Return a list of flows. Cluster ID is not provided by client so default
   * cluster ID has to be taken. daterange, if specified is given as
   * "[startdate]-[enddate]"(i.e. start and end date separated by -) or
   * single date. Dates are interpreted in yyyyMMdd format and are assumed to
   * be in GMT. If a single date is specified, all flows active on that date are
   * returned. If both startdate and enddate is given, all flows active between
   * start and end date will be returned. If only startdate is given, flows
   * active on and after startdate are returned. If only enddate is given, flows
   * active on and before enddate are returned.
   * For example :
   * "daterange=20150711" returns flows active on 20150711.
   * "daterange=20150711-20150714" returns flows active between these 2 dates.
   * "daterange=20150711-" returns flows active on and after 20150711.
   * "daterange=-20150711" returns flows active on and before 20150711.
   */
  @GET
  @Path("/flows/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlows(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @QueryParam("limit") String limit,
      @QueryParam("daterange") String dateRange,
      @QueryParam("fields") String fields) {
    return getFlows(req, res, null, limit, dateRange, fields);
  }

  /**
   * Return a list of flows for a given cluster id. daterange, if specified is
   * given as "[startdate]-[enddate]"(i.e. start and end date separated by -) or
   * single date. Dates are interpreted in yyyyMMdd format and are assumed to
   * be in GMT. If a single date is specified, all flows active on that date are
   * returned. If both startdate and enddate is given, all flows active between
   * start and end date will be returned. If only startdate is given, flows
   * active on and after startdate are returned. If only enddate is given, flows
   * active on and before enddate are returned.
   * For example :
   * "daterange=20150711" returns flows active on 20150711.
   * "daterange=20150711-20150714" returns flows active between these 2 dates.
   * "daterange=20150711-" returns flows active on and after 20150711.
   * "daterange=-20150711" returns flows active on and before 20150711.
   */
  @GET
  @Path("/flows/{clusterid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlows(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @QueryParam("limit") String limit,
      @QueryParam("daterange") String dateRange,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI = getUser(req);
    LOG.info("Received URL " + url + " from user " + getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      DateRange range = parseDateRange(dateRange);
      entities = timelineReaderManager.getEntities(
          null, parseStr(clusterId), null, null, null,
          TimelineEntityType.YARN_FLOW_ACTIVITY.toString(), parseLongStr(limit),
          range.dateStart, range.dateEnd, null, null, null, null, null, null,
          null, null, parseFieldsStr(fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime, "limit");
    }
    long endTime = Time.monotonicNow();
    if (entities == null) {
      entities = Collections.emptySet();
    }
    LOG.info("Processed URL " + url +
        " (Took " + (endTime - startTime) + " ms.)");
    return entities;
  }

  /**
   * Return a single app for given app id. Cluster ID is not provided by
   * client so default cluster ID has to be taken.
   */
  @GET
  @Path("/app/{appid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getApp(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("appid") String appId,
      @QueryParam("flowid") String flowId,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("userid") String userId,
      @QueryParam("fields") String fields) {
    return getApp(req, res, null, appId, flowId, flowRunId, userId, fields);
  }

  /**
   * Return a single app for given cluster id and app id.
   */
  @GET
  @Path("/app/{clusterid}/{appid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getApp(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId,
      @QueryParam("flowid") String flowId,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("userid") String userId,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI = getUser(req);
    LOG.info("Received URL " + url + " from user " + getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(parseStr(userId),
          parseStr(clusterId), parseStr(flowId), parseLongStr(flowRunId),
          parseStr(appId), TimelineEntityType.YARN_APPLICATION.toString(), null,
          parseFieldsStr(fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime, "flowrunid");
    }
    long endTime = Time.monotonicNow();
    if (entity == null) {
      LOG.info("Processed URL " + url + " but app not found" + " (Took " +
          (endTime - startTime) + " ms.)");
      throw new NotFoundException("App " + appId + " not found");
    }
    LOG.info("Processed URL " + url +
        " (Took " + (endTime - startTime) + " ms.)");
    return entity;
  }

  /**
   * Return a list of apps for given user, flow id and flow run id. Cluster ID
   * is not provided by client so default cluster ID has to be taken. If number
   * of matching apps are more than the limit, most recent apps till the limit
   * is reached, will be returned.
   */
  @GET
  @Path("/flowrunapps/{userid}/{flowid}/{flowrunid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowRunApps(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("flowid") String flowId,
      @PathParam("flowrunid") String flowRunId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("modifiedtimestart") String modifiedTimeStart,
      @QueryParam("modifiedtimeend") String modifiedTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("fields") String fields) {
    return getEntities(req, res, null, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowId,
        flowRunId, limit, createdTimeStart, createdTimeEnd, modifiedTimeStart,
        modifiedTimeEnd, relatesTo, isRelatedTo, infofilters, conffilters,
        metricfilters, eventfilters, fields);
  }

  /**
   * Return a list of apps for a given user, cluster id, flow id and flow run
   * id. If number of matching apps are more than the limit, most recent apps
   * till the limit is reached, will be returned.
   */
  @GET
  @Path("/flowrunapps/{userid}/{clusterid}/{flowid}/{flowrunid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowRunApps(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("clusterid") String clusterId,
      @PathParam("flowid") String flowId,
      @PathParam("flowrunid") String flowRunId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("modifiedtimestart") String modifiedTimeStart,
      @QueryParam("modifiedtimeend") String modifiedTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("fields") String fields) {
    return getEntities(req, res, clusterId, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowId,
        flowRunId, limit, createdTimeStart, createdTimeEnd, modifiedTimeStart,
        modifiedTimeEnd, relatesTo, isRelatedTo, infofilters, conffilters,
        metricfilters, eventfilters, fields);
  }

  /**
   * Return a list of apps for given user and flow id. Cluster ID is not
   * provided by client so default cluster ID has to be taken. If number of
   * matching apps are more than the limit, most recent apps till the limit is
   * reached, will be returned.
   */
  @GET
  @Path("/flowapps/{userid}/{flowid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowApps(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("flowid") String flowId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("modifiedtimestart") String modifiedTimeStart,
      @QueryParam("modifiedtimeend") String modifiedTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("fields") String fields) {
    return getEntities(req, res, null, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowId,
        null, limit, createdTimeStart, createdTimeEnd, modifiedTimeStart,
        modifiedTimeEnd, relatesTo, isRelatedTo, infofilters, conffilters,
        metricfilters, eventfilters, fields);
  }

  /**
   * Return a list of apps for a given user, cluster id and flow id. If number
   * of matching apps are more than the limit, most recent apps till the limit
   * is reached, will be returned.
   */
  @GET
  @Path("/flowapps/{userid}/{clusterid}/{flowid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowApps(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("clusterid") String clusterId,
      @PathParam("flowid") String flowId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("modifiedtimestart") String modifiedTimeStart,
      @QueryParam("modifiedtimeend") String modifiedTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("fields") String fields) {
    return getEntities(req, res, clusterId, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowId,
        null, limit, createdTimeStart, createdTimeEnd, modifiedTimeStart,
        modifiedTimeEnd, relatesTo, isRelatedTo, infofilters, conffilters,
        metricfilters, eventfilters, fields);
  }
}