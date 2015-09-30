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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

  private void init(HttpServletResponse response) {
    response.setContentType(null);
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

  private static String parseUser(UserGroupInformation callerUGI, String user) {
    return (callerUGI != null && (user == null || user.isEmpty()) ?
        callerUGI.getUserName().trim() : parseStr(user));
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
      throw new BadRequestException("Requested Invalid Field.");
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
        (null == req.getQueryString() ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI = getUser(req);
    LOG.info("Received URL " + url + " from user " + getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      entities = timelineReaderManager.getEntities(
          parseUser(callerUGI, userId), parseStr(clusterId), parseStr(flowId),
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
          "createdTime or modifiedTime start/end or limit or flowId");
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
        (null == req.getQueryString() ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI = getUser(req);
    LOG.info("Received URL " + url + " from user " + getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(
          parseUser(callerUGI, userId), parseStr(clusterId), parseStr(flowId),
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
   * Return a single flow run for the given cluster, flow id and run id.
   * Cluster ID is not provided by client so default cluster ID has to be taken.
   */
  @GET
  @Path("/flowrun/{flowid}/{flowrunid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getFlowRun(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("flowid") String flowId,
      @PathParam("flowrunid") String flowRunId,
      @QueryParam("userid") String userId,
      @QueryParam("fields") String fields) {
    return getFlowRun(req, res, null, flowId, flowRunId, userId, fields);
  }

  /**
   * Return a single flow run for the given cluster, flow id and run id.
   */
  @GET
  @Path("/flowrun/{clusterid}/{flowid}/{flowrunid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getFlowRun(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("flowid") String flowId,
      @PathParam("flowrunid") String flowRunId,
      @QueryParam("userid") String userId,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (null == req.getQueryString() ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI = getUser(req);
    LOG.info("Received URL " + url + " from user " + getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(
          parseUser(callerUGI, userId), parseStr(clusterId),
          parseStr(flowId), parseLongStr(flowRunId), null,
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
   * Return a list of flows for a given cluster id. Cluster ID is not
   * provided by client so default cluster ID has to be taken.
   */
  @GET
  @Path("/flows/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlows(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @QueryParam("limit") String limit,
      @QueryParam("fields") String fields) {
    return getFlows(req, res, null, limit, fields);
  }

  /**
   * Return a list of flows for a given cluster id.
   */
  @GET
  @Path("/flows/{clusterid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlows(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @QueryParam("limit") String limit,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (null == req.getQueryString() ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI = getUser(req);
    LOG.info("Received URL " + url + " from user " + getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      entities = timelineReaderManager.getEntities(
          null, parseStr(clusterId), null, null, null,
          TimelineEntityType.YARN_FLOW_ACTIVITY.toString(), parseLongStr(limit),
          null, null, null, null, null, null, null, null, null, null,
          parseFieldsStr(fields, COMMA_DELIMITER));
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
}