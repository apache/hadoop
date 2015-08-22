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
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
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

  private static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUGI;
  }

  private TimelineReaderManager getTimelineReaderManager() {
    return (TimelineReaderManager)
        ctxt.getAttribute(TimelineReaderServer.TIMELINE_READER_MANAGER_ATTR);
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
   * Return a set of entities that match the given parameters.
   */
  @GET
  @Path("/entities/{clusterId}/{appId}/{entityType}")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterId") String clusterId,
      @PathParam("appId") String appId,
      @PathParam("entityType") String entityType,
      @QueryParam("userId") String userId,
      @QueryParam("flowId") String flowId,
      @QueryParam("flowRunId") String flowRunId,
      @QueryParam("limit") String limit,
      @QueryParam("createdTimeStart") String createdTimeStart,
      @QueryParam("createdTimeEnd") String createdTimeEnd,
      @QueryParam("modifiedTimeStart") String modifiedTimeStart,
      @QueryParam("modifiedTimeEnd") String modifiedTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("fields") String fields) {
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    UserGroupInformation callerUGI = getUser(req);
    try {
      return timelineReaderManager.getEntities(
          callerUGI != null && (userId == null || userId.isEmpty()) ?
          callerUGI.getUserName().trim() : parseStr(userId),
          parseStr(clusterId), parseStr(flowId),
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
    } catch (NumberFormatException e) {
      throw new BadRequestException(
          "createdTime or modifiedTime start/end or limit or flowId is not" +
          " a numeric value.");
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Requested Invalid Field.");
    } catch (Exception e) {
      LOG.error("Error getting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Return a single entity of the given entity type and Id.
   */
  @GET
  @Path("/entity/{clusterId}/{appId}/{entityType}/{entityId}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterId") String clusterId,
      @PathParam("appId") String appId,
      @PathParam("entityType") String entityType,
      @PathParam("entityId") String entityId,
      @QueryParam("userId") String userId,
      @QueryParam("flowId") String flowId,
      @QueryParam("flowRunId") String flowRunId,
      @QueryParam("fields") String fields) {
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    UserGroupInformation callerUGI = getUser(req);
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(
          callerUGI != null && (userId == null || userId.isEmpty()) ?
          callerUGI.getUserName().trim() : parseStr(userId),
          parseStr(clusterId), parseStr(flowId), parseLongStr(flowRunId),
          parseStr(appId), parseStr(entityType), parseStr(entityId),
          parseFieldsStr(fields, COMMA_DELIMITER));
    } catch (NumberFormatException e) {
      throw new BadRequestException("flowRunId is not a numeric value.");
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Requested Invalid Field.");
    } catch (Exception e) {
      LOG.error("Error getting entity", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (entity == null) {
      throw new NotFoundException("Timeline entity {id: " + parseStr(entityId) +
          ", type: " + parseStr(entityType) + " } is not found");
    }
    return entity;
  }
}