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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
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
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Singleton;

/** REST end point for Timeline Reader. */
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
    } else if (e instanceof NotFoundException) {
      throw (NotFoundException)e;
    } else if (e instanceof BadRequestException) {
      throw (BadRequestException)e;
    } else {
      LOG.error("Error while processing REST request", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Return the description of the timeline reader web services.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   *
   * @return information about the cluster including timeline version.
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
   * Return a single entity for a given entity type and UID which is a delimited
   * string containing clusterid, userid, flow name, flowrun id and app id.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param uId a delimited string containing clusterid, userid, flow name,
   *     flowrun id and app id which are extracted from UID and then used to
   *     query backend(Mandatory path param).
   * @param limit Number of entities to return(Optional query param).
   * @param createdTimeStart If specified, matched entities should not be
   *     created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched entities should not be created
   *     after this timestamp(Optional query param).
   * @param relatesTo If specified, matched entities should relate to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param isRelatedTo If specified, matched entities should be related to
   *     given entities associated with a entity type. relatesto is a comma
   *     separated list in the format [entitytype]:[entityid1]:[entityid2]...
   *     (Optional query param).
   * @param infofilters If specified, matched entities should have exact matches
   *     to the given info represented as key-value pairs. This is represented
   *     as infofilters=info1:value1,info2:value2... (Optional query param).
   * @param conffilters If specified, matched entities should have exact matches
   *     to the given configs represented as key-value pairs. This is
   *     represented as conffilters=conf1:value1,conf2:value2... (Optional query
   *     param).
   * @param metricfilters If specified, matched entities should contain the
   *     given metrics. This is represented as
   *     metricfilters=metricid1, metricid2... (Optional query param).
   * @param eventfilters If specified, matched entities should contain the given
   *     events. This is represented as eventfilters=eventid1, eventid2...
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type, id and created time is returned
   *     (Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of {@link TimelineEntity} instances of the given entity type is
   *     returned.
   *     On failures,
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/app-uid/{uid}/entities/{entitytype}")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("uid") String uId,
      @PathParam("entitytype") String entityType,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
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
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.APPLICATION_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      entities = timelineReaderManager.getEntities(
          TimelineReaderWebServicesUtils.parseStr(context.getUserId()),
          TimelineReaderWebServicesUtils.parseStr(context.getClusterId()),
          TimelineReaderWebServicesUtils.parseStr(context.getFlowName()),
          context.getFlowRunId(),
          TimelineReaderWebServicesUtils.parseStr(context.getAppId()),
          TimelineReaderWebServicesUtils.parseStr(entityType),
          TimelineReaderWebServicesUtils.parseLongStr(limit),
          TimelineReaderWebServicesUtils.parseLongStr(createdTimeStart),
          TimelineReaderWebServicesUtils.parseLongStr(createdTimeEnd),
          TimelineReaderWebServicesUtils.parseKeyStrValuesStr(
          relatesTo, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseKeyStrValuesStr(
          isRelatedTo, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseKeyStrValueObj(
          infofilters, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseKeyStrValueStr(
          conffilters, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseValuesStr(
           metricfilters, COMMA_DELIMITER),
          TimelineReaderWebServicesUtils.parseValuesStr(
          eventfilters, COMMA_DELIMITER),
          TimelineReaderWebServicesUtils.parseFieldsStr(
          fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime,
          "createdTime start/end or limit or flowrunid");
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
   * Return a set of entities that match the given parameters. Cluster ID is not
   * provided by client so default cluster ID has to be taken. If userid, flow
   * name and flowrun id which are optional query parameters are not specified,
   * they will be queried based on app id and default cluster id from the flow
   * context information stored in underlying storage implementation. If number
   * of matching entities are more than the limit, most recent entities till the
   * limit is reached, will be returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param appId Application id to which the entities to be queried belong to(
   *     Mandatory path param).
   * @param entityType Type of entities(Mandatory path param).
   * @param userId User id which should match for the entities(Optional query
   *     param)
   * @param flowName Flow name which should match for the entities(Optional
   *     query param).
   * @param flowRunId Run id which should match for the entities(Optional query
   *     param).
   * @param limit Number of entities to return(Optional query param).
   * @param createdTimeStart If specified, matched entities should not be
   *     created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched entities should not be created
   *     after this timestamp(Optional query param).
   * @param relatesTo If specified, matched entities should relate to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param isRelatedTo If specified, matched entities should be related to
   *     given entities associated with a entity type. relatesto is a comma
   *     separated list in the format [entitytype]:[entityid1]:[entityid2]...
   *     (Optional query param).
   * @param infofilters If specified, matched entities should have exact matches
   *     to the given info represented as key-value pairs. This is represented
   *     as infofilters=info1:value1,info2:value2... (Optional query param).
   * @param conffilters If specified, matched entities should have exact matches
   *     to the given configs represented as key-value pairs. This is
   *     represented as conffilters=conf1:value1,conf2:value2... (Optional query
   *     param).
   * @param metricfilters If specified, matched entities should contain the
   *     given metrics. This is represented as
   *     metricfilters=metricid1, metricid2... (Optional query param).
   * @param eventfilters If specified, matched entities should contain the given
   *     events. This is represented as eventfilters=eventid1, eventid2...
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type, id, created time is returned
   *     (Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of {@link TimelineEntity} instances of the given entity type is
   *     returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     If flow context information cannot be retrieved, HTTP 404(Not Found)
   *     is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/apps/{appid}/entities/{entitytype}")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("appid") String appId,
      @PathParam("entitytype") String entityType,
      @QueryParam("userid") String userId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("fields") String fields) {
    return getEntities(req, res, null, appId, entityType, userId, flowName,
        flowRunId, limit, createdTimeStart, createdTimeEnd, relatesTo,
        isRelatedTo, infofilters, conffilters, metricfilters, eventfilters,
        fields);
  }

  /**
   * Return a set of entities that match the given parameters. If userid, flow
   * name and flowrun id which are optional query parameters are not specified,
   * they will be queried based on app id and cluster id from the flow context
   * information stored in underlying storage implementation. If number of
   * matching entities are more than the limit, most recent entities till the
   * limit is reached, will be returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the entities to be queried belong to(
   *     Mandatory path param).
   * @param appId Application id to which the entities to be queried belong to(
   *     Mandatory path param).
   * @param entityType Type of entities(Mandatory path param).
   * @param userId User id which should match for the entities(Optional query
   *     param)
   * @param flowName Flow name which should match for the entities(Optional
   *     query param).
   * @param flowRunId Run id which should match for the entities(Optional query
   *     param).
   * @param limit Number of entities to return(Optional query param).
   * @param createdTimeStart If specified, matched entities should not be
   *     created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched entities should not be created
   *     after this timestamp(Optional query param).
   * @param relatesTo If specified, matched entities should relate to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param isRelatedTo If specified, matched entities should be related to
   *     given entities associated with a entity type. relatesto is a comma
   *     separated list in the format [entitytype]:[entityid1]:[entityid2]...
   *     (Optional query param).
   * @param infofilters If specified, matched entities should have exact matches
   *     to the given info represented as key-value pairs. This is represented
   *     as infofilters=info1:value1,info2:value2... (Optional query param).
   * @param conffilters If specified, matched entities should have exact matches
   *     to the given configs represented as key-value pairs. This is
   *     represented as conffilters=conf1:value1,conf2:value2... (Optional query
   *     param).
   * @param metricfilters If specified, matched entities should contain the
   *     given metrics. This is represented as
   *     metricfilters=metricid1, metricid2... (Optional query param).
   * @param eventfilters If specified, matched entities should contain the given
   *     events. This is represented as eventfilters=eventid1, eventid2...
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type, id, created time is returned
   *     (Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of {@link TimelineEntity} instances of the given entity type is
   *     returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     If flow context information cannot be retrieved, HTTP 404(Not Found)
   *     is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/entities/{entitytype}")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId,
      @PathParam("entitytype") String entityType,
      @QueryParam("userid") String userId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
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
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      entities = timelineReaderManager.getEntities(
          TimelineReaderWebServicesUtils.parseStr(userId),
          TimelineReaderWebServicesUtils.parseStr(clusterId),
          TimelineReaderWebServicesUtils.parseStr(flowName),
          TimelineReaderWebServicesUtils.parseLongStr(flowRunId),
          TimelineReaderWebServicesUtils.parseStr(appId),
          TimelineReaderWebServicesUtils.parseStr(entityType),
          TimelineReaderWebServicesUtils.parseLongStr(limit),
          TimelineReaderWebServicesUtils.parseLongStr(createdTimeStart),
          TimelineReaderWebServicesUtils.parseLongStr(createdTimeEnd),
          TimelineReaderWebServicesUtils.parseKeyStrValuesStr(
          relatesTo, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseKeyStrValuesStr(
          isRelatedTo, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseKeyStrValueObj(
          infofilters, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseKeyStrValueStr(
          conffilters, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseValuesStr(
          metricfilters, COMMA_DELIMITER),
          TimelineReaderWebServicesUtils.parseValuesStr(
          eventfilters, COMMA_DELIMITER),
          TimelineReaderWebServicesUtils.parseFieldsStr(
          fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime,
          "createdTime start/end or limit or flowrunid");
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
   * Return a single entity for given UID which is a delimited string containing
   * clusterid, userid, flow name, flowrun id, app id, entity type and entityid.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param uId a delimited string containing clusterid, userid, flow name,
   *     flowrun id, app id, entity type and entity id which are extracted from
   *     UID and then used to query backend(Mandatory path param).
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type, id, created time is returned
   *     (Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     {@link TimelineEntity} instance is returned.
   *     On failures,
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.
   *     If entity for the given entity id cannot be found, HTTP 404(Not Found)
   *     is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/entity-uid/{uid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("uid") String uId,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.GENERIC_ENTITY_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      entity = timelineReaderManager.getEntity(context.getUserId(),
          context.getClusterId(), context.getFlowName(), context.getFlowRunId(),
          context.getAppId(), context.getEntityType(), context.getEntityId(),
          TimelineReaderWebServicesUtils.parseFieldsStr(
          fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime, "flowrunid");
    }
    long endTime = Time.monotonicNow();
    if (entity == null) {
      LOG.info("Processed URL " + url + " but entity not found" + " (Took " +
          (endTime - startTime) + " ms.)");
      throw new NotFoundException("Timeline entity with uid: " + uId +
          "is not found");
    }
    LOG.info("Processed URL " + url +
        " (Took " + (endTime - startTime) + " ms.)");
    return entity;
  }

  /**
   * Return a single entity of the given entity type and Id. Cluster ID is not
   * provided by client so default cluster ID has to be taken. If userid, flow
   * name and flowrun id which are optional query parameters are not specified,
   * they will be queried based on app id and default cluster id from the flow
   * context information stored in underlying storage implementation.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param appId Application id to which the entity to be queried belongs to(
   *     Mandatory path param).
   * @param entityType Type of entity(Mandatory path param).
   * @param entityId Id of the entity to be fetched(Mandatory path param).
   * @param userId User id which should match for the entity(Optional query
   *     param).
   * @param flowName Flow name which should match for the entity(Optional query
   *     param).
   * @param flowRunId Run id which should match for the entity(Optional query
   *     param).
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type, id, created time is returned
   *     (Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     {@link TimelineEntity} instance is returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     If flow context information cannot be retrieved or entity for the given
   *     entity id cannot be found, HTTP 404(Not Found) is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/apps/{appid}/entities/{entitytype}/{entityid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("appid") String appId,
      @PathParam("entitytype") String entityType,
      @PathParam("entityid") String entityId,
      @QueryParam("userid") String userId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("fields") String fields) {
    return getEntity(req, res, null, appId, entityType, entityId, userId,
        flowName, flowRunId, fields);
  }

  /**
   * Return a single entity of the given entity type and Id. If userid, flowname
   * and flowrun id which are optional query parameters are not specified, they
   * will be queried based on app id and cluster id from the flow context
   * information stored in underlying storage implementation.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the entity to be queried belongs to(
   *     Mandatory path param).
   * @param appId Application id to which the entity to be queried belongs to(
   *     Mandatory path param).
   * @param entityType Type of entity(Mandatory path param).
   * @param entityId Id of the entity to be fetched(Mandatory path param).
   * @param userId User id which should match for the entity(Optional query
   *     param).
   * @param flowName Flow name which should match for the entity(Optional query
   *     param).
   * @param flowRunId Run id which should match for the entity(Optional query
   *     param).
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type, id and created time is returned
   *     (Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     {@link TimelineEntity} instance is returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     If flow context information cannot be retrieved or entity for the given
   *     entity id cannot be found, HTTP 404(Not Found) is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/entities/{entitytype}/{entityid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId,
      @PathParam("entitytype") String entityType,
      @PathParam("entityid") String entityId,
      @QueryParam("userid") String userId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    String type = TimelineReaderWebServicesUtils.parseStr(entityType);
    String id = TimelineReaderWebServicesUtils.parseStr(entityId);
    try {
      entity = timelineReaderManager.getEntity(
          TimelineReaderWebServicesUtils.parseStr(userId),
          TimelineReaderWebServicesUtils.parseStr(clusterId),
          TimelineReaderWebServicesUtils.parseStr(flowName),
          TimelineReaderWebServicesUtils.parseLongStr(flowRunId),
          TimelineReaderWebServicesUtils.parseStr(appId), type, id,
          TimelineReaderWebServicesUtils.parseFieldsStr(
          fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime, "flowrunid");
    }
    long endTime = Time.monotonicNow();
    if (entity == null) {
      LOG.info("Processed URL " + url + " but entity not found" + " (Took " +
          (endTime - startTime) + " ms.)");
      throw new NotFoundException("Timeline entity {id: " + id + ", type: " +
          type + " } is not found");
    }
    LOG.info("Processed URL " + url +
        " (Took " + (endTime - startTime) + " ms.)");
    return entity;
  }

  /**
   * Return a single flow run for given UID which is a delimited string
   * containing clusterid, userid, flow name and flowrun id.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param uId a delimited string containing clusterid, userid, flow name and
   *     flowrun id which are extracted from UID and then used to query backend
   *     (Mandatory path param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     {@link FlowRunEntity} instance is returned. By default, all metrics for
   *     the flow run will be returned.
   *     On failures,
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.
   *     If flow run for the given flow run id cannot be found, HTTP 404
   *     (Not Found) is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/run-uid/{uid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getFlowRun(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("uid") String uId) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.FLOWRUN_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      entity = timelineReaderManager.getEntity(context.getUserId(),
          context.getClusterId(), context.getFlowName(), context.getFlowRunId(),
          null, TimelineEntityType.YARN_FLOW_RUN.toString(), null, null);
    } catch (Exception e) {
      handleException(e, url, startTime, "flowrunid");
    }
    long endTime = Time.monotonicNow();
    if (entity == null) {
      LOG.info("Processed URL " + url + " but flowrun not found (Took " +
          (endTime - startTime) + " ms.)");
      throw new NotFoundException("Flowrun with uid: " + uId + "is not found");
    }
    LOG.info("Processed URL " + url +
        " (Took " + (endTime - startTime) + " ms.)");
    return entity;
  }

  /**
   * Return a single flow run for the given user, flow name and run id.
   * Cluster ID is not provided by client so default cluster ID has to be taken.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param userId User id representing the user who ran the flow run(Mandatory
   *     path param).
   * @param flowName Flow name to which the flow run to be queried belongs to(
   *     Mandatory path param).
   * @param flowRunId Id of the flow run to be queried(Mandatory path param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     {@link FlowRunEntity} instance is returned. By default, all metrics for
   *     the flow run will be returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     If flow run for the given flow run id cannot be found, HTTP 404
   *     (Not Found) is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/users/{userid}/flows/{flowname}/runs/{flowrunid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getFlowRun(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @PathParam("flowrunid") String flowRunId) {
    return getFlowRun(req, res, null, userId, flowName, flowRunId);
  }

  /**
   * Return a single flow run for the given user, cluster, flow name and run id.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the flow run to be queried belong to(
   *     Mandatory path param).
   * @param userId User id representing the user who ran the flow run(Mandatory
   *     path param).
   * @param flowName Flow name to which the flow run to be queried belongs to(
   *     Mandatory path param).
   * @param flowRunId Id of the flow run to be queried(Mandatory path param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     {@link FlowRunEntity} instance is returned. By default, all metrics for
   *     the flow run will be returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     If flow run for the given flow run id cannot be found, HTTP 404
   *     (Not Found) is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/users/{userid}/flows/{flowname}/"
      + "runs/{flowrunid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getFlowRun(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @PathParam("flowrunid") String flowRunId) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(
          TimelineReaderWebServicesUtils.parseStr(userId),
          TimelineReaderWebServicesUtils.parseStr(clusterId),
          TimelineReaderWebServicesUtils.parseStr(flowName),
          TimelineReaderWebServicesUtils.parseLongStr(flowRunId),
          null, TimelineEntityType.YARN_FLOW_RUN.toString(), null, null);
    } catch (Exception e) {
      handleException(e, url, startTime, "flowrunid");
    }
    long endTime = Time.monotonicNow();
    if (entity == null) {
      LOG.info("Processed URL " + url + " but flowrun not found (Took " +
          (endTime - startTime) + " ms.)");
      throw new NotFoundException("Flow run {flow name: " +
          TimelineReaderWebServicesUtils.parseStr(flowName) + ", run id: " +
          TimelineReaderWebServicesUtils.parseLongStr(flowRunId) +
          " } is not found");
    }
    LOG.info("Processed URL " + url +
        " (Took " + (endTime - startTime) + " ms.)");
    return entity;
  }

  /**
   * Return a list of flow runs for given UID which is a delimited string
   * containing clusterid, userid and flow name.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param uId a delimited string containing clusterid, userid, and flow name
   *     which are extracted from UID and then used to query backend(Mandatory
   *     path param).
   * @param flowName Flow name to which the flow runs to be queried belongs to(
   *     Mandatory path param).
   * @param limit Number of flow runs to return(Optional query param).
   * @param createdTimeStart If specified, matched flow runs should not be
   *     created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched flow runs should not be created
   *     after this timestamp(Optional query param).
   * @param fields Specifies which fields to retrieve, see {@link Field}.
   *     All fields will be retrieved if fields=ALL. Fields other than METRICS
   *     have no meaning for this REST endpoint. If not specified, all fields
   *     other than metrics are returned(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     set of {@link FlowRunEntity} instances for the given flow are returned.
   *     On failures,
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/flow-uid/{uid}/runs/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowRuns(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("uid") String uId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.FLOW_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      entities = timelineReaderManager.getEntities(context.getUserId(),
          context.getClusterId(), context.getFlowName(), null, null,
          TimelineEntityType.YARN_FLOW_RUN.toString(),
          TimelineReaderWebServicesUtils.parseLongStr(limit),
          TimelineReaderWebServicesUtils.parseLongStr(createdTimeStart),
          TimelineReaderWebServicesUtils.parseLongStr(createdTimeEnd),
          null, null, null, null, null, null, TimelineReaderWebServicesUtils.
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
   * Return a set of flows runs for the given user and flow name.
   * Cluster ID is not provided by client so default cluster ID has to be taken.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param userId User id representing the user who ran the flow runs(
   *     Mandatory path param)
   * @param flowName Flow name to which the flow runs to be queried belongs to(
   *     Mandatory path param).
   * @param limit Number of flow runs to return(Optional query param).
   * @param createdTimeStart If specified, matched flow runs should not be
   *     created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched flow runs should not be created
   *     after this timestamp(Optional query param).
   * @param fields Specifies which fields to retrieve, see {@link Field}.
   *     All fields will be retrieved if fields=ALL. Fields other than METRICS
   *     have no meaning for this REST endpoint. If not specified, all fields
   *     other than metrics are returned(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     set of {@link FlowRunEntity} instances for the given flow are returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/users/{userid}/flows/{flowname}/runs/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowRuns(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("fields") String fields) {
    return getFlowRuns(req, res, null, userId, flowName, limit,
        createdTimeStart, createdTimeEnd, fields);
  }

  /**
   * Return a set of flows runs for the given cluster, user and flow name.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the flow runs to be queried belong to(
   *     Mandatory path param).
   * @param userId User id representing the user who ran the flow runs(
   *     Mandatory path param)
   * @param flowName Flow name to which the flow runs to be queried belongs to(
   *     Mandatory path param).
   * @param limit Number of flow runs to return(Optional query param).
   * @param createdTimeStart If specified, matched flow runs should not be
   *     created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched flow runs should not be created
   *     after this timestamp(Optional query param).
   * @param fields Specifies which fields to retrieve, see {@link Field}.
   *     All fields will be retrieved if fields=ALL. Fields other than METRICS
   *     have no meaning for this REST endpoint. If not specified, all fields
   *     other than metrics are returned(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     set of {@link FlowRunEntity} instances for the given flow are returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/users/{userid}/flows/{flowname}/runs/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowRuns(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      entities = timelineReaderManager.getEntities(
          TimelineReaderWebServicesUtils.parseStr(userId),
          TimelineReaderWebServicesUtils.parseStr(clusterId),
          TimelineReaderWebServicesUtils.parseStr(flowName), null, null,
          TimelineEntityType.YARN_FLOW_RUN.toString(),
          TimelineReaderWebServicesUtils.parseLongStr(limit),
          TimelineReaderWebServicesUtils.parseLongStr(createdTimeStart),
          TimelineReaderWebServicesUtils.parseLongStr(createdTimeEnd),
          null, null, null, null, null, null, TimelineReaderWebServicesUtils.
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
   * Return a list of active flows. Cluster ID is not provided by client so
   * default cluster ID has to be taken.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param limit Number of flows to return(Optional query param).
   * @param dateRange If specified is given as "[startdate]-[enddate]"(i.e.
   *     start and end date separated by "-") or single date. Dates are
   *     interpreted in yyyyMMdd format and are assumed to be in GMT(Optional
   *     query param).
   *     If a single date is specified, all flows active on that date are
   *     returned. If both startdate and enddate is given, all flows active
   *     between start and end date will be returned. If only startdate is
   *     given, flows active on and after startdate are returned. If only
   *     enddate is given, flows active on and before enddate are returned.
   *     For example :
   *     "daterange=20150711" returns flows active on 20150711.
   *     "daterange=20150711-20150714" returns flows active between these
   *     2 dates.
   *     "daterange=20150711-" returns flows active on and after 20150711.
   *     "daterange=-20150711" returns flows active on and before 20150711.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     set of {@link FlowActivityEntity} instances are returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/flows/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlows(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @QueryParam("limit") String limit,
      @QueryParam("daterange") String dateRange) {
    return getFlows(req, res, null, limit, dateRange);
  }

  /**
   * Return a list of active flows for a given cluster id.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the flows to be queried belong to(
   *     Mandatory path param).
   * @param limit Number of flows to return(Optional query param).
   * @param dateRange If specified is given as "[startdate]-[enddate]"(i.e.
   *     start and end date separated by "-") or single date. Dates are
   *     interpreted in yyyyMMdd format and are assumed to be in GMT(Optional
   *     query param).
   *     If a single date is specified, all flows active on that date are
   *     returned. If both startdate and enddate is given, all flows active
   *     between start and end date will be returned. If only startdate is
   *     given, flows active on and after startdate are returned. If only
   *     enddate is given, flows active on and before enddate are returned.
   *     For example :
   *     "daterange=20150711" returns flows active on 20150711.
   *     "daterange=20150711-20150714" returns flows active between these
   *     2 dates.
   *     "daterange=20150711-" returns flows active on and after 20150711.
   *     "daterange=-20150711" returns flows active on and before 20150711.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     set of {@link FlowActivityEntity} instances are returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/flows/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlows(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @QueryParam("limit") String limit,
      @QueryParam("daterange") String dateRange) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      DateRange range = parseDateRange(dateRange);
      entities = timelineReaderManager.getEntities(
          null, TimelineReaderWebServicesUtils.parseStr(clusterId), null, null,
          null, TimelineEntityType.YARN_FLOW_ACTIVITY.toString(),
          TimelineReaderWebServicesUtils.parseLongStr(limit), range.dateStart,
          range.dateEnd, null, null, null, null, null, null, null);
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
   * Return a single app for given UID which is a delimited string containing
   * clusterid, userid, flow name, flowrun id and app id.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param uId a delimited string containing clusterid, userid, flow name, flow
   *     run id and app id which are extracted from UID and then used to query
   *     backend(Mandatory path param).
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     {@link TimelineEntity} instance is returned.
   *     On failures,
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.
   *     If app for the given app id cannot be found, HTTP 404(Not Found) is
   *     returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/app-uid/{uid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getApp(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("uid") String uId,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.APPLICATION_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      entity = timelineReaderManager.getEntity(context.getUserId(),
          context.getClusterId(), context.getFlowName(), context.getFlowRunId(),
          context.getAppId(), TimelineEntityType.YARN_APPLICATION.toString(),
          null, TimelineReaderWebServicesUtils.parseFieldsStr(
          fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime, "flowrunid");
    }
    long endTime = Time.monotonicNow();
    if (entity == null) {
      LOG.info("Processed URL " + url + " but app not found" + " (Took " +
          (endTime - startTime) + " ms.)");
      throw new NotFoundException("App with uid " + uId + " not found");
    }
    LOG.info("Processed URL " + url +
        " (Took " + (endTime - startTime) + " ms.)");
    return entity;
  }

  /**
   * Return a single app for given app id. Cluster ID is not provided by client
   * client so default cluster ID has to be taken. If userid, flow name and flow
   * run id which are optional query parameters are not specified, they will be
   * queried based on app id and cluster id from the flow context information
   * stored in underlying storage implementation.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param appId Application id to be queried(Mandatory path param).
   * @param flowName Flow name which should match for the app(Optional query
   *     param).
   * @param flowRunId Run id which should match for the app(Optional query
   *     param).
   * @param userId User id which should match for the app(Optional query param).
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     {@link TimelineEntity} instance is returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     If flow context information cannot be retrieved or app for the given
   *     app id cannot be found, HTTP 404(Not Found) is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/apps/{appid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getApp(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("appid") String appId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("userid") String userId,
      @QueryParam("fields") String fields) {
    return getApp(req, res, null, appId, flowName, flowRunId, userId, fields);
  }

  /**
   * Return a single app for given cluster id and app id. If userid, flow name
   * and flowrun id which are optional query parameters are not specified, they
   * will be queried based on app id and cluster id from the flow context
   * information stored in underlying storage implementation.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the app to be queried belong to(
   *     Mandatory path param).
   * @param appId Application id to be queried(Mandatory path param).
   * @param flowName Flow name which should match for the app(Optional query
   *     param).
   * @param flowRunId Run id which should match for the app(Optional query
   *     param).
   * @param userId User id which should match for the app(Optional query param).
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     {@link TimelineEntity} instance is returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     If flow context information cannot be retrieved or app for the given
   *     app id cannot be found, HTTP 404(Not Found) is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getApp(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("userid") String userId,
      @QueryParam("fields") String fields) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(
          TimelineReaderWebServicesUtils.parseStr(userId),
          TimelineReaderWebServicesUtils.parseStr(clusterId),
          TimelineReaderWebServicesUtils.parseStr(flowName),
          TimelineReaderWebServicesUtils.parseLongStr(flowRunId),
          TimelineReaderWebServicesUtils.parseStr(appId),
          TimelineEntityType.YARN_APPLICATION.toString(), null,
          TimelineReaderWebServicesUtils.parseFieldsStr(
          fields, COMMA_DELIMITER));
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
   * Return a list of apps for given UID which is a delimited string containing
   * clusterid, userid, flow name and flowrun id. If number of matching apps are
   * more than the limit, most recent apps till the limit is reached, will be
   * returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param uId a delimited string containing clusterid, userid, flow name and
   *     flowrun id which are extracted from UID and then used to query backend.
   *     (Mandatory path param).
   * @param limit Number of apps to return(Optional query param).
   * @param createdTimeStart If specified, matched apps should not be created
   *     before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched apps should not be created
   *     after this timestamp(Optional query param).
   * @param relatesTo If specified, matched apps should relate to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param isRelatedTo If specified, matched apps should be related to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param infofilters If specified, matched apps should have exact matches
   *     to the given info represented as key-value pairs. This is represented
   *     as infofilters=info1:value1,info2:value2... (Optional query param).
   * @param conffilters If specified, matched apps should have exact matches
   *     to the given configs represented as key-value pairs. This is
   *     represented as conffilters=conf1:value1,conf2:value2... (Optional query
   *     param).
   * @param metricfilters If specified, matched apps should contain the given
   *     metrics. This is represented as metricfilters=metricid1, metricid2...
   *     (Optional query param).
   * @param eventfilters If specified, matched apps should contain the given
   *     events. This is represented as eventfilters=eventid1, eventid2...
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of {@link TimelineEntity} instances representing apps is
   *     returned.
   *     On failures,
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/run-uid/{uid}/apps")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowRunApps(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("uid") String uId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
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
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.FLOWRUN_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      entities = timelineReaderManager.getEntities(
          TimelineReaderWebServicesUtils.parseStr(context.getUserId()),
          TimelineReaderWebServicesUtils.parseStr(context.getClusterId()),
          TimelineReaderWebServicesUtils.parseStr(context.getFlowName()),
          context.getFlowRunId(),
          TimelineReaderWebServicesUtils.parseStr(context.getAppId()),
          TimelineEntityType.YARN_APPLICATION.toString(),
          TimelineReaderWebServicesUtils.parseLongStr(limit),
          TimelineReaderWebServicesUtils.parseLongStr(createdTimeStart),
          TimelineReaderWebServicesUtils.parseLongStr(createdTimeEnd),
          TimelineReaderWebServicesUtils.parseKeyStrValuesStr(
          relatesTo, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseKeyStrValuesStr(
          isRelatedTo, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseKeyStrValueObj(
          infofilters, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseKeyStrValueStr(
          conffilters, COMMA_DELIMITER, COLON_DELIMITER),
          TimelineReaderWebServicesUtils.parseValuesStr(
          metricfilters, COMMA_DELIMITER),
          TimelineReaderWebServicesUtils.parseValuesStr(
          eventfilters, COMMA_DELIMITER),
          TimelineReaderWebServicesUtils.parseFieldsStr(
          fields, COMMA_DELIMITER));
    } catch (Exception e) {
      handleException(e, url, startTime,
          "createdTime start/end or limit or flowrunid");
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
   * Return a list of apps for given user, flow name and flow run id. Cluster ID
   * is not provided by client so default cluster ID has to be taken. If number
   * of matching apps are more than the limit, most recent apps till the limit
   * is reached, will be returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param userId User id which should match for the apps(Mandatory path param)
   * @param flowName Flow name which should match for the apps(Mandatory path
   *     param).
   * @param flowRunId Run id which should match for the apps(Mandatory path
   *     param).
   * @param limit Number of apps to return(Optional query param).
   * @param createdTimeStart If specified, matched apps should not be created
   *     before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched apps should not be created
   *     after this timestamp(Optional query param).
   * @param relatesTo If specified, matched apps should relate to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param isRelatedTo If specified, matched apps should be related to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param infofilters If specified, matched apps should have exact matches
   *     to the given info represented as key-value pairs. This is represented
   *     as infofilters=info1:value1,info2:value2... (Optional query param).
   * @param conffilters If specified, matched apps should have exact matches
   *     to the given configs represented as key-value pairs. This is
   *     represented as conffilters=conf1:value1,conf2:value2... (Optional query
   *     param).
   * @param metricfilters If specified, matched apps should contain the given
   *     metrics. This is represented as metricfilters=metricid1, metricid2...
   *     (Optional query param).
   * @param eventfilters If specified, matched apps should contain the given
   *     events. This is represented as eventfilters=eventid1, eventid2...
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of {@link TimelineEntity} instances representing apps is
   *     returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/users/{userid}/flows/{flowname}/runs/{flowrunid}/apps/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowRunApps(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @PathParam("flowrunid") String flowRunId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("fields") String fields) {
    return getEntities(req, res, null, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowName,
        flowRunId, limit, createdTimeStart, createdTimeEnd, relatesTo,
        isRelatedTo, infofilters, conffilters, metricfilters, eventfilters,
        fields);
  }

  /**
   * Return a list of apps for a given user, cluster id, flow name and flow run
   * id. If number of matching apps are more than the limit, most recent apps
   * till the limit is reached, will be returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the apps to be queried belong to
   *     (Mandatory path param).
   * @param userId User id which should match for the apps(Mandatory path param)
   * @param flowName Flow name which should match for the apps(Mandatory path
   *     param).
   * @param flowRunId Run id which should match for the apps(Mandatory path
   *     param).
   * @param limit Number of apps to return(Optional query param).
   * @param createdTimeStart If specified, matched apps should not be created
   *     before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched apps should not be created
   *     after this timestamp(Optional query param).
   * @param relatesTo If specified, matched apps should relate to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param isRelatedTo If specified, matched apps should be related to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param infofilters If specified, matched apps should have exact matches
   *     to the given info represented as key-value pairs. This is represented
   *     as infofilters=info1:value1,info2:value2... (Optional query param).
   * @param conffilters If specified, matched apps should have exact matches
   *     to the given configs represented as key-value pairs. This is
   *     represented as conffilters=conf1:value1,conf2:value2... (Optional query
   *     param).
   * @param metricfilters If specified, matched apps should contain the given
   *     metrics. This is represented as metricfilters=metricid1, metricid2...
   *     (Optional query param).
   * @param eventfilters If specified, matched apps should contain the given
   *     events. This is represented as eventfilters=eventid1, eventid2...
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of {@link TimelineEntity} instances representing apps is
   *     returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/users/{userid}/flows/{flowname}/runs/"
      + "{flowrunid}/apps/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowRunApps(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @PathParam("flowrunid") String flowRunId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("fields") String fields) {
    return getEntities(req, res, clusterId, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowName,
        flowRunId, limit, createdTimeStart, createdTimeEnd, relatesTo,
        isRelatedTo, infofilters, conffilters, metricfilters, eventfilters,
        fields);
  }

  /**
   * Return a list of apps for given user and flow name. Cluster ID is not
   * provided by client so default cluster ID has to be taken. If number of
   * matching apps are more than the limit, most recent apps till the limit is
   * reached, will be returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param userId User id which should match for the apps(Mandatory path param)
   * @param flowName Flow name which should match for the apps(Mandatory path
   *     param).
   * @param limit Number of apps to return(Optional query param).
   * @param createdTimeStart If specified, matched apps should not be created
   *     before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched apps should not be created
   *     after this timestamp(Optional query param).
   * @param relatesTo If specified, matched apps should relate to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param isRelatedTo If specified, matched apps should be related to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param infofilters If specified, matched apps should have exact matches
   *     to the given info represented as key-value pairs. This is represented
   *     as infofilters=info1:value1,info2:value2... (Optional query param).
   * @param conffilters If specified, matched apps should have exact matches
   *     to the given configs represented as key-value pairs. This is
   *     represented as conffilters=conf1:value1,conf2:value2... (Optional query
   *     param).
   * @param metricfilters If specified, matched apps should contain the given
   *     metrics. This is represented as metricfilters=metricid1, metricid2...
   *     (Optional query param).
   * @param eventfilters If specified, matched apps should contain the given
   *     events. This is represented as eventfilters=eventid1, eventid2...
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of {@link TimelineEntity} instances representing apps is
   *     returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/users/{userid}/flows/{flowname}/apps/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowApps(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("fields") String fields) {
    return getEntities(req, res, null, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowName,
        null, limit, createdTimeStart, createdTimeEnd, relatesTo, isRelatedTo,
        infofilters, conffilters, metricfilters, eventfilters, fields);
  }

  /**
   * Return a list of apps for a given user, cluster id and flow name. If number
   * of matching apps are more than the limit, most recent apps till the limit
   * is reached, will be returned. If number of matching apps are more than the
   * limit, most recent apps till the limit is reached, will be returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the apps to be queried belong to
   *     (Mandatory path param).
   * @param userId User id which should match for the apps(Mandatory path param)
   * @param flowName Flow name which should match for the apps(Mandatory path
   *     param).
   * @param limit Number of apps to return(Optional query param).
   * @param createdTimeStart If specified, matched apps should not be created
   *     before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched apps should not be created
   *     after this timestamp(Optional query param).
   * @param relatesTo If specified, matched apps should relate to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param isRelatedTo If specified, matched apps should be related to given
   *     entities associated with a entity type. relatesto is a comma separated
   *     list in the format [entitytype]:[entityid1]:[entityid2]... (Optional
   *     query param).
   * @param infofilters If specified, matched apps should have exact matches
   *     to the given info represented as key-value pairs. This is represented
   *     as infofilters=info1:value1,info2:value2... (Optional query param).
   * @param conffilters If specified, matched apps should have exact matches
   *     to the given configs represented as key-value pairs. This is
   *     represented as conffilters=conf1:value1,conf2:value2... (Optional query
   *     param).
   * @param metricfilters If specified, matched apps should contain the given
   *     metrics. This is represented as metricfilters=metricid1, metricid2...
   *     (Optional query param).
   * @param eventfilters If specified, matched apps should contain the given
   *     events. This is represented as eventfilters=eventid1, eventid2...
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 4 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of {@link TimelineEntity} instances representing apps is
   *     returned.
   *     On failures,
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/users/{userid}/flows/{flowname}/apps/")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getFlowApps(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("fields") String fields) {
    return getEntities(req, res, clusterId, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowName,
        null, limit, createdTimeStart, createdTimeEnd, relatesTo, isRelatedTo,
        infofilters, conffilters, metricfilters, eventfilters, fields);
  }
}