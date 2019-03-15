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
import java.util.LinkedHashSet;
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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.metrics.TimelineReaderMetrics;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST end point for Timeline Reader. */
@Private
@Unstable
@Singleton
@Path("/ws/v2/timeline")
public class TimelineReaderWebServices {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineReaderWebServices.class);

  @Context private ServletContext ctxt;

  private static final String QUERY_STRING_SEP = "?";
  private static final String RANGE_DELIMITER = "-";
  private static final String DATE_PATTERN = "yyyyMMdd";
  private static final TimelineReaderMetrics METRICS =
      TimelineReaderMetrics.getInstance();

  @VisibleForTesting
  static final ThreadLocal<DateFormat> DATE_FORMAT =
      new ThreadLocal<DateFormat>() {
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

  private static final class DateRange {
    private Long dateStart;
    private Long dateEnd;
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
    } else if (e instanceof TimelineParseException) {
      throw new BadRequestException(e.getMessage() == null ?
          "Filter Parsing failed." : e.getMessage());
    } else if (e instanceof BadRequestException) {
      throw (BadRequestException)e;
    } else if (e instanceof ForbiddenException) {
      throw (ForbiddenException) e;
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
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
   * @param entityType Type of entities(Mandatory path param).
   * @param limit If specified, defines the number of entities to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type, id and created time is returned
   *     (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the entities
   *     would not contain metric values before this timestamp(Optional query
   *     param).
   * @param metricsTimeEnd If specified, returned metrics for the entities would
   *     not contain metric values after this timestamp(Optional query param).
   * @param fromId If specified, retrieve the next set of entities from the
   *     given fromId. The set of entities retrieved is inclusive of specified
   *     fromId. fromId should be taken from the value associated with FROM_ID
   *     info key in entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of <cite>TimelineEntity</cite> instances of the given entity type
   *     is returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/app-uid/{uid}/entities/{entitytype}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.APPLICATION_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      context.setEntityType(
          TimelineReaderWebServicesUtils.parseStr(entityType));
      entities = timelineReaderManager.getEntities(context,
          TimelineReaderWebServicesUtils.createTimelineEntityFilters(
          limit, createdTimeStart, createdTimeEnd, relatesTo, isRelatedTo,
              infofilters, conffilters, metricfilters, eventfilters,
              fromId),
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
          confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
          metricsTimeStart, metricsTimeEnd));
      checkAccessForGenericEntities(entities, callerUGI, entityType);
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime,
          "Either limit or createdtime start/end or metricslimit or metricstime"
              + " start/end or fromid");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entities == null) {
      entities = Collections.emptySet();
    }
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
   * @param limit If specified, defines the number of entities to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type, id, created time is returned
   *     (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the entities
   *     would not contain metric values before this timestamp(Optional query
   *     param).
   * @param metricsTimeEnd If specified, returned metrics for the entities would
   *     not contain metric values after this timestamp(Optional query param).
   * @param fromId If specified, retrieve the next set of entities from the
   *     given fromId. The set of entities retrieved is inclusive of specified
   *     fromId. fromId should be taken from the value associated with FROM_ID
   *     info key in entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of <cite>TimelineEntity</cite> instances of the given entity type
   *     is returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     If flow context information cannot be retrieved, HTTP 404(Not Found)
   *     is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/apps/{appid}/entities/{entitytype}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    return getEntities(req, res, null, appId, entityType, userId, flowName,
        flowRunId, limit, createdTimeStart, createdTimeEnd, relatesTo,
        isRelatedTo, infofilters, conffilters, metricfilters, eventfilters,
        confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd, fromId);
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
   * @param limit If specified, defines the number of entities to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type, id, created time is returned
   *     (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the entities
   *     would not contain metric values before this timestamp(Optional query
   *     param).
   * @param metricsTimeEnd If specified, returned metrics for the entities would
   *     not contain metric values after this timestamp(Optional query param).
   * @param fromId If specified, retrieve the next set of entities from the
   *     given fromId. The set of entities retrieved is inclusive of specified
   *     fromId. fromId should be taken from the value associated with FROM_ID
   *     info key in entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of <cite>TimelineEntity</cite> instances of the given entity type
   *     is returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     If flow context information cannot be retrieved, HTTP 404(Not Found)
   *     is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/entities/{entitytype}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext context = TimelineReaderWebServicesUtils
          .createTimelineReaderContext(clusterId, userId, flowName, flowRunId,
              appId, entityType, null, null);
      entities = timelineReaderManager.getEntities(context,
          TimelineReaderWebServicesUtils
              .createTimelineEntityFilters(limit, createdTimeStart,
                  createdTimeEnd, relatesTo, isRelatedTo, infofilters,
                  conffilters, metricfilters, eventfilters, fromId),
          TimelineReaderWebServicesUtils
              .createTimelineDataToRetrieve(confsToRetrieve, metricsToRetrieve,
                  fields, metricsLimit, metricsTimeStart, metricsTimeEnd));

      checkAccessForGenericEntities(entities, callerUGI, entityType);
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime,
          "Either flowrunid or limit or createdtime start/end or metricslimit"
              + " or metricstime start/end or fromid");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entities == null) {
      entities = Collections.emptySet();
    }
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type, id, created time is returned
   *     (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the entity would
   *     not contain metric values before this timestamp(Optional query param).
   * @param metricsTimeEnd If specified, returned metrics for the entity would
   *     not contain metric values after this timestamp(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     <cite>TimelineEntity</cite> instance is returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.<br>
   *     If entity for the given entity id cannot be found, HTTP 404(Not Found)
   *     is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/entity-uid/{uid}/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("uid") String uId,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.GENERIC_ENTITY_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      entity = timelineReaderManager.getEntity(context,
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
          confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
          metricsTimeStart, metricsTimeEnd));
      checkAccessForGenericEntity(entity, callerUGI);
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime, "Either metricslimit or metricstime"
          + " start/end");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entity == null) {
      LOG.info("Processed URL " + url + " but entity not found" + " (Took " +
          (Time.monotonicNow() - startTime) + " ms.)");
      throw new NotFoundException("Timeline entity with uid: " + uId +
          "is not found");
    }
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type, id, created time is returned
   *     (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the entity would
   *     not contain metric values before this timestamp(Optional query param).
   * @param metricsTimeEnd If specified, returned metrics for the entity would
   *     not contain metric values after this timestamp(Optional query param).
   * @param entityIdPrefix Defines the id prefix for the entity to be fetched.
   *     If specified, then entity retrieval will be faster.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     <cite>TimelineEntity</cite> instance is returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     If flow context information cannot be retrieved or entity for the given
   *     entity id cannot be found, HTTP 404(Not Found) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/apps/{appid}/entities/{entitytype}/{entityid}/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("appid") String appId,
      @PathParam("entitytype") String entityType,
      @PathParam("entityid") String entityId,
      @QueryParam("userid") String userId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("entityidprefix") String entityIdPrefix) {
    return getEntity(req, res, null, appId, entityType, entityId, userId,
        flowName, flowRunId, confsToRetrieve, metricsToRetrieve, fields,
        metricsLimit, metricsTimeStart, metricsTimeEnd, entityIdPrefix);
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *     {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type, id and created time is returned
   *     (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the entity would
   *     not contain metric values before this timestamp(Optional query param).
   * @param metricsTimeEnd If specified, returned metrics for the entity would
   *     not contain metric values after this timestamp(Optional query param).
   * @param entityIdPrefix Defines the id prefix for the entity to be fetched.
   *     If specified, then entity retrieval will be faster.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     <cite>TimelineEntity</cite> instance is returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     If flow context information cannot be retrieved or entity for the given
   *     entity id cannot be found, HTTP 404(Not Found) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/entities/{entitytype}/{entityid}/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("entityidprefix") String entityIdPrefix) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(
          TimelineReaderWebServicesUtils.createTimelineReaderContext(
              clusterId, userId, flowName, flowRunId, appId, entityType,
              entityIdPrefix, entityId),
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
          confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
          metricsTimeStart, metricsTimeEnd));
      checkAccessForGenericEntity(entity, callerUGI);
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime, "Either flowrunid or metricslimit or"
          + " metricstime start/end");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entity == null) {
      LOG.info("Processed URL " + url + " but entity not found" + " (Took " +
          (Time.monotonicNow() - startTime) + " ms.)");
      throw new NotFoundException("Timeline entity {id: " + entityId +
          ", type: " + entityType + " } is not found");
    }
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
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     <cite>FlowRunEntity</cite> instance is returned. By default, all
   *     metrics for the flow run will be returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.<br>
   *     If flow run for the given flow run id cannot be found, HTTP 404
   *     (Not Found) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/run-uid/{uid}/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public TimelineEntity getFlowRun(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("uid") String uId,
      @QueryParam("metricstoretrieve") String metricsToRetrieve) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.FLOWRUN_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      // TODO to be removed or modified once ACL story is played
      checkAccess(timelineReaderManager, callerUGI, context.getUserId());
      context.setEntityType(TimelineEntityType.YARN_FLOW_RUN.toString());
      entity = timelineReaderManager.getEntity(context,
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
          null, metricsToRetrieve, null, null, null, null));
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime, "flowrunid");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entity == null) {
      LOG.info("Processed URL " + url + " but flowrun not found (Took " +
          (Time.monotonicNow() - startTime) + " ms.)");
      throw new NotFoundException("Flowrun with uid: " + uId + "is not found");
    }
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
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     <cite>FlowRunEntity</cite> instance is returned. By default, all
   *     metrics for the flow run will be returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     If flow run for the given flow run id cannot be found, HTTP 404
   *     (Not Found) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/users/{userid}/flows/{flowname}/runs/{flowrunid}/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public TimelineEntity getFlowRun(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @PathParam("flowrunid") String flowRunId,
      @QueryParam("metricstoretrieve") String metricsToRetrieve) {
    return getFlowRun(req, res, null, userId, flowName, flowRunId,
        metricsToRetrieve);
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
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     <cite>FlowRunEntity</cite> instance is returned. By default, all
   *     metrics for the flow run will be returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     If flow run for the given flow run id cannot be found, HTTP 404
   *     (Not Found) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/users/{userid}/flows/{flowname}/"
      + "runs/{flowrunid}/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public TimelineEntity getFlowRun(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @PathParam("flowrunid") String flowRunId,
      @QueryParam("metricstoretrieve") String metricsToRetrieve) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      TimelineReaderContext context = TimelineReaderWebServicesUtils
          .createTimelineReaderContext(clusterId, userId, flowName, flowRunId,
              null, TimelineEntityType.YARN_FLOW_RUN.toString(), null, null);
      // TODO to be removed or modified once ACL story is played
      checkAccess(timelineReaderManager, callerUGI, context.getUserId());

      entity = timelineReaderManager.getEntity(context,
          TimelineReaderWebServicesUtils
              .createTimelineDataToRetrieve(null, metricsToRetrieve, null, null,
                  null, null));
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime, "flowrunid");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entity == null) {
      LOG.info("Processed URL " + url + " but flowrun not found (Took " +
          (Time.monotonicNow() - startTime) + " ms.)");
      throw new NotFoundException("Flow run {flow name: " +
          TimelineReaderWebServicesUtils.parseStr(flowName) + ", run id: " +
          TimelineReaderWebServicesUtils.parseLongStr(flowRunId) +
          " } is not found");
    }
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
   * @param limit If specified, defines the number of flow runs to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
   * @param createdTimeStart If specified, matched flow runs should not be
   *     created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched flow runs should not be created
   *     after this timestamp(Optional query param).
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields to retrieve, see {@link Field}. All
   *     fields will be retrieved if fields=ALL. Amongst all the fields, only
   *     METRICS makes sense for flow runs hence only ALL or METRICS are
   *     supported as fields for fetching flow runs. Other fields will lead to
   *     HTTP 400 (Bad Request) response. (Optional query param).
   * @param fromId If specified, retrieve the next set of flow run entities
   *     from the given fromId. The set of entities retrieved is inclusive of
   *     specified fromId. fromId should be taken from the value associated
   *     with FROM_ID info key in entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     set of <cite>FlowRunEntity</cite> instances for the given flow are
   *     returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/flow-uid/{uid}/runs/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getFlowRuns(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("uid") String uId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("fromid") String fromId) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.FLOW_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      // TODO to be removed or modified once ACL story is played
      checkAccess(timelineReaderManager, callerUGI, context.getUserId());
      context.setEntityType(TimelineEntityType.YARN_FLOW_RUN.toString());
      entities = timelineReaderManager.getEntities(context,
          TimelineReaderWebServicesUtils.createTimelineEntityFilters(
          limit, createdTimeStart, createdTimeEnd, null, null, null,
              null, null, null, fromId),
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
          null, metricsToRetrieve, fields, null, null, null));
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime,
          "createdTime start/end or limit or fromId");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entities == null) {
      entities = Collections.emptySet();
    }
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
   * @param limit If specified, defines the number of flow runs to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
   * @param createdTimeStart If specified, matched flow runs should not be
   *     created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched flow runs should not be created
   *     after this timestamp(Optional query param).
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields to retrieve, see {@link Field}. All
   *     fields will be retrieved if fields=ALL. Amongst all the fields, only
   *     METRICS makes sense for flow runs hence only ALL or METRICS are
   *     supported as fields for fetching flow runs. Other fields will lead to
   *     HTTP 400 (Bad Request) response. (Optional query param).
   * @param fromId If specified, retrieve the next set of flow run entities
   *     from the given fromId. The set of entities retrieved is inclusive of
   *     specified fromId. fromId should be taken from the value associated
   *     with FROM_ID info key in entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     set of <cite>FlowRunEntity</cite> instances for the given flow are
   *     returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/users/{userid}/flows/{flowname}/runs/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getFlowRuns(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("fromid") String fromId) {
    return getFlowRuns(req, res, null, userId, flowName, limit,
        createdTimeStart, createdTimeEnd, metricsToRetrieve, fields, fromId);
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
   * @param limit If specified, defines the number of flow runs to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
   * @param createdTimeStart If specified, matched flow runs should not be
   *     created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched flow runs should not be created
   *     after this timestamp(Optional query param).
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields to retrieve, see {@link Field}. All
   *     fields will be retrieved if fields=ALL. Amongst all the fields, only
   *     METRICS makes sense for flow runs hence only ALL or METRICS are
   *     supported as fields for fetching flow runs. Other fields will lead to
   *     HTTP 400 (Bad Request) response. (Optional query param).
   * @param fromId If specified, retrieve the next set of flow run entities
   *     from the given fromId. The set of entities retrieved is inclusive of
   *     specified fromId. fromId should be taken from the value associated
   *     with FROM_ID info key in entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     set of <cite>FlowRunEntity</cite> instances for the given flow are
   *     returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/users/{userid}/flows/{flowname}/runs/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getFlowRuns(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("userid") String userId,
      @PathParam("flowname") String flowName,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("fromid") String fromId) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext timelineReaderContext = TimelineReaderWebServicesUtils
          .createTimelineReaderContext(clusterId, userId, flowName, null,
              null, TimelineEntityType.YARN_FLOW_RUN.toString(), null,
              null);
      // TODO to be removed or modified once ACL story is played
      checkAccess(timelineReaderManager, callerUGI,
          timelineReaderContext.getUserId());

      entities = timelineReaderManager.getEntities(timelineReaderContext,
          TimelineReaderWebServicesUtils
              .createTimelineEntityFilters(limit, createdTimeStart,
                  createdTimeEnd, null, null, null, null, null, null, fromId),
          TimelineReaderWebServicesUtils
              .createTimelineDataToRetrieve(null, metricsToRetrieve, fields,
                  null, null, null));
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime,
          "createdTime start/end or limit or fromId");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entities == null) {
      entities = Collections.emptySet();
    }
    return entities;
  }

  /**
   * Return a list of active flows. Cluster ID is not provided by client so
   * default cluster ID has to be taken.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param limit If specified, defines the number of flows to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
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
   * @param fromId If specified, retrieve the next set of flows from the given
   *     fromId. The set of flows retrieved is inclusive of specified fromId.
   *     fromId should be taken from the value associated with FROM_ID info key
   *     in flow entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     set of <cite>FlowActivityEntity</cite> instances are returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.<br>
   */
  @GET
  @Path("/flows/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getFlows(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @QueryParam("limit") String limit,
      @QueryParam("daterange") String dateRange,
      @QueryParam("fromid") String fromId) {
    return getFlows(req, res, null, limit, dateRange, fromId);
  }

  /**
   * Return a list of active flows for a given cluster id.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the flows to be queried belong to(
   *     Mandatory path param).
   * @param limit If specified, defines the number of flows to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
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
   * @param fromId If specified, retrieve the next set of flows from the given
   *     fromId. The set of flows retrieved is inclusive of specified fromId.
   *     fromId should be taken from the value associated with FROM_ID info key
   *     in flow entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     set of <cite>FlowActivityEntity</cite> instances are returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/flows/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getFlows(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @QueryParam("limit") String limit,
      @QueryParam("daterange") String dateRange,
      @QueryParam("fromid") String fromId) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      DateRange range = parseDateRange(dateRange);
      TimelineEntityFilters entityFilters =
          TimelineReaderWebServicesUtils.createTimelineEntityFilters(
              limit, range.dateStart, range.dateEnd,
              null, null, null, null, null, null, fromId);
      entities = timelineReaderManager.getEntities(
          TimelineReaderWebServicesUtils.createTimelineReaderContext(
          clusterId, null, null, null, null,
              TimelineEntityType.YARN_FLOW_ACTIVITY.toString(), null, null),
          entityFilters, TimelineReaderWebServicesUtils.
              createTimelineDataToRetrieve(null, null, null, null, null, null));
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime, "limit");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entities == null) {
      entities = Collections.emptySet();
    } else {
      checkAccess(timelineReaderManager, callerUGI, entities,
          FlowActivityEntity.USER_INFO_KEY, true);
    }
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the apps would
   *     not contain metric values before this timestamp(Optional query param).
   * @param metricsTimeEnd If specified, returned metrics for the apps would
   *     not contain metric values after this timestamp(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     <cite>TimelineEntity</cite> instance is returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.<br>
   *     If app for the given app id cannot be found, HTTP 404(Not Found) is
   *     returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/app-uid/{uid}/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public TimelineEntity getApp(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("uid") String uId,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.APPLICATION_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      context.setEntityType(TimelineEntityType.YARN_APPLICATION.toString());
      entity = timelineReaderManager.getEntity(context,
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
          confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
          metricsTimeStart, metricsTimeEnd));
      checkAccessForAppEntity(entity, callerUGI);
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime, "Either metricslimit or metricstime"
          + " start/end");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entity == null) {
      LOG.info("Processed URL " + url + " but app not found" + " (Took " +
          (Time.monotonicNow() - startTime) + " ms.)");
      throw new NotFoundException("App with uid " + uId + " not found");
    }
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the app would
   *     not contain metric values before this timestamp(Optional query param).
   * @param metricsTimeEnd If specified, returned metrics for the app would
   *     not contain metric values after this timestamp(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     <cite>TimelineEntity</cite> instance is returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     If flow context information cannot be retrieved or app for the given
   *     app id cannot be found, HTTP 404(Not Found) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/apps/{appid}/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public TimelineEntity getApp(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("appid") String appId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("userid") String userId,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd) {
    return getApp(req, res, null, appId, flowName, flowRunId, userId,
        confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd);
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the app would
   *     not contain metric values before this timestamp(Optional query param).
   * @param metricsTimeEnd If specified, returned metrics for the app would
   *     not contain metric values after this timestamp(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     <cite>TimelineEntity</cite> instance is returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     If flow context information cannot be retrieved or app for the given
   *     app id cannot be found, HTTP 404(Not Found) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public TimelineEntity getApp(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("userid") String userId,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    TimelineEntity entity = null;
    try {
      entity = timelineReaderManager.getEntity(
          TimelineReaderWebServicesUtils.createTimelineReaderContext(
          clusterId, userId, flowName, flowRunId, appId,
              TimelineEntityType.YARN_APPLICATION.toString(), null, null),
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
          confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
          metricsTimeStart, metricsTimeEnd));
      checkAccessForAppEntity(entity, callerUGI);
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime, "Either flowrunid or metricslimit or"
          + " metricstime start/end");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entity == null) {
      LOG.info("Processed URL " + url + " but app not found" + " (Took " +
          (Time.monotonicNow() - startTime) + " ms.)");
      throw new NotFoundException("App " + appId + " not found");
    }
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
   * @param limit If specified, defines the number of apps to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the apps would
   *     not contain metric values before this timestamp(Optional query param).
   * @param metricsTimeEnd If specified, returned metrics for the apps would
   *     not contain metric values after this timestamp(Optional query param).
   * @param fromId If specified, retrieve the next set of applications
   *     from the given fromId. The set of applications retrieved is inclusive
   *     of specified fromId. fromId should be taken from the value associated
   *     with FROM_ID info key in entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of <cite>TimelineEntity</cite> instances representing apps is
   *     returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request or UID is incorrect,
   *     HTTP 400(Bad Request) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/run-uid/{uid}/apps")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext context =
          TimelineUIDConverter.FLOWRUN_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      // TODO to be removed or modified once ACL story is played
      checkAccess(timelineReaderManager, callerUGI, context.getUserId());
      context.setEntityType(TimelineEntityType.YARN_APPLICATION.toString());
      entities = timelineReaderManager.getEntities(context,
          TimelineReaderWebServicesUtils.createTimelineEntityFilters(
          limit, createdTimeStart, createdTimeEnd, relatesTo, isRelatedTo,
              infofilters, conffilters, metricfilters, eventfilters,
              fromId),
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
          confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
          metricsTimeStart, metricsTimeEnd));
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime,
          "Either limit or createdtime start/end or metricslimit or"
              + " metricstime start/end");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entities == null) {
      entities = Collections.emptySet();
    }
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
   * @param limit If specified, defines the number of apps to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the apps would
   *     not contain metric values before this timestamp(Optional query param).
   * @param metricsTimeEnd If specified, returned metrics for the apps would
   *     not contain metric values after this timestamp(Optional query param).
   * @param fromId If specified, retrieve the next set of applications
   *     from the given fromId. The set of applications retrieved is inclusive
   *     of specified fromId. fromId should be taken from the value associated
   *     with FROM_ID info key in entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of <cite>TimelineEntity</cite> instances representing apps is
   *     returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/users/{userid}/flows/{flowname}/runs/{flowrunid}/apps/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    return getEntities(req, res, null, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowName,
        flowRunId, limit, createdTimeStart, createdTimeEnd, relatesTo,
        isRelatedTo, infofilters, conffilters, metricfilters, eventfilters,
        confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd, fromId);
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
   * @param limit If specified, defines the number of apps to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the apps would
   *     not contain metric values before this timestamp(Optional query param).
   * @param metricsTimeEnd If specified, returned metrics for the apps would
   *     not contain metric values after this timestamp(Optional query param).
   * @param fromId If specified, retrieve the next set of applications
   *     from the given fromId. The set of applications retrieved is inclusive
   *     of specified fromId. fromId should be taken from the value associated
   *     with FROM_ID info key in entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of <cite>TimelineEntity</cite> instances representing apps is
   *     returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/users/{userid}/flows/{flowname}/runs/"
      + "{flowrunid}/apps/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    return getEntities(req, res, clusterId, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowName,
        flowRunId, limit, createdTimeStart, createdTimeEnd, relatesTo,
        isRelatedTo, infofilters, conffilters, metricfilters, eventfilters,
        confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd, fromId);
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
   * @param limit If specified, defines the number of apps to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the apps would
   *     not contain metric values before this timestamp(Optional query param).
   * @param metricsTimeEnd If specified, returned metrics for the apps would
   *     not contain metric values after this timestamp(Optional query param).
   * @param fromId If specified, retrieve the next set of applications
   *     from the given fromId. The set of applications retrieved is inclusive
   *     of specified fromId. fromId should be taken from the value associated
   *     with FROM_ID info key in entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of <cite>TimelineEntity</cite> instances representing apps is
   *     returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/users/{userid}/flows/{flowname}/apps/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    return getEntities(req, res, null, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowName,
        null, limit, createdTimeStart, createdTimeEnd, relatesTo, isRelatedTo,
        infofilters, conffilters, metricfilters, eventfilters,
        confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd, fromId);
  }

  /**
   * Return a list of apps for a given user, cluster id and flow name. If number
   * of matching apps are more than the limit, most recent apps till the limit
   * is reached, will be returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the apps to be queried belong to
   *     (Mandatory path param).
   * @param userId User id which should match for the apps(Mandatory path param)
   * @param flowName Flow name which should match for the apps(Mandatory path
   *     param).
   * @param limit If specified, defines the number of apps to return. The
   *     maximum possible value for limit can be {@link Long#MAX_VALUE}. If it
   *     is not specified or has a value less than 0, then limit will be
   *     considered as 100. (Optional query param).
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
   * @param confsToRetrieve If specified, defines which configurations to
   *     retrieve and send back in response. These configs will be retrieved
   *     irrespective of whether configs are specified in fields to retrieve or
   *     not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *     and send back in response. These metrics will be retrieved
   *     irrespective of whether metrics are specified in fields to retrieve or
   *     not.
   * @param fields Specifies which fields of the app entity object to retrieve,
   *     see {@link Field}. All fields will be retrieved if fields=ALL. If not
   *     specified, 3 fields i.e. entity type(equivalent to YARN_APPLICATION),
   *     app id and app created time is returned(Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *     Considered only if fields contains METRICS/ALL or metricsToRetrieve is
   *     specified. Ignored otherwise. The maximum possible value for
   *     metricsLimit can be {@link Integer#MAX_VALUE}. If it is not specified
   *     or has a value less than 1, and metrics have to be retrieved, then
   *     metricsLimit will be considered as 1 i.e. latest single value of
   *     metric(s) will be returned. (Optional query param).
   * @param metricsTimeStart If specified, returned metrics for the apps would
   *     not contain metric values before this timestamp(Optional query param).
   * @param metricsTimeEnd If specified, returned metrics for the apps would
   *     not contain metric values after this timestamp(Optional query param).
   * @param fromId If specified, retrieve the next set of applications
   *     from the given fromId. The set of applications retrieved is inclusive
   *     of specified fromId. fromId should be taken from the value associated
   *     with FROM_ID info key in entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *     a set of <cite>TimelineEntity</cite> instances representing apps is
   *     returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/users/{userid}/flows/{flowname}/apps/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    return getEntities(req, res, clusterId, null,
        TimelineEntityType.YARN_APPLICATION.toString(), userId, flowName,
        null, limit, createdTimeStart, createdTimeEnd, relatesTo, isRelatedTo,
        infofilters, conffilters, metricfilters, eventfilters,
        confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd, fromId);
  }

  /**
   * Return a set of application-attempt entities for a given applicationId.
   * Cluster ID is not provided by client so default cluster ID has to be taken.
   * If userid, flow name and flowrun id which are optional query parameters are
   * not specified, they will be queried based on app id and default cluster id
   * from the flow context information stored in underlying storage
   * implementation. If number of matching entities are more than the limit,
   * most recent entities till the limit is reached, will be returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param appId Application id to which the entities to be queried belong to(
   *          Mandatory path param).
   * @param userId User id which should match for the entities(Optional query
   *          param)
   * @param flowName Flow name which should match for the entities(Optional
   *          query param).
   * @param flowRunId Run id which should match for the entities(Optional query
   *          param).
   * @param limit If specified, defines the number of entities to return. The
   *          maximum possible value for limit can be {@link Long#MAX_VALUE}. If
   *          it is not specified or has a value less than 0, then limit will be
   *          considered as 100. (Optional query param).
   * @param createdTimeStart If specified, matched entities should not be
   *          created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched entities should not be created
   *          after this timestamp(Optional query param).
   * @param relatesTo If specified, matched entities should relate to given
   *          entities associated with a entity type. relatesto is a comma
   *          separated list in the format
   *          [entitytype]:[entityid1]:[entityid2]... (Optional query param).
   * @param isRelatedTo If specified, matched entities should be related to
   *          given entities associated with a entity type. relatesto is a comma
   *          separated list in the format
   *          [entitytype]:[entityid1]:[entityid2]... (Optional query param).
   * @param infofilters If specified, matched entities should have exact matches
   *          to the given info represented as key-value pairs. This is
   *          represented as infofilters=info1:value1,info2:value2... (Optional
   *          query param).
   * @param conffilters If specified, matched entities should have exact matches
   *          to the given configs represented as key-value pairs. This is
   *          represented as conffilters=conf1:value1,conf2:value2... (Optional
   *          query param).
   * @param metricfilters If specified, matched entities should contain the
   *          given metrics. This is represented as metricfilters=metricid1,
   *          metricid2... (Optional query param).
   * @param eventfilters If specified, matched entities should contain the given
   *          events. This is represented as eventfilters=eventid1, eventid2...
   * @param confsToRetrieve If specified, defines which configurations to
   *          retrieve and send back in response. These configs will be
   *          retrieved irrespective of whether configs are specified in fields
   *          to retrieve or not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *          and send back in response. These metrics will be retrieved
   *          irrespective of whether metrics are specified in fields to
   *          retrieve or not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *          {@link Field}. All fields will be retrieved if fields=ALL. If not
   *          specified, 3 fields i.e. entity type, id, created time is returned
   *          (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *          Considered only if fields contains METRICS/ALL or
   *          metricsToRetrieve is specified. Ignored otherwise. The maximum
   *          possible value for metricsLimit can be {@link Integer#MAX_VALUE}.
   *          If it is not specified or has a value less than 1, and metrics
   *          have to be retrieved, then metricsLimit will be considered as 1
   *          i.e. latest single value of metric(s) will be returned. (Optional
   *          query param).
   * @param metricsTimeStart If specified, returned metrics for the app attempts
   *          would not contain metric values before this timestamp(Optional
   *          query param).
   * @param metricsTimeEnd If specified, returned metrics for the app attempts
   *          would not contain metric values after this timestamp(Optional
   *          query param).
   * @param fromId If specified, retrieve the next set of application-attempt
   *         entities from the given fromId. The set of application-attempt
   *         entities retrieved is inclusive of specified fromId. fromId should
   *         be taken from the value associated with FROM_ID info key in
   *         entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *         set of <cite>TimelineEntity</cite> instances of the app-attempt
   *         entity type is returned.<br>
   *         On failures,<br>
   *         If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *         returned.<br>
   *         If flow context information cannot be retrieved, HTTP 404(Not
   *         Found) is returned.<br>
   *         For all other errors while retrieving data, HTTP 500(Internal
   *         Server Error) is returned.
   */
  @GET
  @Path("/apps/{appid}/appattempts")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getAppAttempts(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {

    return getAppAttempts(req, res, null, appId, userId, flowName, flowRunId,
        limit, createdTimeStart, createdTimeEnd, relatesTo, isRelatedTo,
        infofilters, conffilters, metricfilters, eventfilters, confsToRetrieve,
        metricsToRetrieve, fields, metricsLimit, metricsTimeStart,
        metricsTimeEnd, fromId);
  }

  /**
   * Return a set of application-attempt entities for a given applicationId. If
   * userid, flow name and flowrun id which are optional query parameters are
   * not specified, they will be queried based on app id and cluster id from the
   * flow context information stored in underlying storage implementation. If
   * number of matching entities are more than the limit, most recent entities
   * till the limit is reached, will be returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the entities to be queried belong to(
   *          Mandatory path param).
   * @param appId Application id to which the entities to be queried belong to(
   *          Mandatory path param).
   * @param userId User id which should match for the entities(Optional query
   *          param)
   * @param flowName Flow name which should match for the entities(Optional
   *          query param).
   * @param flowRunId Run id which should match for the entities(Optional query
   *          param).
   * @param limit If specified, defines the number of entities to return. The
   *          maximum possible value for limit can be {@link Long#MAX_VALUE}. If
   *          it is not specified or has a value less than 0, then limit will be
   *          considered as 100. (Optional query param).
   * @param createdTimeStart If specified, matched entities should not be
   *          created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched entities should not be created
   *          after this timestamp(Optional query param).
   * @param relatesTo If specified, matched entities should relate to given
   *          entities associated with a entity type. relatesto is a comma
   *          separated list in the format
   *          [entitytype]:[entityid1]:[entityid2]... (Optional query param).
   * @param isRelatedTo If specified, matched entities should be related to
   *          given entities associated with a entity type. relatesto is a comma
   *          separated list in the format
   *          [entitytype]:[entityid1]:[entityid2]... (Optional query param).
   * @param infofilters If specified, matched entities should have exact matches
   *          to the given info represented as key-value pairs. This is
   *          represented as infofilters=info1:value1,info2:value2... (Optional
   *          query param).
   * @param conffilters If specified, matched entities should have exact matches
   *          to the given configs represented as key-value pairs. This is
   *          represented as conffilters=conf1:value1,conf2:value2... (Optional
   *          query param).
   * @param metricfilters If specified, matched entities should contain the
   *          given metrics. This is represented as metricfilters=metricid1,
   *          metricid2... (Optional query param).
   * @param eventfilters If specified, matched entities should contain the given
   *          events. This is represented as eventfilters=eventid1, eventid2...
   * @param confsToRetrieve If specified, defines which configurations to
   *          retrieve and send back in response. These configs will be
   *          retrieved irrespective of whether configs are specified in fields
   *          to retrieve or not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *          and send back in response. These metrics will be retrieved
   *          irrespective of whether metrics are specified in fields to
   *          retrieve or not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *          {@link Field}. All fields will be retrieved if fields=ALL. If not
   *          specified, 3 fields i.e. entity type, id, created time is returned
   *          (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *          Considered only if fields contains METRICS/ALL or
   *          metricsToRetrieve is specified. Ignored otherwise. The maximum
   *          possible value for metricsLimit can be {@link Integer#MAX_VALUE}.
   *          If it is not specified or has a value less than 1, and metrics
   *          have to be retrieved, then metricsLimit will be considered as 1
   *          i.e. latest single value of metric(s) will be returned. (Optional
   *          query param).
   * @param metricsTimeStart If specified, returned metrics for the app attempts
   *          would not contain metric values before this timestamp(Optional
   *          query param).
   * @param metricsTimeEnd If specified, returned metrics for the app attempts
   *          would not contain metric values after this timestamp(Optional
   *          query param).
   * @param fromId If specified, retrieve the next set of application-attempt
   *         entities from the given fromId. The set of application-attempt
   *         entities retrieved is inclusive of specified fromId. fromId should
   *         be taken from the value associated with FROM_ID info key in
   *         entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *         set of <cite>TimelineEntity</cite> instances of the app-attempts
   *         entity type is returned.<br>
   *         On failures,<br>
   *         If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *         returned.<br>
   *         If flow context information cannot be retrieved, HTTP 404(Not
   *         Found) is returned.<br>
   *         For all other errors while retrieving data, HTTP 500(Internal
   *         Server Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/appattempts")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getAppAttempts(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId, @QueryParam("userid") String userId,
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {

    return getEntities(req, res, clusterId, appId,
        TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(), userId,
        flowName, flowRunId, limit, createdTimeStart, createdTimeEnd, relatesTo,
        isRelatedTo, infofilters, conffilters, metricfilters, eventfilters,
        confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd, fromId);
  }

  /**
   * Return a single application-attempt entity for the given attempt Id.
   * Cluster ID is not provided by client so default cluster ID has to be taken.
   * If userid, flow name and flowrun id which are optional query parameters are
   * not specified, they will be queried based on app id and default cluster id
   * from the flow context information stored in underlying storage
   * implementation.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param appId Application id to which the entity to be queried belongs to(
   *          Mandatory path param).
   * @param appAttemptId Application Attempt Id to which the containers belong
   *          to( Mandatory path param).
   * @param userId User id which should match for the entity(Optional query
   *          param).
   * @param flowName Flow name which should match for the entity(Optional query
   *          param).
   * @param flowRunId Run id which should match for the entity(Optional query
   *          param).
   * @param confsToRetrieve If specified, defines which configurations to
   *          retrieve and send back in response. These configs will be
   *          retrieved irrespective of whether configs are specified in fields
   *          to retrieve or not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *          and send back in response. These metrics will be retrieved
   *          irrespective of whether metrics are specified in fields to
   *          retrieve or not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *          {@link Field}. All fields will be retrieved if fields=ALL. If not
   *          specified, 3 fields i.e. entity type, id, created time is returned
   *          (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *          Considered only if fields contains METRICS/ALL or
   *          metricsToRetrieve is specified. Ignored otherwise. The maximum
   *          possible value for metricsLimit can be {@link Integer#MAX_VALUE}.
   *          If it is not specified or has a value less than 1, and metrics
   *          have to be retrieved, then metricsLimit will be considered as 1
   *          i.e. latest single value of metric(s) will be returned. (Optional
   *          query param).
   * @param metricsTimeStart If specified, returned metrics for the app attempt
   *          would not contain metric values before this timestamp(Optional
   *          query param).
   * @param metricsTimeEnd If specified, returned metrics for the app attempt
   *          would not contain metric values after this timestamp(Optional
   *          query param).
   * @param entityIdPrefix Defines the id prefix for the entity to be fetched.
   *          If specified, then entity retrieval will be faster.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *         <cite>TimelineEntity</cite> instance is returned.<br>
   *         On failures,<br>
   *         If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *         returned.<br>
   *         If flow context information cannot be retrieved or entity for the
   *         given entity id cannot be found, HTTP 404(Not Found) is
   *         returned.<br>
   *         For all other errors while retrieving data, HTTP 500(Internal
   *         Server Error) is returned.
   */
  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getAppAttempt(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("appattemptid") String appAttemptId,
      @QueryParam("userid") String userId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("entityidprefix") String entityIdPrefix) {
    return getAppAttempt(req, res, null, appId, appAttemptId, userId, flowName,
        flowRunId, confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd, entityIdPrefix);
  }

  /**
   * Return a single application attempt entity of the given entity Id. If
   * userid, flowname and flowrun id which are optional query parameters are not
   * specified, they will be queried based on app id and cluster id from the
   * flow context information stored in underlying storage implementation.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the entity to be queried belongs to(
   *          Mandatory path param).
   * @param appId Application id to which the entity to be queried belongs to(
   *          Mandatory path param).
   * @param appAttemptId Application Attempt Id to which the containers belong
   *          to( Mandatory path param).
   * @param userId User id which should match for the entity(Optional query
   *          param).
   * @param flowName Flow name which should match for the entity(Optional query
   *          param).
   * @param flowRunId Run id which should match for the entity(Optional query
   *          param).
   * @param confsToRetrieve If specified, defines which configurations to
   *          retrieve and send back in response. These configs will be
   *          retrieved irrespective of whether configs are specified in fields
   *          to retrieve or not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *          and send back in response. These metrics will be retrieved
   *          irrespective of whether metrics are specified in fields to
   *          retrieve or not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *          {@link Field}. All fields will be retrieved if fields=ALL. If not
   *          specified, 3 fields i.e. entity type, id and created time is
   *          returned (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *          Considered only if fields contains METRICS/ALL or
   *          metricsToRetrieve is specified. Ignored otherwise. The maximum
   *          possible value for metricsLimit can be {@link Integer#MAX_VALUE}.
   *          If it is not specified or has a value less than 1, and metrics
   *          have to be retrieved, then metricsLimit will be considered as 1
   *          i.e. latest single value of metric(s) will be returned. (Optional
   *          query param).
   * @param metricsTimeStart If specified, returned metrics for the app attempt
   *          would not contain metric values before this timestamp(Optional
   *          query param).
   * @param metricsTimeEnd If specified, returned metrics for the app attempt
   *          would not contain metric values after this timestamp(Optional
   *          query param).
   * @param entityIdPrefix Defines the id prefix for the entity to be fetched.
   *          If specified, then entity retrieval will be faster.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *         <cite>TimelineEntity</cite> instance is returned.<br>
   *         On failures,<br>
   *         If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *         returned.<br>
   *         If flow context information cannot be retrieved or entity for the
   *         given entity id cannot be found, HTTP 404(Not Found) is
   *         returned.<br>
   *         For all other errors while retrieving data, HTTP 500(Internal
   *         Server Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/appattempts/{appattemptid}")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getAppAttempt(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId,
      @PathParam("appattemptid") String appAttemptId,
      @QueryParam("userid") String userId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("entityidprefix") String entityIdPrefix) {
    return getEntity(req, res, clusterId, appId,
        TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(), appAttemptId,
        userId, flowName, flowRunId, confsToRetrieve, metricsToRetrieve, fields,
        metricsLimit, metricsTimeStart, metricsTimeEnd, entityIdPrefix);
  }

  /**
   * Return a set of container entities belonging to given application attempt
   * id. Cluster ID is not provided by client so default cluster ID has to be
   * taken. If userid, flow name and flowrun id which are optional query
   * parameters are not specified, they will be queried based on app id and
   * default cluster id from the flow context information stored in underlying
   * storage implementation. If number of matching entities are more than the
   * limit, most recent entities till the limit is reached, will be returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param appId Application id to which the entities to be queried belong to(
   *          Mandatory path param).
   * @param appattemptId Application Attempt Id to which the containers belong
   *          to( Mandatory path param).
   * @param userId User id which should match for the entities(Optional query
   *          param)
   * @param flowName Flow name which should match for the entities(Optional
   *          query param).
   * @param flowRunId Run id which should match for the entities(Optional query
   *          param).
   * @param limit If specified, defines the number of entities to return. The
   *          maximum possible value for limit can be {@link Long#MAX_VALUE}. If
   *          it is not specified or has a value less than 0, then limit will be
   *          considered as 100. (Optional query param).
   * @param createdTimeStart If specified, matched entities should not be
   *          created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched entities should not be created
   *          after this timestamp(Optional query param).
   * @param relatesTo If specified, matched entities should relate to given
   *          entities associated with a entity type. relatesto is a comma
   *          separated list in the format
   *          [entitytype]:[entityid1]:[entityid2]... (Optional query param).
   * @param isRelatedTo If specified, matched entities should be related to
   *          given entities associated with a entity type. relatesto is a comma
   *          separated list in the format
   *          [entitytype]:[entityid1]:[entityid2]... (Optional query param).
   * @param infofilters If specified, matched entities should have exact matches
   *          to the given info represented as key-value pairs. This is
   *          represented as infofilters=info1:value1,info2:value2... (Optional
   *          query param).
   * @param conffilters If specified, matched entities should have exact matches
   *          to the given configs represented as key-value pairs. This is
   *          represented as conffilters=conf1:value1,conf2:value2... (Optional
   *          query param).
   * @param metricfilters If specified, matched entities should contain the
   *          given metrics. This is represented as metricfilters=metricid1,
   *          metricid2... (Optional query param).
   * @param eventfilters If specified, matched entities should contain the given
   *          events. This is represented as eventfilters=eventid1, eventid2...
   * @param confsToRetrieve If specified, defines which configurations to
   *          retrieve and send back in response. These configs will be
   *          retrieved irrespective of whether configs are specified in fields
   *          to retrieve or not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *          and send back in response. These metrics will be retrieved
   *          irrespective of whether metrics are specified in fields to
   *          retrieve or not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *          {@link Field}. All fields will be retrieved if fields=ALL. If not
   *          specified, 3 fields i.e. entity type, id, created time is returned
   *          (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *          Considered only if fields contains METRICS/ALL or
   *          metricsToRetrieve is specified. Ignored otherwise. The maximum
   *          possible value for metricsLimit can be {@link Integer#MAX_VALUE}.
   *          If it is not specified or has a value less than 1, and metrics
   *          have to be retrieved, then metricsLimit will be considered as 1
   *          i.e. latest single value of metric(s) will be returned. (Optional
   *          query param).
   * @param metricsTimeStart If specified, returned metrics for the containers
   *          would not contain metric values before this timestamp(Optional
   *          query param).
   * @param metricsTimeEnd If specified, returned metrics for the containers
   *          would not contain metric values after this timestamp(Optional
   *          query param).
   * @param fromId If specified, retrieve the next set of container
   *         entities from the given fromId. The set of container
   *         entities retrieved is inclusive of specified fromId. fromId should
   *         be taken from the value associated with FROM_ID info key in
   *         entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *         set of <cite>TimelineEntity</cite> instances of the containers
   *         belongs to given application attempt id.<br>
   *         On failures,<br>
   *         If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *         returned.<br>
   *         If flow context information cannot be retrieved, HTTP 404(Not
   *         Found) is returned.<br>
   *         For all other errors while retrieving data, HTTP 500(Internal
   *         Server Error) is returned.
   */
  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}/containers")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getContainers(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("appattemptid") String appattemptId,
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    return getContainers(req, res, null, appId, appattemptId, userId, flowName,
        flowRunId, limit, createdTimeStart, createdTimeEnd, relatesTo,
        isRelatedTo, infofilters, conffilters, metricfilters, eventfilters,
        confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd, fromId);
  }

  /**
   * Return a set of container entities belonging to given application attempt
   * id. If userid, flow name and flowrun id which are optional query parameters
   * are not specified, they will be queried based on app id and cluster id from
   * the flow context information stored in underlying storage implementation.
   * If number of matching entities are more than the limit, most recent
   * entities till the limit is reached, will be returned.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the entities to be queried belong to(
   *          Mandatory path param).
   * @param appId Application id to which the entities to be queried belong to(
   *          Mandatory path param).
   * @param appattemptId Application Attempt Id to which the containers belong
   *          to( Mandatory path param).
   * @param userId User id which should match for the entities(Optional query
   *          param)
   * @param flowName Flow name which should match for the entities(Optional
   *          query param).
   * @param flowRunId Run id which should match for the entities(Optional query
   *          param).
   * @param limit If specified, defines the number of entities to return. The
   *          maximum possible value for limit can be {@link Long#MAX_VALUE}. If
   *          it is not specified or has a value less than 0, then limit will be
   *          considered as 100. (Optional query param).
   * @param createdTimeStart If specified, matched entities should not be
   *          created before this timestamp(Optional query param).
   * @param createdTimeEnd If specified, matched entities should not be created
   *          after this timestamp(Optional query param).
   * @param relatesTo If specified, matched entities should relate to given
   *          entities associated with a entity type. relatesto is a comma
   *          separated list in the format
   *          [entitytype]:[entityid1]:[entityid2]... (Optional query param).
   * @param isRelatedTo If specified, matched entities should be related to
   *          given entities associated with a entity type. relatesto is a comma
   *          separated list in the format
   *          [entitytype]:[entityid1]:[entityid2]... (Optional query param).
   * @param infofilters If specified, matched entities should have exact matches
   *          to the given info represented as key-value pairs. This is
   *          represented as infofilters=info1:value1,info2:value2... (Optional
   *          query param).
   * @param conffilters If specified, matched entities should have exact matches
   *          to the given configs represented as key-value pairs. This is
   *          represented as conffilters=conf1:value1,conf2:value2... (Optional
   *          query param).
   * @param metricfilters If specified, matched entities should contain the
   *          given metrics. This is represented as metricfilters=metricid1,
   *          metricid2... (Optional query param).
   * @param eventfilters If specified, matched entities should contain the given
   *          events. This is represented as eventfilters=eventid1, eventid2...
   * @param confsToRetrieve If specified, defines which configurations to
   *          retrieve and send back in response. These configs will be
   *          retrieved irrespective of whether configs are specified in fields
   *          to retrieve or not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *          and send back in response. These metrics will be retrieved
   *          irrespective of whether metrics are specified in fields to
   *          retrieve or not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *          {@link Field}. All fields will be retrieved if fields=ALL. If not
   *          specified, 3 fields i.e. entity type, id, created time is returned
   *          (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *          Considered only if fields contains METRICS/ALL or
   *          metricsToRetrieve is specified. Ignored otherwise. The maximum
   *          possible value for metricsLimit can be {@link Integer#MAX_VALUE}.
   *          If it is not specified or has a value less than 1, and metrics
   *          have to be retrieved, then metricsLimit will be considered as 1
   *          i.e. latest single value of metric(s) will be returned. (Optional
   *          query param).
   * @param metricsTimeStart If specified, returned metrics for the containers
   *          would not contain metric values before this timestamp(Optional
   *          query param).
   * @param metricsTimeEnd If specified, returned metrics for the containers
   *          would not contain metric values after this timestamp(Optional
   *          query param).
   * @param fromId If specified, retrieve the next set of container
   *         entities from the given fromId. The set of container
   *         entities retrieved is inclusive of specified fromId. fromId should
   *         be taken from the value associated with FROM_ID info key in
   *         entity response which was sent earlier.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *         set of <cite>TimelineEntity</cite> instances of the containers
   *         belongs to given application attempt id.<br>
   *         On failures,<br>
   *         If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *         returned.<br>
   *         If flow context information cannot be retrieved, HTTP 404(Not
   *         Found) is returned.<br>
   *         For all other errors while retrieving data, HTTP 500(Internal
   *         Server Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/appattempts/{appattemptid}/containers")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<TimelineEntity> getContainers(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId,
      @PathParam("appattemptid") String appattemptId,
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {

    String entityType = TimelineEntityType.YARN_CONTAINER.toString();
    String parentEntityType =
        TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString();
    String jsonFormatString = "{\"type\":\"" + parentEntityType + "\",\"id\":\""
        + appattemptId + "\"}";
    String containerFilters =
        "SYSTEM_INFO_PARENT_ENTITY eq " + jsonFormatString;
    String infofilter;
    if (infofilters != null) {
      infofilter = containerFilters + " AND " + infofilters;
    } else {
      infofilter = containerFilters;
    }
    return getEntities(req, res, clusterId, appId, entityType, userId, flowName,
        flowRunId, limit, createdTimeStart, createdTimeEnd, relatesTo,
        isRelatedTo, infofilter, conffilters, metricfilters, eventfilters,
        confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd, fromId);
  }

  /**
   * Return a single container entity for the given container Id. Cluster ID is
   * not provided by client so default cluster ID has to be taken. If userid,
   * flow name and flowrun id which are optional query parameters are not
   * specified, they will be queried based on app id and default cluster id from
   * the flow context information stored in underlying storage implementation.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param appId Application id to which the entity to be queried belongs to(
   *          Mandatory path param).
   * @param containerId Container Id to which the entity to be queried belongs
   *          to( Mandatory path param).
   * @param userId User id which should match for the entity(Optional query
   *          param).
   * @param flowName Flow name which should match for the entity(Optional query
   *          param).
   * @param flowRunId Run id which should match for the entity(Optional query
   *          param).
   * @param confsToRetrieve If specified, defines which configurations to
   *          retrieve and send back in response. These configs will be
   *          retrieved irrespective of whether configs are specified in fields
   *          to retrieve or not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *          and send back in response. These metrics will be retrieved
   *          irrespective of whether metrics are specified in fields to
   *          retrieve or not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *          {@link Field}. All fields will be retrieved if fields=ALL. If not
   *          specified, 3 fields i.e. entity type, id, created time is returned
   *          (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *          Considered only if fields contains METRICS/ALL or
   *          metricsToRetrieve is specified. Ignored otherwise. The maximum
   *          possible value for metricsLimit can be {@link Integer#MAX_VALUE}.
   *          If it is not specified or has a value less than 1, and metrics
   *          have to be retrieved, then metricsLimit will be considered as 1
   *          i.e. latest single value of metric(s) will be returned. (Optional
   *          query param).
   * @param metricsTimeStart If specified, returned metrics for the container
   *          would not contain metric values before this timestamp(Optional
   *          query param).
   * @param metricsTimeEnd If specified, returned metrics for the container
   *          would not contain metric values after this timestamp(Optional
   *          query param).
   * @param entityIdPrefix Defines the id prefix for the entity to be fetched.
   *          If specified, then entity retrieval will be faster.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing
   *         <cite>TimelineEntity</cite> instance is returned.<br>
   *         On failures,<br>
   *         If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *         returned.<br>
   *         If flow context information cannot be retrieved or entity for the
   *         given entity id cannot be found, HTTP 404(Not Found) is
   *         returned.<br>
   *         For all other errors while retrieving data, HTTP 500(Internal
   *         Server Error) is returned.
   */
  @GET
  @Path("/apps/{appid}/containers/{containerid}")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getContainer(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("containerid") String containerId,
      @QueryParam("userid") String userId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("entityidprefix") String entityIdPrefix) {
    return getContainer(req, res, null, appId, containerId, userId, flowName,
        flowRunId, confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        entityIdPrefix, metricsTimeStart, metricsTimeEnd);
  }

  /**
   * Return a single container entity for the given container Id. If userid,
   * flowname and flowrun id which are optional query parameters are not
   * specified, they will be queried based on app id and cluster id from the
   * flow context information stored in underlying storage implementation.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param clusterId Cluster id to which the entity to be queried belongs to(
   *          Mandatory path param).
   * @param appId Application id to which the entity to be queried belongs to(
   *          Mandatory path param).
   * @param containerId Container Id to which the entity to be queried belongs
   *          to( Mandatory path param).
   * @param userId User id which should match for the entity(Optional query
   *          param).
   * @param flowName Flow name which should match for the entity(Optional query
   *          param).
   * @param flowRunId Run id which should match for the entity(Optional query
   *          param).
   * @param confsToRetrieve If specified, defines which configurations to
   *          retrieve and send back in response. These configs will be
   *          retrieved irrespective of whether configs are specified in fields
   *          to retrieve or not.
   * @param metricsToRetrieve If specified, defines which metrics to retrieve
   *          and send back in response. These metrics will be retrieved
   *          irrespective of whether metrics are specified in fields to
   *          retrieve or not.
   * @param fields Specifies which fields of the entity object to retrieve, see
   *          {@link Field}. All fields will be retrieved if fields=ALL. If not
   *          specified, 3 fields i.e. entity type, id and created time is
   *          returned (Optional query param).
   * @param metricsLimit If specified, defines the number of metrics to return.
   *          Considered only if fields contains METRICS/ALL or
   *          metricsToRetrieve is specified. Ignored otherwise. The maximum
   *          possible value for metricsLimit can be {@link Integer#MAX_VALUE}.
   *          If it is not specified or has a value less than 1, and metrics
   *          have to be retrieved, then metricsLimit will be considered as 1
   *          i.e. latest single value of metric(s) will be returned. (Optional
   *          query param).
   * @param metricsTimeStart If specified, returned metrics for the container
   *          would not contain metric values before this timestamp(Optional
   *          query param).
   * @param metricsTimeEnd If specified, returned metrics for the container
   *          would not contain metric values after this timestamp(Optional
   *          query param).
   * @param entityIdPrefix Defines the id prefix for the entity to be fetched.
   *          If specified, then entity retrieval will be faster.
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *         <cite>TimelineEntity</cite> instance is returned.<br>
   *         On failures,<br>
   *         If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *         returned.<br>
   *         If flow context information cannot be retrieved or entity for the
   *         given entity id cannot be found, HTTP 404(Not Found) is
   *         returned.<br>
   *         For all other errors while retrieving data, HTTP 500(Internal
   *         Server Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/containers/{containerid}")
  @Produces(MediaType.APPLICATION_JSON)
  public TimelineEntity getContainer(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId,
      @PathParam("containerid") String containerId,
      @QueryParam("userid") String userId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("entityidprefix") String entityIdPrefix) {
    return getEntity(req, res, clusterId, appId,
        TimelineEntityType.YARN_CONTAINER.toString(), containerId, userId,
        flowName, flowRunId, confsToRetrieve, metricsToRetrieve, fields,
        metricsLimit, metricsTimeStart, metricsTimeEnd, entityIdPrefix);
  }

  /**
   * Returns a set of available entity types for a given app id. Cluster ID is
   * not provided by client so default cluster ID has to be taken. If userid,
   * flow name and flow run id which are optional query parameters are not
   * specified, they will be queried based on app id and cluster id from the
   * flow context information stored in underlying storage implementation.
   *
   * @param req Servlet request.
   * @param res Servlet response.
   * @param appId Application id to be queried(Mandatory path param).
   * @param flowName Flow name which should match for the app(Optional query
   *     param).
   * @param flowRunId Run id which should match for the app(Optional query
   *     param).
   * @param userId User id which should match for the app(Optional query param).
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     list contains all timeline entity types is returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     If flow context information cannot be retrieved or app for the given
   *     app id cannot be found, HTTP 404(Not Found) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/apps/{appid}/entity-types")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<String> getEntityTypes(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("appid") String appId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("userid") String userId) {
    return getEntityTypes(req, res, null, appId, flowName, flowRunId, userId);
  }

  /**
   * Returns a set of available entity types for a given app id. If userid,
   * flow name and flow run id which are optional query parameters are not
   * specified, they will be queried based on app id and cluster id from the
   * flow context information stored in underlying storage implementation.
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
   *
   * @return If successful, a HTTP 200(OK) response having a JSON representing a
   *     list contains all timeline entity types is returned.<br>
   *     On failures,<br>
   *     If any problem occurs in parsing request, HTTP 400(Bad Request) is
   *     returned.<br>
   *     If flow context information cannot be retrieved or app for the given
   *     app id cannot be found, HTTP 404(Not Found) is returned.<br>
   *     For all other errors while retrieving data, HTTP 500(Internal Server
   *     Error) is returned.
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/entity-types")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<String> getEntityTypes(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("appid") String appId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("userid") String userId) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<String> results = null;
    try {
      results = timelineReaderManager.getEntityTypes(
          TimelineReaderWebServicesUtils.createTimelineReaderContext(
          clusterId, userId, flowName, flowRunId, appId,
              null, null, null));
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime, "flowrunid");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntityTypesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    return results;
  }

  @GET
  @Path("/users/{userid}/entities/{entitytype}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getSubAppEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("userid") String userId,
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    return getSubAppEntities(req, res, null, userId, entityType, limit,
        createdTimeStart, createdTimeEnd, relatesTo, isRelatedTo, infofilters,
        conffilters, metricfilters, eventfilters, confsToRetrieve,
        metricsToRetrieve, fields, metricsLimit, metricsTimeStart,
        metricsTimeEnd, fromId);
  }

  @GET
  @Path("/clusters/{clusterid}/users/{userid}/entities/{entitytype}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getSubAppEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("userid") String userId,
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
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user " +
        TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext context =
          TimelineReaderWebServicesUtils.createTimelineReaderContext(clusterId,
              null, null, null, null, entityType, null, null, userId);
      entities = timelineReaderManager.getEntities(context,
          TimelineReaderWebServicesUtils.createTimelineEntityFilters(
          limit, createdTimeStart, createdTimeEnd, relatesTo, isRelatedTo,
              infofilters, conffilters, metricfilters, eventfilters,
              fromId),
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
          confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
          metricsTimeStart, metricsTimeEnd));
      checkAccessForSubAppEntities(entities,callerUGI);
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime,
          "Either limit or createdtime start/end or metricslimit or metricstime"
              + " start/end or fromid");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL " + url +
          " (Took " + latency + " ms.)");
    }
    if (entities == null) {
      entities = Collections.emptySet();
    }
    return entities;
  }

  @GET
  @Path("/users/{userid}/entities/{entitytype}/{entityid}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getSubAppEntities(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("userid") String userId,
      @PathParam("entitytype") String entityType,
      @PathParam("entityid") String entityId,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("entityidprefix") String entityIdPrefix) {
    return getSubAppEntities(req, res, null, userId, entityType, entityId,
        confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd, entityIdPrefix);
  }

  @GET
  @Path("/clusters/{clusterid}/users/{userid}/entities/{entitytype}/{entityid}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getSubAppEntities(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("clusterid") String clusterId,
      @PathParam("userid") String userId,
      @PathParam("entitytype") String entityType,
      @PathParam("entityid") String entityId,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("entityidprefix") String entityIdPrefix) {
    String url = req.getRequestURI() + (req.getQueryString() == null ? ""
        : QUERY_STRING_SEP + req.getQueryString());
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL " + url + " from user "
        + TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      TimelineReaderContext context = TimelineReaderWebServicesUtils
          .createTimelineReaderContext(clusterId, null, null, null, null,
              entityType, entityIdPrefix, entityId, userId);
      entities = timelineReaderManager.getEntities(context,
          new TimelineEntityFilters.Builder().build(),
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
              confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
              metricsTimeStart, metricsTimeEnd));
      checkAccessForSubAppEntities(entities,callerUGI);
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime, "Either metricslimit or metricstime"
          + " start/end");
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info(
          "Processed URL " + url + " (Took " + latency + " ms.)");
    }
    if (entities == null) {
      entities = Collections.emptySet();
    }

    return entities;
  }

  static boolean isDisplayEntityPerUserFilterEnabled(Configuration config) {
    return !config
        .getBoolean(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_READ_AUTH_ENABLED)
        && config
        .getBoolean(YarnConfiguration.FILTER_ENTITY_LIST_BY_USER, false);
  }

  // TODO to be removed or modified once ACL story is played
  private void checkAccessForSubAppEntities(Set<TimelineEntity> entities,
      UserGroupInformation callerUGI) throws Exception {
    if (entities != null && entities.size() > 0
        && isDisplayEntityPerUserFilterEnabled(
        getTimelineReaderManager().getConfig())) {
      TimelineReaderContext timelineReaderContext = null;
      TimelineEntity entity = entities.iterator().next();
      String fromId =
          (String) entity.getInfo().get(TimelineReaderUtils.FROMID_KEY);
      timelineReaderContext =
          TimelineFromIdConverter.SUB_APPLICATION_ENTITY_FROMID
              .decodeUID(fromId);
      checkAccess(getTimelineReaderManager(), callerUGI,
          timelineReaderContext.getDoAsUser());
    }
  }

  // TODO to be removed or modified once ACL story is played
  private void checkAccessForAppEntity(TimelineEntity entity,
      UserGroupInformation callerUGI) throws Exception {
    if (entity != null && isDisplayEntityPerUserFilterEnabled(
        getTimelineReaderManager().getConfig())) {
      String fromId =
          (String) entity.getInfo().get(TimelineReaderUtils.FROMID_KEY);
      TimelineReaderContext timelineReaderContext =
          TimelineFromIdConverter.APPLICATION_FROMID.decodeUID(fromId);
      checkAccess(getTimelineReaderManager(), callerUGI,
          timelineReaderContext.getUserId());
    }
  }

  // TODO to be removed or modified once ACL story is played
  private void checkAccessForGenericEntity(TimelineEntity entity,
      UserGroupInformation callerUGI) throws Exception {
    if (entity != null && isDisplayEntityPerUserFilterEnabled(
        getTimelineReaderManager().getConfig())) {
      String fromId =
          (String) entity.getInfo().get(TimelineReaderUtils.FROMID_KEY);
      TimelineReaderContext timelineReaderContext =
          TimelineFromIdConverter.GENERIC_ENTITY_FROMID.decodeUID(fromId);
      checkAccess(getTimelineReaderManager(), callerUGI,
          timelineReaderContext.getUserId());
    }
  }

  // TODO to be removed or modified once ACL story is played
  private void checkAccessForGenericEntities(Set<TimelineEntity> entities,
      UserGroupInformation callerUGI, String entityType) throws Exception {
    if (entities != null && entities.size() > 0
        && isDisplayEntityPerUserFilterEnabled(
        getTimelineReaderManager().getConfig())) {
      TimelineReaderContext timelineReaderContext = null;
      TimelineEntity entity = entities.iterator().next();
      String uid =
          (String) entity.getInfo().get(TimelineReaderUtils.FROMID_KEY);
      if (TimelineEntityType.YARN_APPLICATION.matches(entityType)) {
        timelineReaderContext =
            TimelineFromIdConverter.APPLICATION_FROMID.decodeUID(uid);
      } else {
        timelineReaderContext =
            TimelineFromIdConverter.GENERIC_ENTITY_FROMID.decodeUID(uid);
      }
      checkAccess(getTimelineReaderManager(), callerUGI,
          timelineReaderContext.getUserId());
    }
  }

  // TODO to be removed/modified once ACL story has played
  static boolean validateAuthUserWithEntityUser(
      TimelineReaderManager readerManager, UserGroupInformation ugi,
      String entityUser) {
    String authUser = TimelineReaderWebServicesUtils.getUserName(ugi);
    String requestedUser = TimelineReaderWebServicesUtils.parseStr(entityUser);
    LOG.debug(
          "Authenticated User: {} Requested User:{}", authUser, entityUser);
    return (readerManager.checkAccess(ugi) || authUser.equals(requestedUser));
  }

  // TODO to be removed/modified once ACL story has played
  static boolean checkAccess(TimelineReaderManager readerManager,
      UserGroupInformation ugi, String entityUser) {
    if (isDisplayEntityPerUserFilterEnabled(readerManager.getConfig())) {
      if (!validateAuthUserWithEntityUser(readerManager, ugi,
          entityUser)) {
        String userName = ugi == null ? null : ugi.getShortUserName();
        String msg = "User " + userName
            + " is not allowed to read TimelineService V2 data.";
        throw new ForbiddenException(msg);
      }
    }
    return true;
  }

  // TODO to be removed or modified once ACL story is played
  static void checkAccess(TimelineReaderManager readerManager,
      UserGroupInformation callerUGI, Set<TimelineEntity> entities,
      String entityUserKey, boolean verifyForAllEntity) {
    if (entities.size() > 0 && isDisplayEntityPerUserFilterEnabled(
        readerManager.getConfig())) {
      Set<TimelineEntity> userEntities = new LinkedHashSet<>();
      userEntities.addAll(entities);
      for (TimelineEntity entity : userEntities) {
        if (entity.getInfo() != null) {
          String userId = (String) entity.getInfo().get(entityUserKey);
          if (!validateAuthUserWithEntityUser(readerManager, callerUGI,
              userId)) {
            entities.remove(entity);
            if (!verifyForAllEntity) {
              break;
            }
          }
        }
      }
    }
  }
}
