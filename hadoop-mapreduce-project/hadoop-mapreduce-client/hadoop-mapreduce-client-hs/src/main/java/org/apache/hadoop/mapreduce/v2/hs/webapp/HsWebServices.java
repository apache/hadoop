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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import java.io.IOException;
import java.util.Set;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.AMWebServices;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.MapTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ReduceTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TasksInfo;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.AMAttemptInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.AMAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.HistoryInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.ExtendedLogMetaRequest;
import org.apache.hadoop.yarn.server.webapp.WrappedLogMetaRequest;
import org.apache.hadoop.yarn.server.webapp.YarnWebServiceParams;
import org.apache.hadoop.yarn.server.webapp.LogServlet;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.WebApp;

import org.apache.hadoop.classification.VisibleForTesting;

@Singleton
@Path("/ws/v1/history")
public class HsWebServices extends WebServices {
  private final HistoryContext ctx;
  private WebApp webapp;
  private LogServlet logServlet;
  private boolean mrAclsEnabled;

  @Context
  private HttpServletResponse response;

  @Context
  private UriInfo uriInfo;

  @Inject
  public HsWebServices(
      final @Named("ctx") HistoryContext ctx,
      final @Named("conf") Configuration conf,
      final @Named("hsWebApp") WebApp webapp,
      final @Named("appClient") @Nullable ApplicationClientProtocol appBaseProto) {
    super(appBaseProto);
    this.ctx = ctx;
    this.webapp = webapp;
    this.logServlet = new LogServlet(conf, this);
    this.mrAclsEnabled = conf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
  }

  private boolean hasAccess(Job job, HttpServletRequest request) {
    String remoteUser = request.getRemoteUser();
    if (remoteUser != null) {
      return job.checkAccess(UserGroupInformation.createRemoteUser(remoteUser),
          JobACL.VIEW_JOB);
    }
    return true;
  }

  private void checkAccess(Job job, HttpServletRequest request) {
    if (!hasAccess(job, request)) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }
  }
  private void checkAccess(String containerIdStr, HttpServletRequest hsr) {
    if (mrAclsEnabled) {
      checkAccess(AMWebServices.getJobFromContainerIdString(containerIdStr, ctx), hsr);
    }
  }

  private void init() {
    //clear content type
    response.setContentType(null);
  }

  @VisibleForTesting
  void setResponse(HttpServletResponse response) {
    this.response = response;
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public HistoryInfo get() {
    return getHistoryInfo();
  }

  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public HistoryInfo getHistoryInfo() {
    init();
    return new HistoryInfo();
  }

  @GET
  @Path("/mapreduce/jobs")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public JobsInfo getJobs(@QueryParam("user") String userQuery,
      @QueryParam("limit") String count,
      @QueryParam("state") String stateQuery,
      @QueryParam("queue") String queueQuery,
      @QueryParam("startedTimeBegin") String startedBegin,
      @QueryParam("startedTimeEnd") String startedEnd,
      @QueryParam("finishedTimeBegin") String finishBegin,
      @QueryParam("finishedTimeEnd") String finishEnd) {

    Long countParam = null;
    init();
    
    if (count != null && !count.isEmpty()) {
      try {
        countParam = Long.parseLong(count);
      } catch (NumberFormatException e) {
        throw new BadRequestException(e.getMessage());
      }
      if (countParam <= 0) {
        throw new BadRequestException("limit value must be greater then 0");
      }
    }

    Long sBegin = null;
    if (startedBegin != null && !startedBegin.isEmpty()) {
      try {
        sBegin = Long.parseLong(startedBegin);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (sBegin < 0) {
        throw new BadRequestException("startedTimeBegin must be greater than 0");
      }
    }
    
    Long sEnd = null;
    if (startedEnd != null && !startedEnd.isEmpty()) {
      try {
        sEnd = Long.parseLong(startedEnd);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (sEnd < 0) {
        throw new BadRequestException("startedTimeEnd must be greater than 0");
      }
    }
    if (sBegin != null && sEnd != null && sBegin > sEnd) {
      throw new BadRequestException(
          "startedTimeEnd must be greater than startTimeBegin");
    }

    Long fBegin = null;
    if (finishBegin != null && !finishBegin.isEmpty()) {
      try {
        fBegin = Long.parseLong(finishBegin);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (fBegin < 0) {
        throw new BadRequestException("finishedTimeBegin must be greater than 0");
      }
    }
    Long fEnd = null;
    if (finishEnd != null && !finishEnd.isEmpty()) {
      try {
        fEnd = Long.parseLong(finishEnd);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (fEnd < 0) {
        throw new BadRequestException("finishedTimeEnd must be greater than 0");
      }
    }
    if (fBegin != null && fEnd != null && fBegin > fEnd) {
      throw new BadRequestException(
          "finishedTimeEnd must be greater than finishedTimeBegin");
    }
    
    JobState jobState = null;
    if (stateQuery != null) {
      jobState = JobState.valueOf(stateQuery);
    }

    return ctx.getPartialJobs(0l, countParam, userQuery, queueQuery, 
        sBegin, sEnd, fBegin, fEnd, jobState);
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public JobInfo getJob(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    return new JobInfo(job);
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/jobattempts")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public AMAttemptsInfo getJobAttempts(@PathParam("jobid") String jid) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    AMAttemptsInfo amAttempts = new AMAttemptsInfo();
    for (AMInfo amInfo : job.getAMInfos()) {
      AMAttemptInfo attempt = new AMAttemptInfo(amInfo, MRApps.toString(job
          .getID()), job.getUserName(), uriInfo.getBaseUri().toString(),
          webapp.name());
      amAttempts.add(attempt);
    }
    return amAttempts;
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/counters")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public JobCounterInfo getJobCounters(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    return new JobCounterInfo(this.ctx, job);
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/conf")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public ConfInfo getJobConf(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    ConfInfo info;
    try {
      info = new ConfInfo(job);
    } catch (IOException e) {
      throw new NotFoundException("unable to load configuration for job: "
          + jid);
    }
    return info;
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/tasks")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public TasksInfo getJobTasks(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @QueryParam("type") String type) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    TasksInfo allTasks = new TasksInfo();
    for (Task task : job.getTasks().values()) {
      TaskType ttype = null;
      if (type != null && !type.isEmpty()) {
        try {
          ttype = MRApps.taskType(type);
        } catch (YarnRuntimeException e) {
          throw new BadRequestException("tasktype must be either m or r");
        }
      }
      if (ttype != null && task.getType() != ttype) {
        continue;
      }
      allTasks.add(new TaskInfo(task));
    }
    return allTasks;
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/tasks/{taskid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public TaskInfo getJobTask(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    Task task = AMWebServices.getTaskFromTaskIdString(tid, job);
    return new TaskInfo(task);

  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/tasks/{taskid}/counters")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public JobTaskCounterInfo getSingleTaskCounters(
      @Context HttpServletRequest hsr, @PathParam("jobid") String jid,
      @PathParam("taskid") String tid) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    TaskId taskID = MRApps.toTaskID(tid);
    if (taskID == null) {
      throw new NotFoundException("taskid " + tid + " not found or invalid");
    }
    Task task = job.getTask(taskID);
    if (task == null) {
      throw new NotFoundException("task not found with id " + tid);
    }
    return new JobTaskCounterInfo(task);
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public TaskAttemptsInfo getJobTaskAttempts(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid) {

    init();
    TaskAttemptsInfo attempts = new TaskAttemptsInfo();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    Task task = AMWebServices.getTaskFromTaskIdString(tid, job);
    for (TaskAttempt ta : task.getAttempts().values()) {
      if (ta != null) {
        if (task.getType() == TaskType.REDUCE) {
          attempts.add(new ReduceTaskAttemptInfo(ta));
        } else {
          attempts.add(new MapTaskAttemptInfo(ta, false));
        }
      }
    }
    return attempts;
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public TaskAttemptInfo getJobTaskAttemptId(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid,
      @PathParam("attemptid") String attId) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    Task task = AMWebServices.getTaskFromTaskIdString(tid, job);
    TaskAttempt ta = AMWebServices.getTaskAttemptFromTaskAttemptString(attId,
        task);
    if (task.getType() == TaskType.REDUCE) {
      return new ReduceTaskAttemptInfo(ta);
    } else {
      return new MapTaskAttemptInfo(ta, false);
    }
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public JobTaskAttemptCounterInfo getJobTaskAttemptIdCounters(
      @Context HttpServletRequest hsr, @PathParam("jobid") String jid,
      @PathParam("taskid") String tid, @PathParam("attemptid") String attId) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    Task task = AMWebServices.getTaskFromTaskIdString(tid, job);
    TaskAttempt ta = AMWebServices.getTaskAttemptFromTaskAttemptString(attId,
        task);
    return new JobTaskAttemptCounterInfo(ta);
  }

  /**
   * Returns the user qualified path name of the remote log directory for
   * each pre-configured log aggregation file controller.
   *
   * @param req                HttpServletRequest
   * @return Path names grouped by file controller name
   */
  @GET
  @Path("/remote-log-dir")
  @Produces({ MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + ";" + JettyUtils.UTF_8 })
  public Response getRemoteLogDirPath(@Context HttpServletRequest req,
      @QueryParam(YarnWebServiceParams.REMOTE_USER) String user,
      @QueryParam(YarnWebServiceParams.APP_ID) String appIdStr)
      throws IOException {
    init();
    return logServlet.getRemoteLogDirPath(user, appIdStr);
  }

  @GET
  @Path("/extended-log-query")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public Response getAggregatedLogsMeta(@Context HttpServletRequest hsr,
      @QueryParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String fileName,
      @QueryParam(YarnWebServiceParams.FILESIZE) Set<String> fileSize,
      @QueryParam(YarnWebServiceParams.MODIFICATION_TIME) Set<String>
                                              modificationTime,
      @QueryParam(YarnWebServiceParams.APP_ID) String appIdStr,
      @QueryParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId) throws IOException {
    init();
    ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder logsRequest =
        new ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder();
    logsRequest.setAppId(appIdStr);
    logsRequest.setFileName(fileName);
    logsRequest.setContainerId(containerIdStr);
    logsRequest.setFileSize(fileSize);
    logsRequest.setModificationTime(modificationTime);
    logsRequest.setNodeId(nmId);
    return logServlet.getContainerLogsInfo(hsr, logsRequest);
  }

  @GET
  @Path("/aggregatedlogs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public Response getAggregatedLogsMeta(@Context HttpServletRequest hsr,
      @QueryParam(YarnWebServiceParams.APP_ID) String appIdStr,
      @QueryParam(YarnWebServiceParams.APPATTEMPT_ID) String appAttemptIdStr,
      @QueryParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    init();
    return logServlet.getLogsInfo(hsr, appIdStr, appAttemptIdStr,
        containerIdStr, nmId, redirectedFromNode, manualRedirection);
  }

  @GET
  @Path("/containers/{containerid}/logs")
  @Produces({ MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + ";" + JettyUtils.UTF_8})
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public Response getContainerLogs(@Context HttpServletRequest hsr,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    init();
    checkAccess(containerIdStr, hsr);
    WrappedLogMetaRequest.Builder logMetaRequestBuilder =
        LogServlet.createRequestFromContainerId(containerIdStr);

    return logServlet.getContainerLogsInfo(hsr, logMetaRequestBuilder, nmId,
        redirectedFromNode, null, manualRedirection);
  }

  @GET
  @Path("/containerlogs/{containerid}/{filename}")
  @Produces({ MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8 })
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public Response getContainerLogFile(@Context HttpServletRequest req,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME)
          String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT)
          String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE)
          String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    init();
    checkAccess(containerIdStr, req);
    return logServlet.getLogFile(req, containerIdStr, filename, format, size,
        nmId, redirectedFromNode, null, manualRedirection);
  }

  @VisibleForTesting
  LogServlet getLogServlet() {
    return this.logServlet;
  }

  @VisibleForTesting
  public void setLogServlet(LogServlet logServlet) {
    this.logServlet = logServlet;
  }

  @VisibleForTesting
  public void setHttpServletResponse(HttpServletResponse resp) {
    this.response = resp;
  }
}
