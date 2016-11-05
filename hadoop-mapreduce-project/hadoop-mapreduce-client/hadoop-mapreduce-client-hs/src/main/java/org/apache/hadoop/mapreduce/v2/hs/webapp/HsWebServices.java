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
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.JobACL;
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
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.WebApp;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

@Path("/ws/v1/history")
public class HsWebServices {
  private final HistoryContext ctx;
  private WebApp webapp;

  private @Context HttpServletResponse response;
  @Context
  UriInfo uriInfo;

  @Inject
  public HsWebServices(final HistoryContext ctx, final Configuration conf,
      final WebApp webapp) {
    this.ctx = ctx;
    this.webapp = webapp;
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
          attempts.add(new ReduceTaskAttemptInfo(ta, task.getType()));
        } else {
          attempts.add(new TaskAttemptInfo(ta, task.getType(), false));
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
      return new ReduceTaskAttemptInfo(ta, task.getType());
    } else {
      return new TaskAttemptInfo(ta, task.getType(), false);
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

}
