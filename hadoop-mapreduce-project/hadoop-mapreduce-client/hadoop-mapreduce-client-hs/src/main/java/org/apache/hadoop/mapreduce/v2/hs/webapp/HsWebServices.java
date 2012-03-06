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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
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
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.AMAttemptInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.AMAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.HistoryInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.WebApp;

import com.google.inject.Inject;

@Path("/ws/v1/history")
public class HsWebServices {
  private final AppContext appCtx;
  private WebApp webapp;
  private final Configuration conf;

  @Context
  UriInfo uriInfo;

  @Inject
  public HsWebServices(final AppContext appCtx, final Configuration conf,
      final WebApp webapp) {
    this.appCtx = appCtx;
    this.conf = conf;
    this.webapp = webapp;
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public HistoryInfo get() {
    return getHistoryInfo();
  }

  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public HistoryInfo getHistoryInfo() {
    return new HistoryInfo();
  }

  @GET
  @Path("/mapreduce/jobs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobsInfo getJobs(@QueryParam("user") String userQuery,
      @QueryParam("limit") String count,
      @QueryParam("queue") String queueQuery,
      @QueryParam("startedTimeBegin") String startedBegin,
      @QueryParam("startedTimeEnd") String startedEnd,
      @QueryParam("finishedTimeBegin") String finishBegin,
      @QueryParam("finishedTimeEnd") String finishEnd) {
    JobsInfo allJobs = new JobsInfo();
    long num = 0;
    boolean checkCount = false;
    boolean checkStart = false;
    boolean checkEnd = false;
    long countNum = 0;

    // set values suitable in case both of begin/end not specified
    long sBegin = 0;
    long sEnd = Long.MAX_VALUE;
    long fBegin = 0;
    long fEnd = Long.MAX_VALUE;

    if (count != null && !count.isEmpty()) {
      checkCount = true;
      try {
        countNum = Long.parseLong(count);
      } catch (NumberFormatException e) {
        throw new BadRequestException(e.getMessage());
      }
      if (countNum <= 0) {
        throw new BadRequestException("limit value must be greater then 0");
      }
    }

    if (startedBegin != null && !startedBegin.isEmpty()) {
      checkStart = true;
      try {
        sBegin = Long.parseLong(startedBegin);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (sBegin < 0) {
        throw new BadRequestException("startedTimeBegin must be greater than 0");
      }
    }
    if (startedEnd != null && !startedEnd.isEmpty()) {
      checkStart = true;
      try {
        sEnd = Long.parseLong(startedEnd);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (sEnd < 0) {
        throw new BadRequestException("startedTimeEnd must be greater than 0");
      }
    }
    if (sBegin > sEnd) {
      throw new BadRequestException(
          "startedTimeEnd must be greater than startTimeBegin");
    }

    if (finishBegin != null && !finishBegin.isEmpty()) {
      checkEnd = true;
      try {
        fBegin = Long.parseLong(finishBegin);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (fBegin < 0) {
        throw new BadRequestException("finishedTimeBegin must be greater than 0");
      }
    }
    if (finishEnd != null && !finishEnd.isEmpty()) {
      checkEnd = true;
      try {
        fEnd = Long.parseLong(finishEnd);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (fEnd < 0) {
        throw new BadRequestException("finishedTimeEnd must be greater than 0");
      }
    }
    if (fBegin > fEnd) {
      throw new BadRequestException(
          "finishedTimeEnd must be greater than finishedTimeBegin");
    }

    for (Job job : appCtx.getAllJobs().values()) {
      if (checkCount && num == countNum) {
        break;
      }

      // can't really validate queue is a valid one since queues could change
      if (queueQuery != null && !queueQuery.isEmpty()) {
        if (!job.getQueueName().equals(queueQuery)) {
          continue;
        }
      }

      if (userQuery != null && !userQuery.isEmpty()) {
        if (!job.getUserName().equals(userQuery)) {
          continue;
        }
      }

      JobReport report = job.getReport();
      
      if (checkStart
          && (report.getStartTime() < sBegin || report.getStartTime() > sEnd)) {
        continue;
      }
      if (checkEnd
          && (report.getFinishTime() < fBegin || report.getFinishTime() > fEnd)) {
        continue;
      }
      
      JobInfo jobInfo = new JobInfo(job);
      
      allJobs.add(jobInfo);
      num++;
    }
    return allJobs;
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobInfo getJob(@PathParam("jobid") String jid) {

    Job job = AMWebServices.getJobFromJobIdString(jid, appCtx);
    return new JobInfo(job);
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/jobattempts")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AMAttemptsInfo getJobAttempts(@PathParam("jobid") String jid) {

    Job job = AMWebServices.getJobFromJobIdString(jid, appCtx);
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
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobCounterInfo getJobCounters(@PathParam("jobid") String jid) {

    Job job = AMWebServices.getJobFromJobIdString(jid, appCtx);
    return new JobCounterInfo(this.appCtx, job);
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/conf")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ConfInfo getJobConf(@PathParam("jobid") String jid) {

    Job job = AMWebServices.getJobFromJobIdString(jid, appCtx);
    ConfInfo info;
    try {
      info = new ConfInfo(job, this.conf);
    } catch (IOException e) {
      throw new NotFoundException("unable to load configuration for job: "
          + jid);
    }
    return info;
  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/tasks")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TasksInfo getJobTasks(@PathParam("jobid") String jid,
      @QueryParam("type") String type) {

    Job job = AMWebServices.getJobFromJobIdString(jid, appCtx);
    TasksInfo allTasks = new TasksInfo();
    for (Task task : job.getTasks().values()) {
      TaskType ttype = null;
      if (type != null && !type.isEmpty()) {
        try {
          ttype = MRApps.taskType(type);
        } catch (YarnException e) {
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
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskInfo getJobTask(@PathParam("jobid") String jid,
      @PathParam("taskid") String tid) {

    Job job = AMWebServices.getJobFromJobIdString(jid, appCtx);
    Task task = AMWebServices.getTaskFromTaskIdString(tid, job);
    return new TaskInfo(task);

  }

  @GET
  @Path("/mapreduce/jobs/{jobid}/tasks/{taskid}/counters")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobTaskCounterInfo getSingleTaskCounters(
      @PathParam("jobid") String jid, @PathParam("taskid") String tid) {

    Job job = AMWebServices.getJobFromJobIdString(jid, appCtx);
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
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskAttemptsInfo getJobTaskAttempts(@PathParam("jobid") String jid,
      @PathParam("taskid") String tid) {

    TaskAttemptsInfo attempts = new TaskAttemptsInfo();
    Job job = AMWebServices.getJobFromJobIdString(jid, appCtx);
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
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskAttemptInfo getJobTaskAttemptId(@PathParam("jobid") String jid,
      @PathParam("taskid") String tid, @PathParam("attemptid") String attId) {

    Job job = AMWebServices.getJobFromJobIdString(jid, appCtx);
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
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobTaskAttemptCounterInfo getJobTaskAttemptIdCounters(
      @PathParam("jobid") String jid, @PathParam("taskid") String tid,
      @PathParam("attemptid") String attId) {

    Job job = AMWebServices.getJobFromJobIdString(jid, appCtx);
    Task task = AMWebServices.getTaskFromTaskIdString(tid, job);
    TaskAttempt ta = AMWebServices.getTaskAttemptFromTaskAttemptString(attId,
        task);
    return new JobTaskAttemptCounterInfo(ta);
  }

}
