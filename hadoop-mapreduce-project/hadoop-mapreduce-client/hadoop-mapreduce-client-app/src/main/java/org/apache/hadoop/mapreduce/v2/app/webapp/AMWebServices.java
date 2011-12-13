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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AppInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ReduceTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TasksInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.google.inject.Inject;

@Path("/ws/v1/mapreduce")
public class AMWebServices {
  private final AppContext appCtx;
  private final App app;
  private final Configuration conf;

  @Inject
  public AMWebServices(final App app, final AppContext context,
      final Configuration conf) {
    this.appCtx = context;
    this.app = app;
    this.conf = conf;
  }

  Boolean hasAccess(Job job, HttpServletRequest request) {
    UserGroupInformation callerUgi = UserGroupInformation
        .createRemoteUser(request.getRemoteUser());
    if (!job.checkAccess(callerUgi, JobACL.VIEW_JOB)) {
      return false;
    }
    return true;
  }

  /**
   * check for job access.
   *
   * @param job
   *          the job that is being accessed
   */
  void checkAccess(Job job, HttpServletRequest request) {
    if (!hasAccess(job, request)) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppInfo get() {
    return getAppInfo();
  }

  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppInfo getAppInfo() {
    return new AppInfo(this.app, this.app.context);
  }

  @GET
  @Path("/jobs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobsInfo getJobs(@Context HttpServletRequest hsr) {
    JobsInfo allJobs = new JobsInfo();
    for (Job job : appCtx.getAllJobs().values()) {
      // getAllJobs only gives you a partial we want a full
      Job fullJob = appCtx.getJob(job.getID());
      if (fullJob == null) {
        continue;
      }
      allJobs.add(new JobInfo(fullJob, hasAccess(fullJob, hsr)));
    }
    return allJobs;
  }

  @GET
  @Path("/jobs/{jobid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobInfo getJob(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {
    JobId jobId = MRApps.toJobID(jid);
    if (jobId == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    Job job = appCtx.getJob(jobId);
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    return new JobInfo(job, hasAccess(job, hsr));

  }

  @GET
  @Path("/jobs/{jobid}/counters")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobCounterInfo getJobCounters(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {
    JobId jobId = MRApps.toJobID(jid);
    if (jobId == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    Job job = appCtx.getJob(jobId);
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    checkAccess(job, hsr);
    return new JobCounterInfo(this.appCtx, job);
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/counters")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobTaskCounterInfo getSingleTaskCounters(
      @Context HttpServletRequest hsr, @PathParam("jobid") String jid,
      @PathParam("taskid") String tid) {
    JobId jobId = MRApps.toJobID(jid);
    if (jobId == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    Job job = this.appCtx.getJob(jobId);
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
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
  @Path("/jobs/{jobid}/conf")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ConfInfo getJobConf(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {
    JobId jobId = MRApps.toJobID(jid);
    if (jobId == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    Job job = appCtx.getJob(jobId);
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    checkAccess(job, hsr);
    ConfInfo info;
    try {
      info = new ConfInfo(job, this.conf);
    } catch (IOException e) {
      throw new NotFoundException("unable to load configuration for job: " + jid);
    }
    return info;
  }

  @GET
  @Path("/jobs/{jobid}/tasks")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TasksInfo getJobTasks(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @QueryParam("type") String type) {
    Job job = this.appCtx.getJob(MRApps.toJobID(jid));
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    checkAccess(job, hsr);
    TasksInfo allTasks = new TasksInfo();
    for (Task task : job.getTasks().values()) {
      TaskType ttype = null;
      if (type != null && !type.isEmpty()) {
        try {
          ttype = MRApps.taskType(type);
        } catch (YarnException e) {
          throw new BadRequestException("tasktype must be either m or r");        }
      }
      if (ttype != null && task.getType() != ttype) {
        continue;
      }
      allTasks.add(new TaskInfo(task));
    }
    return allTasks;
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskInfo getJobTask(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid) {
    Job job = this.appCtx.getJob(MRApps.toJobID(jid));
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    checkAccess(job, hsr);
    TaskId taskID = MRApps.toTaskID(tid);
    if (taskID == null) {
      throw new NotFoundException("taskid " + tid + " not found or invalid");
    }
    Task task = job.getTask(taskID);
    if (task == null) {
      throw new NotFoundException("task not found with id " + tid);
    }
    return new TaskInfo(task);

  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskAttemptsInfo getJobTaskAttempts(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid) {
    TaskAttemptsInfo attempts = new TaskAttemptsInfo();
    Job job = this.appCtx.getJob(MRApps.toJobID(jid));
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    checkAccess(job, hsr);
    TaskId taskID = MRApps.toTaskID(tid);
    if (taskID == null) {
      throw new NotFoundException("taskid " + tid + " not found or invalid");
    }
    Task task = job.getTask(taskID);
    if (task == null) {
      throw new NotFoundException("task not found with id " + tid);
    }
    for (TaskAttempt ta : task.getAttempts().values()) {
      if (ta != null) {
        if (task.getType() == TaskType.REDUCE) {
          attempts.add(new ReduceTaskAttemptInfo(ta, task.getType()));
        } else {
          attempts.add(new TaskAttemptInfo(ta, task.getType(), true));
        }
      }
    }
    return attempts;
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskAttemptInfo getJobTaskAttemptId(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid,
      @PathParam("attemptid") String attId) {
    Job job = this.appCtx.getJob(MRApps.toJobID(jid));
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    checkAccess(job, hsr);
    TaskId taskID = MRApps.toTaskID(tid);
    if (taskID == null) {
      throw new NotFoundException("taskid " + tid + " not found or invalid");
    }
    Task task = job.getTask(taskID);
    if (task == null) {
      throw new NotFoundException("task not found with id " + tid);
    }
    TaskAttemptId attemptId = MRApps.toTaskAttemptID(attId);
    if (attemptId == null) {
      throw new NotFoundException("task attempt id " + attId
          + " not found or invalid");
    }
    TaskAttempt ta = task.getAttempt(attemptId);
    if (ta == null) {
      throw new NotFoundException("Error getting info on task attempt id "
          + attId);
    }
    if (task.getType() == TaskType.REDUCE) {
      return new ReduceTaskAttemptInfo(ta, task.getType());
    } else {
      return new TaskAttemptInfo(ta, task.getType(), true);
    }
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobTaskAttemptCounterInfo getJobTaskAttemptIdCounters(
      @Context HttpServletRequest hsr, @PathParam("jobid") String jid,
      @PathParam("taskid") String tid, @PathParam("attemptid") String attId) {
    JobId jobId = MRApps.toJobID(jid);
    if (jobId == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    Job job = this.appCtx.getJob(jobId);
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    checkAccess(job, hsr);
    TaskId taskID = MRApps.toTaskID(tid);
    if (taskID == null) {
      throw new NotFoundException("taskid " + tid + " not found or invalid");
    }
    Task task = job.getTask(taskID);
    if (task == null) {
      throw new NotFoundException("task not found with id " + tid);
    }
    TaskAttemptId attemptId = MRApps.toTaskAttemptID(attId);
    if (attemptId == null) {
      throw new NotFoundException("task attempt id " + attId
          + " not found or invalid");
    }
    TaskAttempt ta = task.getAttempt(attemptId);
    if (ta == null) {
      throw new NotFoundException("Error getting info on task attempt id "
          + attId);
    }
    return new JobTaskAttemptCounterInfo(ta);
  }
}
