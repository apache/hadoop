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

import static org.apache.hadoop.yarn.util.StringHelper.join;

import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.Controller;

import com.google.inject.Inject;

public class AppController extends Controller implements AMParams {
  final App app;
  
  protected AppController(App app, Configuration conf, RequestContext ctx,
      String title) {
    super(ctx);
    this.app = app;
    set(APP_ID, Apps.toString(app.context.getApplicationID()));
    set(RM_WEB, YarnConfiguration.getRMWebAppURL(conf));
  }

  @Inject
  protected AppController(App app, Configuration conf, RequestContext ctx) {
    this(app, conf, ctx, "am");
  }

  @Override public void index() {
    setTitle(join("MapReduce Application ", $(APP_ID)));
  }

  public void info() {
    info("Application Master Overview").
      _("Application ID:", $(APP_ID)).
      _("Application Name:", app.context.getApplicationName()).
      _("User:", app.context.getUser()).
      _("Started on:", Times.format(app.context.getStartTime())).
      _("Elasped: ", Times.elapsed(app.context.getStartTime(), 0));
    render(InfoPage.class);
  }

  public void job() {
    requireJob();
    render(JobPage.class);
  }

  public void jobCounters() {
    requireJob();
    if (app.job != null) {
      setTitle(join("Counters for ", $(JOB_ID)));
    }
    render(CountersPage.class);
  }

  public void tasks() {
    requireJob();
    if (app.job != null) {
      try {
        String tt = $(TASK_TYPE);
        tt = tt.isEmpty() ? "All" : StringUtils.capitalize(MRApps.taskType(tt).
            toString().toLowerCase(Locale.US));
        setTitle(join(tt, " Tasks for ", $(JOB_ID)));
      } catch (Exception e) {
        badRequest(e.getMessage());
      }
    }
    render(TasksPage.class);
  }
  
  public void task() {
    requireTask();
    if (app.task != null) {
      setTitle(join("Attempts for ", $(TASK_ID)));
    }
    render(TaskPage.class);
  }

  public void attempts() {
    requireJob();
    if (app.job != null) {
      try {
        String taskType = $(TASK_TYPE);
        if (taskType.isEmpty()) {
          throw new RuntimeException("missing task-type.");
        }
        String attemptState = $(ATTEMPT_STATE);
        if (attemptState.isEmpty()) {
          throw new RuntimeException("missing attempt-state.");
        }
        setTitle(join(attemptState, " ",
            MRApps.taskType(taskType).toString(), " attempts in ", $(JOB_ID)));
      } catch (Exception e) {
        badRequest(e.getMessage());
      }
    }
    render(AttemptsPage.class);
  }

  void badRequest(String s) {
    setStatus(response().SC_BAD_REQUEST);
    setTitle(join("Bad request: ", s));
  }

  void notFound(String s) {
    setStatus(response().SC_NOT_FOUND);
    setTitle(join("Not found: ", s));
  }

  void requireJob() {
    try {
      if ($(JOB_ID).isEmpty()) {
        throw new RuntimeException("missing job ID");
      }
      JobId jobID = MRApps.toJobID($(JOB_ID));
      app.job = app.context.getJob(jobID);
      if (app.job == null) {
        notFound($(JOB_ID));
      }
    } catch (Exception e) {
      badRequest(e.getMessage() == null ? e.getClass().getName() : e.getMessage());
    }
  }

  void requireTask() {
    try {
      if ($(TASK_ID).isEmpty()) {
        throw new RuntimeException("missing task ID");
      }
      TaskId taskID = MRApps.toTaskID($(TASK_ID));
      app.job = app.context.getJob(taskID.getJobId());
      if (app.job == null) {
        notFound(MRApps.toString(taskID.getJobId()));
      } else {
        app.task = app.job.getTask(taskID);
        if (app.task == null) {
          notFound($(TASK_ID));
        }
      }
    } catch (Exception e) {
      badRequest(e.getMessage());
    }
  }
}
