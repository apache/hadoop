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

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.View;

import com.google.inject.Inject;

/**
 * This class renders the various pages that the web app supports.
 */
public class AppController extends Controller implements AMParams {
  final App app;
  
  protected AppController(App app, Configuration conf, RequestContext ctx,
      String title) {
    super(ctx);
    this.app = app;
    set(APP_ID, app.context.getApplicationID().toString());
    set(RM_WEB, YarnConfiguration.getRMWebAppURL(conf));
  }

  @Inject
  protected AppController(App app, Configuration conf, RequestContext ctx) {
    this(app, conf, ctx, "am");
  }

  /**
   * Render the default(index.html) page for the Application Controller
   */
  @Override public void index() {
    setTitle(join("MapReduce Application ", $(APP_ID)));
  }

  /**
   * Render the /info page with an overview of current application.
   */
  public void info() {
    info("Application Master Overview").
      _("Application ID:", $(APP_ID)).
      _("Application Name:", app.context.getApplicationName()).
      _("User:", app.context.getUser()).
      _("Started on:", Times.format(app.context.getStartTime())).
      _("Elasped: ", org.apache.hadoop.util.StringUtils.formatTime(
        Times.elapsed(app.context.getStartTime(), 0)));
    render(InfoPage.class);
  }

  /**
   * @return The class that will render the /job page
   */
  protected Class<? extends View> jobPage() {
    return JobPage.class;
  }
  
  /**
   * Render the /job page
   */
  public void job() {
    requireJob();
    render(jobPage());
  }

  /**
   * @return the class that will render the /jobcounters page
   */
  protected Class<? extends View> countersPage() {
    return CountersPage.class;
  }
  
  /**
   * Render the /jobcounters page
   */
  public void jobCounters() {
    requireJob();
    if (app.getJob() != null) {
      setTitle(join("Counters for ", $(JOB_ID)));
    }
    render(countersPage());
  }

  /**
   * @return the class that will render the /tasks page
   */
  protected Class<? extends View> tasksPage() {
    return TasksPage.class;
  }
  
  /**
   * Render the /tasks page
   */
  public void tasks() {
    requireJob();
    if (app.getJob() != null) {
      try {
        String tt = $(TASK_TYPE);
        tt = tt.isEmpty() ? "All" : StringUtils.capitalize(MRApps.taskType(tt).
            toString().toLowerCase(Locale.US));
        setTitle(join(tt, " Tasks for ", $(JOB_ID)));
      } catch (Exception e) {
        badRequest(e.getMessage());
      }
    }
    render(tasksPage());
  }
  
  /**
   * @return the class that will render the /task page
   */
  protected Class<? extends View> taskPage() {
    return TaskPage.class;
  }
  
  /**
   * Render the /task page
   */
  public void task() {
    requireTask();
    if (app.getTask() != null) {
      setTitle(join("Attempts for ", $(TASK_ID)));
    }
    render(taskPage());
  }

  /**
   * @return the class that will render the /attempts page
   */
  protected Class<? extends View> attemptsPage() {
    return AttemptsPage.class;
  }
  
  /**
   * Render the attempts page
   */
  public void attempts() {
    requireJob();
    if (app.getJob() != null) {
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

        render(attemptsPage());
      } catch (Exception e) {
        badRequest(e.getMessage());
      }
    }
  }

  /**
   * @return the page that will be used to render the /conf page
   */
  protected Class<? extends View> confPage() {
    return JobConfPage.class;
  }

  /**
   * Render the /conf page
   */
  public void conf() {
    requireJob();
    render(confPage());
  }

  /**
   * Render a BAD_REQUEST error.
   * @param s the error message to include.
   */
  void badRequest(String s) {
    setStatus(HttpServletResponse.SC_BAD_REQUEST);
    setTitle(join("Bad request: ", s));
  }

  /**
   * Render a NOT_FOUND error.
   * @param s the error message to include.
   */
  void notFound(String s) {
    setStatus(HttpServletResponse.SC_NOT_FOUND);
    setTitle(join("Not found: ", s));
  }

  /**
   * Ensure that a JOB_ID was passed into the page.
   */
  public void requireJob() {
    try {
      if ($(JOB_ID).isEmpty()) {
        throw new RuntimeException("missing job ID");
      }
      JobId jobID = MRApps.toJobID($(JOB_ID));
      app.setJob(app.context.getJob(jobID));
      if (app.getJob() == null) {
        notFound($(JOB_ID));
      }
    } catch (Exception e) {
      badRequest(e.getMessage() == null ? 
          e.getClass().getName() : e.getMessage());
    }
  }

  /**
   * Ensure that a TASK_ID was passed into the page.
   */
  public void requireTask() {
    try {
      if ($(TASK_ID).isEmpty()) {
        throw new RuntimeException("missing task ID");
      }
      TaskId taskID = MRApps.toTaskID($(TASK_ID));
      app.setJob(app.context.getJob(taskID.getJobId()));
      if (app.getJob() == null) {
        notFound(MRApps.toString(taskID.getJobId()));
      } else {
        app.setTask(app.getJob().getTask(taskID));
        if (app.getTask() == null) {
          notFound($(TASK_ID));
        }
      }
    } catch (Exception e) {
      badRequest(e.getMessage());
    }
  }
}
