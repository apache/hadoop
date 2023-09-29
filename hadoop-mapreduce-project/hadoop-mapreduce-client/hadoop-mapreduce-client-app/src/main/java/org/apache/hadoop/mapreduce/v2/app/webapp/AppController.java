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

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AppInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class renders the various pages that the web app supports.
 */
public class AppController extends Controller implements AMParams {
  private static final Logger LOG =
      LoggerFactory.getLogger(AppController.class);
  private static final Joiner JOINER = Joiner.on("");
  
  protected final App app;
  
  protected AppController(App app, Configuration conf, RequestContext ctx,
      String title) {
    super(ctx);
    this.app = app;
    set(APP_ID, app.context.getApplicationID().toString());
    set(RM_WEB,
        JOINER.join(MRWebAppUtil.getYARNWebappScheme(),
            WebAppUtils.getResolvedRemoteRMWebAppURLWithoutScheme(conf,
                MRWebAppUtil.getYARNHttpPolicy())));
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
    AppInfo info = new AppInfo(app, app.context);
    info("Application Master Overview").
        __("Application ID:", info.getId()).
        __("Application Name:", info.getName()).
        __("User:", info.getUser()).
        __("Started on:", Times.format(info.getStartTime())).
        __("Elasped: ", org.apache.hadoop.util.StringUtils.formatTime(
          info.getElapsedTime() ));
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
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
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
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    if (app.getJob() != null) {
      setTitle(join("Counters for ", $(JOB_ID)));
    }
    render(countersPage());
  }
  
  /**
   * Display a page showing a task's counters
   */
  public void taskCounters() {
    try {
      requireTask();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    if (app.getTask() != null) {
      setTitle(StringHelper.join("Counters for ", $(TASK_ID)));
    }
    render(countersPage());
  }
  
  /**
   * @return the class that will render the /singlejobcounter page
   */
  protected Class<? extends View> singleCounterPage() {
    return SingleCounterPage.class;
  }
  
  /**
   * Render the /singlejobcounter page
   * @throws IOException on any error.
   */
  public void singleJobCounter() throws IOException{
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    set(COUNTER_GROUP, URLDecoder.decode($(COUNTER_GROUP), "UTF-8"));
    set(COUNTER_NAME, URLDecoder.decode($(COUNTER_NAME), "UTF-8"));
    if (app.getJob() != null) {
      setTitle(StringHelper.join($(COUNTER_GROUP)," ",$(COUNTER_NAME),
          " for ", $(JOB_ID)));
    }
    render(singleCounterPage());
  }
  
  /**
   * Render the /singletaskcounter page
   * @throws IOException on any error.
   */
  public void singleTaskCounter() throws IOException{
    try {
      requireTask();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    set(COUNTER_GROUP, URLDecoder.decode($(COUNTER_GROUP), "UTF-8"));
    set(COUNTER_NAME, URLDecoder.decode($(COUNTER_NAME), "UTF-8"));
    if (app.getTask() != null) {
      setTitle(StringHelper.join($(COUNTER_GROUP)," ",$(COUNTER_NAME),
          " for ", $(TASK_ID)));
    }
    render(singleCounterPage());
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
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    if (app.getJob() != null) {
      try {
        String tt = $(TASK_TYPE);
        tt = tt.isEmpty() ? "All" : StringUtils.capitalize(
            org.apache.hadoop.util.StringUtils.toLowerCase(
                MRApps.taskType(tt).toString()));
        setTitle(join(tt, " Tasks for ", $(JOB_ID)));
      } catch (Exception e) {
        LOG.error("Failed to render tasks page with task type : "
            + $(TASK_TYPE) + " for job id : " + $(JOB_ID), e);
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
    try {
      requireTask();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
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
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
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
        LOG.error("Failed to render attempts page with task type : "
            + $(TASK_TYPE) + " for job id : " + $(JOB_ID), e);
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
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    render(confPage());
  }

  /**
   * Handle requests to download the job configuration.
   */
  public void downloadConf() {
    try {
      requireJob();
    } catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    writeJobConf();
  }

  private void writeJobConf() {
    String jobId = $(JOB_ID);
    assert(!jobId.isEmpty());

    JobId jobID = MRApps.toJobID($(JOB_ID));
    Job job = app.context.getJob(jobID);
    assert(job != null);

    try {
      Configuration jobConf = job.loadConfFile();
      response().setContentType("text/xml");
      response().setHeader("Content-Disposition",
          "attachment; filename=" + jobId + ".xml");
      jobConf.writeXml(writer());
    } catch (IOException e) {
      LOG.error("Error reading/writing job" +
          " conf file for job: " + jobId, e);
      renderText(e.getMessage());
    }
  }

  /**
   * Render a BAD_REQUEST error.
   * @param s the error message to include.
   */
  void badRequest(String s) {
    setStatus(HttpServletResponse.SC_BAD_REQUEST);
    String title = "Bad request: ";
    setTitle((s != null) ? join(title, s) : title);
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
   * Render a ACCESS_DENIED error.
   * @param s the error message to include.
   */
  void accessDenied(String s) {
    setStatus(HttpServletResponse.SC_FORBIDDEN);
    setTitle(join("Access denied: ", s));
  }

  /**
   * check for job access.
   * @param job the job that is being accessed
   * @return True if the requesting user has permission to view the job
   */
  boolean checkAccess(Job job) {
    String remoteUser = request().getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null && !job.checkAccess(callerUGI, JobACL.VIEW_JOB)) {
      return false;
    }
    return true;
  }

  /**
   * Ensure that a JOB_ID was passed into the page.
   */
  public void requireJob() {
    if ($(JOB_ID).isEmpty()) {
      badRequest("missing job ID");
      throw new RuntimeException("Bad Request: Missing job ID");
    }

    JobId jobID = MRApps.toJobID($(JOB_ID));
    app.setJob(app.context.getJob(jobID));
    if (app.getJob() == null) {
      notFound($(JOB_ID));
      throw new RuntimeException("Not Found: " + $(JOB_ID));
    }

    /* check for acl access */
    Job job = app.context.getJob(jobID);
    if (!checkAccess(job)) {
      accessDenied("User " + request().getRemoteUser() + " does not have " +
          " permission to view job " + $(JOB_ID));
      throw new RuntimeException("Access denied: User " +
          request().getRemoteUser() + " does not have permission to view job " +
          $(JOB_ID));
    }
  }

  /**
   * Ensure that a TASK_ID was passed into the page.
   */
  public void requireTask() {
    if ($(TASK_ID).isEmpty()) {
      badRequest("missing task ID");
      throw new RuntimeException("missing task ID");
    }

    TaskId taskID = MRApps.toTaskID($(TASK_ID));
    Job job = app.context.getJob(taskID.getJobId());
    app.setJob(job);
    if (app.getJob() == null) {
      notFound(MRApps.toString(taskID.getJobId()));
      throw new RuntimeException("Not Found: " + $(JOB_ID));
    } else {
      app.setTask(app.getJob().getTask(taskID));
      if (app.getTask() == null) {
        notFound($(TASK_ID));
        throw new RuntimeException("Not Found: " + $(TASK_ID));
      }
    }
    if (!checkAccess(job)) {
      accessDenied("User " + request().getRemoteUser() + " does not have " +
          " permission to view job " + $(JOB_ID));
      throw new RuntimeException("Access denied: User " +
          request().getRemoteUser() + " does not have permission to view job " +
          $(JOB_ID));
    }
  }
}
