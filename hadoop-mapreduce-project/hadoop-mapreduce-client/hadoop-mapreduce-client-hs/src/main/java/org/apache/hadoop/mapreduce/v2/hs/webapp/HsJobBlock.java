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

import com.google.inject.Inject;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMWebApp.*;
import static org.apache.hadoop.yarn.util.StringHelper.*;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

/**
 * Render a block of HTML for a give job.
 */
public class HsJobBlock extends HtmlBlock {
  final AppContext appContext;

  int runningMapTasks = 0;
  int pendingMapTasks = 0;
  int runningReduceTasks = 0;
  int pendingReduceTasks = 0;

  int newMapAttempts = 0;
  int runningMapAttempts = 0;
  int killedMapAttempts = 0;
  int failedMapAttempts = 0;
  int successfulMapAttempts = 0;
  int newReduceAttempts = 0;
  int runningReduceAttempts = 0;
  int killedReduceAttempts = 0;
  int failedReduceAttempts = 0;
  int successfulReduceAttempts = 0;

  @Inject HsJobBlock(AppContext appctx) {
    appContext = appctx;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
   */
  @Override protected void render(Block html) {
    String jid = $(JOB_ID);
    if (jid.isEmpty()) {
      html.
        p()._("Sorry, can't do anything without a JobID.")._();
      return;
    }
    JobId jobID = MRApps.toJobID(jid);
    Job job = appContext.getJob(jobID);
    if (job == null) {
      html.
        p()._("Sorry, ", jid, " not found.")._();
      return;
    }
    JobReport jobReport = job.getReport();
    String mapPct = percent(jobReport.getMapProgress());
    String reducePct = percent(jobReport.getReduceProgress());
    int mapTasks = job.getTotalMaps();
    int mapTasksComplete = job.getCompletedMaps();
    int reduceTasks = job.getTotalReduces();
    int reducesTasksComplete = job.getCompletedReduces();
    long startTime = jobReport.getStartTime();
    long finishTime = jobReport.getFinishTime();
    countTasksAndAttempts(job);
    info("Job Overview").
        _("Job Name:", job.getName()).
        _("State:", job.getState()).
        _("Uberized:", job.isUber()).
        _("Started:", new Date(startTime)).
        _("Elapsed:", StringUtils.formatTime(
            Times.elapsed(startTime, finishTime)));
    html.
      _(InfoBlock.class).
      div(_INFO_WRAP).

      // Tasks table
        table("#job").
          tr().
            th(_TH, "Task Type").
            th(_TH, "Progress").
            th(_TH, "Total").
            th(_TH, "Pending").
            th(_TH, "Running").
            th(_TH, "Complete")._().
          tr(_ODD).
            th().
              a(url("tasks", jid, "m"), "Map")._().
            td().
              div(_PROGRESSBAR).
                $title(join(mapPct, '%')). // tooltip
                div(_PROGRESSBAR_VALUE).
                  $style(join("width:", mapPct, '%'))._()._()._().
            td(String.valueOf(mapTasks)).
            td(String.valueOf(pendingMapTasks)).
            td(String.valueOf(runningMapTasks)).
            td(String.valueOf(mapTasksComplete))._().
          tr(_EVEN).
            th().
              a(url("tasks", jid, "r"), "Reduce")._().
            td().
              div(_PROGRESSBAR).
                $title(join(reducePct, '%')). // tooltip
                div(_PROGRESSBAR_VALUE).
                  $style(join("width:", reducePct, '%'))._()._()._().
            td(String.valueOf(reduceTasks)).
            td(String.valueOf(pendingReduceTasks)).
            td(String.valueOf(runningReduceTasks)).
            td(String.valueOf(reducesTasksComplete))._()
          ._().

        // Attempts table
        table("#job").
        tr().
          th(_TH, "Attempt Type").
          th(_TH, "New").
          th(_TH, "Running").
          th(_TH, "Failed").
          th(_TH, "Killed").
          th(_TH, "Successful")._().
        tr(_ODD).
          th("Maps").
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.NEW.toString()), 
              String.valueOf(newMapAttempts))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.RUNNING.toString()), 
              String.valueOf(runningMapAttempts))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.FAILED.toString()), 
              String.valueOf(failedMapAttempts))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.KILLED.toString()), 
              String.valueOf(killedMapAttempts))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.SUCCESSFUL.toString()), 
              String.valueOf(successfulMapAttempts))._().
        _().
        tr(_EVEN).
          th("Reduces").
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.NEW.toString()), 
              String.valueOf(newReduceAttempts))._().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.RUNNING.toString()), 
              String.valueOf(runningReduceAttempts))._().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.FAILED.toString()), 
              String.valueOf(failedReduceAttempts))._().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.KILLED.toString()), 
              String.valueOf(killedReduceAttempts))._().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.SUCCESSFUL.toString()), 
              String.valueOf(successfulReduceAttempts))._().
         _().
       _().
     _();
  }

  /**
   * Go through a job and update the member variables with counts for
   * information to output in the page.
   * @param job the job to get counts for.
   */
  private void countTasksAndAttempts(Job job) {
    Map<TaskId, Task> tasks = job.getTasks();
    for (Task task : tasks.values()) {
      switch (task.getType()) {
      case MAP:
        // Task counts
        switch (task.getState()) {
        case RUNNING:
          ++runningMapTasks;
          break;
        case SCHEDULED:
          ++pendingMapTasks;
          break;
        }
        break;
      case REDUCE:
        // Task counts
        switch (task.getState()) {
        case RUNNING:
          ++runningReduceTasks;
          break;
        case SCHEDULED:
          ++pendingReduceTasks;
          break;
        }
        break;
      }

      // Attempts counts
      Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
      for (TaskAttempt attempt : attempts.values()) {

        int newAttempts = 0, running = 0, successful = 0, failed = 0, killed =0;

        if (TaskAttemptStateUI.NEW.correspondsTo(attempt.getState())) {
          ++newAttempts;
        } else if (TaskAttemptStateUI.RUNNING.correspondsTo(attempt
            .getState())) {
          ++running;
        } else if (TaskAttemptStateUI.SUCCESSFUL.correspondsTo(attempt
            .getState())) {
          ++successful;
        } else if (TaskAttemptStateUI.FAILED
            .correspondsTo(attempt.getState())) {
          ++failed;
        } else if (TaskAttemptStateUI.KILLED
            .correspondsTo(attempt.getState())) {
          ++killed;
        }

        switch (task.getType()) {
        case MAP:
          newMapAttempts += newAttempts;
          runningMapAttempts += running;
          successfulMapAttempts += successful;
          failedMapAttempts += failed;
          killedMapAttempts += killed;
          break;
        case REDUCE:
          newReduceAttempts += newAttempts;
          runningReduceAttempts += running;
          successfulReduceAttempts += successful;
          failedReduceAttempts += failed;
          killedReduceAttempts += killed;
          break;
        }
      }
    }
  }
}
