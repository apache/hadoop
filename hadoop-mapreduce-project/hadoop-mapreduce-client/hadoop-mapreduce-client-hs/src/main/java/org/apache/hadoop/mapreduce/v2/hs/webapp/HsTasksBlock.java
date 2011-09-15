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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TFOOT;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * Render the a table of tasks for a given type.
 */
public class HsTasksBlock extends HtmlBlock {
  final App app;

  @Inject HsTasksBlock(App app) {
    this.app = app;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
   */
  @Override protected void render(Block html) {
    if (app.getJob() == null) {
      html.
        h2($(TITLE));
      return;
    }
    TaskType type = null;
    String symbol = $(TASK_TYPE);
    if (!symbol.isEmpty()) {
      type = MRApps.taskType(symbol);
    }
    
    THEAD<TABLE<Hamlet>> thead = html.table("#tasks").thead();
    //Create the spanning row
    int attemptColSpan = type == TaskType.REDUCE ? 8 : 3;
    thead.tr().
      th().$colspan(5).$class("ui-state-default")._("Task")._().
      th().$colspan(attemptColSpan).$class("ui-state-default").
        _("Successful Attempt")._().
    _();
    
    TR<THEAD<TABLE<Hamlet>>> theadRow = thead.
          tr().
            th("Name").
            th("State").
            th("Start Time").
            th("Finish Time").
            th("Elapsed Time").
            th("Start Time"); //Attempt
    
    if(type == TaskType.REDUCE) {
      theadRow.th("Shuffle Finish Time"); //Attempt
      theadRow.th("Merge Finish Time"); //Attempt
    }
    
    theadRow.th("Finish Time"); //Attempt
    
    if(type == TaskType.REDUCE) {
      theadRow.th("Elapsed Time Shuffle"); //Attempt
      theadRow.th("Elapsed Time Merge"); //Attempt
      theadRow.th("Elapsed Time Reduce"); //Attempt
    }
    theadRow.th("Elapsed Time"); //Attempt
    
    TBODY<TABLE<Hamlet>> tbody = theadRow._()._().tbody();
    for (Task task : app.getJob().getTasks().values()) {
      if (type != null && task.getType() != type) {
        continue;
      }
      String tid = MRApps.toString(task.getID());
      
      TaskReport report = task.getReport();
      long startTime = report.getStartTime();
      long finishTime = report.getFinishTime();
      long elapsed = Times.elapsed(startTime, finishTime, false);
      
      long attemptStartTime = -1;
      long shuffleFinishTime = -1;
      long sortFinishTime = -1;
      long attemptFinishTime = -1;
      long elapsedShuffleTime = -1;
      long elapsedSortTime = -1;;
      long elapsedReduceTime = -1;
      long attemptElapsed = -1;
      TaskAttempt successful = getSuccessfulAttempt(task);
      if(successful != null) {
        attemptStartTime = successful.getLaunchTime();
        attemptFinishTime = successful.getFinishTime();
        if(type == TaskType.REDUCE) {
          shuffleFinishTime = successful.getShuffleFinishTime();
          sortFinishTime = successful.getSortFinishTime();
          elapsedShuffleTime =
              Times.elapsed(attemptStartTime, shuffleFinishTime, false);
          elapsedSortTime =
              Times.elapsed(shuffleFinishTime, sortFinishTime, false);
          elapsedReduceTime =
              Times.elapsed(sortFinishTime, attemptFinishTime, false); 
        }
        attemptElapsed =
            Times.elapsed(attemptStartTime, attemptFinishTime, false);
      }
      
      TR<TBODY<TABLE<Hamlet>>> row = tbody.tr();
      row.
          td().
            br().$title(String.valueOf(task.getID().getId()))._(). // sorting
            a(url("task", tid), tid)._().
          td(report.getTaskState().toString()).
          td().
            br().$title(String.valueOf(startTime))._().
            _(Times.format(startTime))._().
          td().
            br().$title(String.valueOf(finishTime))._().
            _(Times.format(finishTime))._().
          td().
            br().$title(String.valueOf(elapsed))._().
            _(formatTime(elapsed))._().
          td().
            br().$title(String.valueOf(attemptStartTime))._().
            _(Times.format(attemptStartTime))._();
      if(type == TaskType.REDUCE) {
        row.td().
          br().$title(String.valueOf(shuffleFinishTime))._().
          _(Times.format(shuffleFinishTime))._();
        row.td().
        br().$title(String.valueOf(sortFinishTime))._().
        _(Times.format(sortFinishTime))._();
      }
      row.
          td().
            br().$title(String.valueOf(attemptFinishTime))._().
            _(Times.format(attemptFinishTime))._();
      
      if(type == TaskType.REDUCE) {
        row.td().
          br().$title(String.valueOf(elapsedShuffleTime))._().
        _(formatTime(elapsedShuffleTime))._();
        row.td().
        br().$title(String.valueOf(elapsedSortTime))._().
      _(formatTime(elapsedSortTime))._();
        row.td().
          br().$title(String.valueOf(elapsedReduceTime))._().
        _(formatTime(elapsedReduceTime))._();
      }
      
      row.td().
        br().$title(String.valueOf(attemptElapsed))._().
        _(formatTime(attemptElapsed))._();
      row._();
    }
    TR<TFOOT<TABLE<Hamlet>>> footRow = tbody._().tfoot().tr();
    footRow.th().input("search_init").$type(InputType.text).$name("task")
        .$value("ID")._()._().th().input("search_init").$type(InputType.text)
        .$name("state").$value("State")._()._().th().input("search_init")
        .$type(InputType.text).$name("start_time").$value("Start Time")._()._()
        .th().input("search_init").$type(InputType.text).$name("finish_time")
        .$value("Finish Time")._()._().th().input("search_init")
        .$type(InputType.text).$name("elapsed_time").$value("Elapsed Time")._()
        ._().th().input("search_init").$type(InputType.text)
        .$name("attempt_start_time").$value("Start Time")._()._();
    
    if(type == TaskType.REDUCE) {
      footRow.th().input("search_init").$type(InputType.text)
          .$name("shuffle_time").$value("Shuffle Time")._()._();
      footRow.th().input("search_init").$type(InputType.text)
          .$name("merge_time").$value("Merge Time")._()._();
    }

    footRow.th().input("search_init").$type(InputType.text)
        .$name("attempt_finish").$value("Finish Time")._()._();

    if(type == TaskType.REDUCE) {
      footRow.th().input("search_init").$type(InputType.text)
          .$name("elapsed_shuffle_time").$value("Elapsed Shuffle Time")._()._();
      footRow.th().input("search_init").$type(InputType.text)
          .$name("elapsed_merge_time").$value("Elapsed Merge Time")._()._();
      footRow.th().input("search_init").$type(InputType.text)
          .$name("elapsed_reduce_time").$value("Elapsed Reduce Time")._()._();
    }

    footRow.th().input("search_init").$type(InputType.text)
        .$name("attempt_elapsed").$value("Elapsed Time")._()._();
    
    footRow._()._()._();
  }

  private String formatTime(long elapsed) {
    return elapsed < 0 ? "N/A" : StringUtils.formatTime(elapsed);
  }
  
  private TaskAttempt getSuccessfulAttempt(Task task) {
    for(TaskAttempt attempt: task.getAttempts().values()) {
      if(attempt.getState() == TaskAttemptState.SUCCEEDED) {
        return attempt;
      }
    }
    return null;
  }
}
