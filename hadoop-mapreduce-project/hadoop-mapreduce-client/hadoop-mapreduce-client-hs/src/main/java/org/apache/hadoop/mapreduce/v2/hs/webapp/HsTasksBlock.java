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

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ReduceTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
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

    // Write all the data into a JavaScript array of arrays for JQuery
    // DataTables to display
    StringBuilder tasksTableData = new StringBuilder("[\n");
    for (Task task : app.getJob().getTasks().values()) {
      if (type != null && task.getType() != type) {
        continue;
      }
      TaskInfo info = new TaskInfo(task);
      String tid = info.getId();

      long startTime = info.getStartTime();
      long finishTime = info.getFinishTime();
      long elapsed = info.getElapsedTime();

      long attemptStartTime = -1;
      long shuffleFinishTime = -1;
      long sortFinishTime = -1;
      long attemptFinishTime = -1;
      long elapsedShuffleTime = -1;
      long elapsedSortTime = -1;;
      long elapsedReduceTime = -1;
      long attemptElapsed = -1;
      TaskAttempt successful = info.getSuccessful();
      if(successful != null) {
        TaskAttemptInfo ta;
        if(type == TaskType.REDUCE) {
          ReduceTaskAttemptInfo rta = new ReduceTaskAttemptInfo(successful, type);
          shuffleFinishTime = rta.getShuffleFinishTime();
          sortFinishTime = rta.getMergeFinishTime();
          elapsedShuffleTime = rta.getElapsedShuffleTime();
          elapsedSortTime = rta.getElapsedMergeTime();
          elapsedReduceTime = rta.getElapsedReduceTime();
          ta = rta;
        } else {
          ta = new TaskAttemptInfo(successful, type, false);
        }
        attemptStartTime = ta.getStartTime();
        attemptFinishTime = ta.getFinishTime();
        attemptElapsed = ta.getElapsedTime();
      }

      tasksTableData.append("[\"")
      .append("<a href='" + url("task", tid)).append("'>")
      .append(tid).append("</a>\",\"")
      .append(info.getState()).append("\",\"")
      .append(startTime).append("\",\"")
      .append(finishTime).append("\",\"")
      .append(elapsed).append("\",\"")
      .append(attemptStartTime).append("\",\"");
      if(type == TaskType.REDUCE) {
        tasksTableData.append(shuffleFinishTime).append("\",\"")
        .append(sortFinishTime).append("\",\"");
      }
      tasksTableData.append(attemptFinishTime).append("\",\"");
      if(type == TaskType.REDUCE) {
        tasksTableData.append(elapsedShuffleTime).append("\",\"")
        .append(elapsedSortTime).append("\",\"")
        .append(elapsedReduceTime).append("\",\"");
      }
      tasksTableData.append(attemptElapsed).append("\"],\n");
    }
    //Remove the last comma and close off the array of arrays
    if(tasksTableData.charAt(tasksTableData.length() - 2) == ',') {
      tasksTableData.delete(tasksTableData.length()-2, tasksTableData.length()-1);
    }
    tasksTableData.append("]");
    html.script().$type("text/javascript").
    _("var tasksTableData=" + tasksTableData)._();
    
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
}
