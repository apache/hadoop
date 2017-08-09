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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_STATE;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class TasksBlock extends HtmlBlock {
  final App app;

  @Inject TasksBlock(App app) {
    this.app = app;
  }

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
    TBODY<TABLE<Hamlet>> tbody = html.
      table("#tasks").
        thead().
          tr().
            th("Task").
            th("Progress").
            th("Status").
            th("State").
            th("Start Time").
            th("Finish Time").
            th("Elapsed Time").__().__().
        tbody();
    StringBuilder tasksTableData = new StringBuilder("[\n");

    for (Task task : app.getJob().getTasks().values()) {
      if (type != null && task.getType() != type) {
        continue;
      }
      String taskStateStr = $(TASK_STATE);
      if (taskStateStr == null || taskStateStr.trim().equals("")) {
        taskStateStr = "ALL";
      }

      if (!taskStateStr.equalsIgnoreCase("ALL"))
      {
        try {
          // get stateUI enum
          MRApps.TaskStateUI stateUI = MRApps.taskState(taskStateStr);
          if (!stateUI.correspondsTo(task.getState()))
          {
            continue;
          }
        } catch (IllegalArgumentException e) {
          continue; // not supported state, ignore
        }
      }

      TaskInfo info = new TaskInfo(task);
      String tid = info.getId();
      String pct = StringUtils.format("%.2f", info.getProgress());
      tasksTableData.append("[\"<a href='").append(url("task", tid))
      .append("'>").append(tid).append("</a>\",\"")
      //Progress bar
      .append("<br title='").append(pct)
      .append("'> <div class='").append(C_PROGRESSBAR).append("' title='")
      .append(join(pct, '%')).append("'> ").append("<div class='")
      .append(C_PROGRESSBAR_VALUE).append("' style='")
      .append(join("width:", pct, '%')).append("'> </div> </div>\",\"")
      .append(StringEscapeUtils.escapeJavaScript(
              StringEscapeUtils.escapeHtml(info.getStatus()))).append("\",\"")

      .append(info.getState()).append("\",\"")
      .append(info.getStartTime()).append("\",\"")
      .append(info.getFinishTime()).append("\",\"")
      .append(info.getElapsedTime()).append("\"],\n");
    }
    //Remove the last comma and close off the array of arrays
    if(tasksTableData.charAt(tasksTableData.length() - 2) == ',') {
      tasksTableData.delete(tasksTableData.length()-2, tasksTableData.length()-1);
    }
    tasksTableData.append("]");
    html.script().$type("text/javascript").
        __("var tasksTableData=" + tasksTableData).__();

    tbody.__().__();
  }
}
