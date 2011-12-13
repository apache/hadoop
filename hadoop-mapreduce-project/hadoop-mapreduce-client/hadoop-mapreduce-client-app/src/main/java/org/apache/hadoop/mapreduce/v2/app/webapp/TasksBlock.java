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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.util.StringHelper.percent;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR_VALUE;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
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
            th("State").
            th("Start Time").
            th("Finish Time").
            th("Elapsed Time")._()._().
        tbody();
    for (Task task : app.getJob().getTasks().values()) {
      if (type != null && task.getType() != type) {
        continue;
      }
      TaskInfo info = new TaskInfo(task);
      String tid = info.getId();
      String pct = percent(info.getProgress() / 100);
      long startTime = info.getStartTime();
      long finishTime = info.getFinishTime();
      long elapsed = info.getElapsedTime();
      tbody.
        tr().
          td().
            br().$title(String.valueOf(info.getTaskNum()))._(). // sorting
            a(url("task", tid), tid)._().
          td().
            br().$title(pct)._().
            div(_PROGRESSBAR).
              $title(join(pct, '%')). // tooltip
              div(_PROGRESSBAR_VALUE).
                $style(join("width:", pct, '%'))._()._()._().
          td(info.getState()).
          td().
            br().$title(String.valueOf(startTime))._().
            _(Times.format(startTime))._().
          td().
            br().$title(String.valueOf(finishTime))._().
            _(Times.format(finishTime))._().
          td().
            br().$title(String.valueOf(elapsed))._().
            _(StringUtils.formatTime(elapsed))._()._();
    }
    tbody._()._();
  }
}
