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

import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
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
    TBODY<TABLE<Hamlet>> tbody = html.
      table("#tasks").
        thead().
          tr().
            th("Task").
            th("State").
            th("Start Time").
            th("Finish Time").
            th("Elapsed Time")._()._().
        tbody();
    for (Task task : app.getJob().getTasks().values()) {
      if (type != null && task.getType() != type) {
        continue;
      }
      String tid = MRApps.toString(task.getID());
      TaskReport report = task.getReport();
      long startTime = report.getStartTime();
      long finishTime = report.getFinishTime();
      long elapsed = Times.elapsed(startTime, finishTime);
      tbody.
        tr().
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
            _(StringUtils.formatTime(elapsed))._()._();
    }
    tbody._()._();
  }
}
