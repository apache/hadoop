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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import java.util.Collection;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class TaskPage extends AppView {

  static class AttemptsBlock extends HtmlBlock {
    final App app;
    final boolean enableUIActions;
    private String stateURLFormat;

    @Inject
    AttemptsBlock(App ctx, Configuration conf) {
      app = ctx;
      this.enableUIActions =
          conf.getBoolean(MRConfig.MASTER_WEBAPP_UI_ACTIONS_ENABLED,
              MRConfig.DEFAULT_MASTER_WEBAPP_UI_ACTIONS_ENABLED);
    }

    @Override
    protected void render(Block html) {
      if (!isValidRequest()) {
        html.
          h2($(TITLE));
        return;
      }

      if (enableUIActions) {
        // Kill task attempt
        String appID = app.getJob().getID().getAppId().toString();
        String jobID = app.getJob().getID().toString();
        String taskID = app.getTask().getID().toString();
        stateURLFormat =
            String.format("/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks/%s/"
                + "attempts", appID, jobID, taskID) + "/%s/state";

        String current =
            String.format("/proxy/%s/mapreduce/task/%s", appID, taskID);

        StringBuilder script = new StringBuilder();
        script.append("function confirmAction(stateURL) {")
            .append(" b = confirm(\"Are you sure?\");")
            .append(" if (b == true) {")
            .append(" $.ajax({")
            .append(" type: 'PUT',")
            .append(" url: stateURL,")
            .append(" contentType: 'application/json',")
            .append(" data: '{\"state\":\"KILLED\"}',")
            .append(" dataType: 'json'")
            .append(" }).done(function(data){")
            .append(" setTimeout(function(){")
            .append(" location.href = '").append(current).append("';")
            .append(" }, 1000);")
            .append(" }).fail(function(data){")
            .append(" console.log(data);")
            .append(" });")
            .append(" }")
            .append("}");

        html.script().$type("text/javascript")._(script.toString())._();
      }

      TR<THEAD<TABLE<Hamlet>>> tr = html.table("#attempts").thead().tr();
      tr.th(".id", "Attempt").
      th(".progress", "Progress").
      th(".state", "State").
      th(".status", "Status").
      th(".node", "Node").
      th(".logs", "Logs").
      th(".tsh", "Started").
      th(".tsh", "Finished").
      th(".tsh", "Elapsed").
      th(".note", "Note");
      if (enableUIActions) {
        tr.th(".actions", "Actions");
      }

      TBODY<TABLE<Hamlet>> tbody = tr._()._().tbody();
      // Write all the data into a JavaScript array of arrays for JQuery
      // DataTables to display
      StringBuilder attemptsTableData = new StringBuilder("[\n");

      for (TaskAttempt attempt : getTaskAttempts()) {
        TaskAttemptInfo ta = new TaskAttemptInfo(attempt, true);
        String progress = StringUtils.formatPercent(ta.getProgress() / 100, 2);

        String nodeHttpAddr = ta.getNode();
        String diag = ta.getNote() == null ? "" : ta.getNote();
        attemptsTableData.append("[\"")
        .append(ta.getId()).append("\",\"")
        .append(progress).append("\",\"")
        .append(ta.getState().toString()).append("\",\"")
        .append(StringEscapeUtils.escapeJavaScript(
              StringEscapeUtils.escapeHtml(ta.getStatus()))).append("\",\"")

        .append(nodeHttpAddr == null ? "N/A" :
          "<a class='nodelink' href='" + MRWebAppUtil.getYARNWebappScheme() + nodeHttpAddr + "'>"
          + nodeHttpAddr + "</a>")
        .append("\",\"")

        .append(ta.getAssignedContainerId() == null ? "N/A" :
          "<a class='logslink' href='" + url(MRWebAppUtil.getYARNWebappScheme(), nodeHttpAddr, "node"
            , "containerlogs", ta.getAssignedContainerIdStr(), app.getJob()
            .getUserName()) + "'>logs</a>")
          .append("\",\"")

        .append(ta.getStartTime()).append("\",\"")
        .append(ta.getFinishTime()).append("\",\"")
        .append(ta.getElapsedTime()).append("\",\"")
        .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
          diag)));
        if (enableUIActions) {
          attemptsTableData.append("\",\"")
          .append("<a href=javascript:void(0) onclick=confirmAction('")
          .append(String.format(stateURLFormat, ta.getId()))
          .append("');>Kill</a>")
          .append("\"],\n");
        } else {
          attemptsTableData.append("\"],\n");
        }
      }
      //Remove the last comma and close off the array of arrays
      if(attemptsTableData.charAt(attemptsTableData.length() - 2) == ',') {
        attemptsTableData.delete(attemptsTableData.length()-2, attemptsTableData.length()-1);
      }
      attemptsTableData.append("]");
      html.script().$type("text/javascript").
      _("var attemptsTableData=" + attemptsTableData)._();

      tbody._()._();

    }

    protected boolean isValidRequest() {
      return app.getTask() != null;
    }

    protected Collection<TaskAttempt> getTaskAttempts() {
      return app.getTask().getAttempts().values();
    }
  }

  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);

    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:3}");
    set(DATATABLES_ID, "attempts");
    set(initID(DATATABLES, "attempts"), attemptsTableInit());
    setTableStyles(html, "attempts");
  }

  @Override protected Class<? extends SubView> content() {
    return AttemptsBlock.class;
  }

  private String attemptsTableInit() {
    return tableInit()
    .append(", 'aaData': attemptsTableData")
    .append(", bDeferRender: true")
    .append(", bProcessing: true")
    .append("\n,aoColumnDefs:[\n")

    //logs column should not filterable (it includes container ID which may pollute searches)
    .append("\n{'aTargets': [ 5 ]")
    .append(", 'bSearchable': false }")

    .append("\n, {'sType':'numeric', 'aTargets': [ 6, 7")
    .append(" ], 'mRender': renderHadoopDate }")

    .append("\n, {'sType':'numeric', 'aTargets': [ 8")
    .append(" ], 'mRender': renderHadoopElapsedTime }]")

    // Sort by id upon page load
    .append("\n, aaSorting: [[0, 'asc']]")
    .append("}").toString();
  }
}
