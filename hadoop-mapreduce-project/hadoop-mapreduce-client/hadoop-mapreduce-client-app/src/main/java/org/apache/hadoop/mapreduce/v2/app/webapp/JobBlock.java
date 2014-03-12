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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._EVEN;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._ODD;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR_VALUE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AMAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class JobBlock extends HtmlBlock {
  final AppContext appContext;

  @Inject JobBlock(AppContext appctx) {
    appContext = appctx;
  }

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

    List<AMInfo> amInfos = job.getAMInfos();
    String amString =
        amInfos.size() == 1 ? "ApplicationMaster" : "ApplicationMasters"; 

    JobInfo jinfo = new JobInfo(job, true);
    info("Job Overview").
        _("Job Name:", jinfo.getName()).
        _("State:", jinfo.getState()).
        _("Uberized:", jinfo.isUberized()).
        _("Started:", new Date(jinfo.getStartTime())).
        _("Elapsed:", StringUtils.formatTime(jinfo.getElapsedTime()));
    DIV<Hamlet> div = html.
      _(InfoBlock.class).
      div(_INFO_WRAP);

    // MRAppMasters Table
    TABLE<DIV<Hamlet>> table = div.table("#job");
    table.
      tr().
      th(amString).
      _().
      tr().
      th(_TH, "Attempt Number").
      th(_TH, "Start Time").
      th(_TH, "Node").
      th(_TH, "Logs").
      _();
    for (AMInfo amInfo : amInfos) {
      AMAttemptInfo attempt = new AMAttemptInfo(amInfo,
          jinfo.getId(), jinfo.getUserName());

      table.tr().
        td(String.valueOf(attempt.getAttemptId())).
        td(new Date(attempt.getStartTime()).toString()).
        td().a(".nodelink", url(MRWebAppUtil.getYARNWebappScheme(),
            attempt.getNodeHttpAddress()),
            attempt.getNodeHttpAddress())._().
        td().a(".logslink", url(attempt.getLogsLink()), 
            "logs")._().
        _();
    }

    table._();
    div._();

    html.div(_INFO_WRAP).        
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
            th("Map").
            td().
              div(_PROGRESSBAR).
                $title(join(jinfo.getMapProgressPercent(), '%')). // tooltip
                div(_PROGRESSBAR_VALUE).
                  $style(join("width:", jinfo.getMapProgressPercent(), '%'))._()._()._().
            td().a(url("tasks", jid, "m", "ALL"),String.valueOf(jinfo.getMapsTotal()))._().
            td().a(url("tasks", jid, "m", "PENDING"),String.valueOf(jinfo.getMapsPending()))._().
            td().a(url("tasks", jid, "m", "RUNNING"),String.valueOf(jinfo.getMapsRunning()))._().
            td().a(url("tasks", jid, "m", "COMPLETED"),String.valueOf(jinfo.getMapsCompleted()))._()._().
          tr(_EVEN).
            th("Reduce").
            td().
              div(_PROGRESSBAR).
                $title(join(jinfo.getReduceProgressPercent(), '%')). // tooltip
                div(_PROGRESSBAR_VALUE).
                  $style(join("width:", jinfo.getReduceProgressPercent(), '%'))._()._()._().
            td().a(url("tasks", jid, "r", "ALL"),String.valueOf(jinfo.getReducesTotal()))._().
            td().a(url("tasks", jid, "r", "PENDING"),String.valueOf(jinfo.getReducesPending()))._().
            td().a(url("tasks", jid, "r", "RUNNING"),String.valueOf(jinfo.getReducesRunning()))._().
            td().a(url("tasks", jid, "r", "COMPLETED"),String.valueOf(jinfo.getReducesCompleted()))._()._()
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
              String.valueOf(jinfo.getNewMapAttempts()))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.RUNNING.toString()),
              String.valueOf(jinfo.getRunningMapAttempts()))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.FAILED.toString()),
              String.valueOf(jinfo.getFailedMapAttempts()))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.KILLED.toString()),
              String.valueOf(jinfo.getKilledMapAttempts()))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.SUCCESSFUL.toString()),
              String.valueOf(jinfo.getSuccessfulMapAttempts()))._().
        _().
        tr(_EVEN).
          th("Reduces").
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.NEW.toString()),
              String.valueOf(jinfo.getNewReduceAttempts()))._().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.RUNNING.toString()),
              String.valueOf(jinfo.getRunningReduceAttempts()))._().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.FAILED.toString()),
              String.valueOf(jinfo.getFailedReduceAttempts()))._().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.KILLED.toString()),
              String.valueOf(jinfo.getKilledReduceAttempts()))._().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.SUCCESSFUL.toString()),
              String.valueOf(jinfo.getSuccessfulReduceAttempts()))._().
         _().
       _().
     _();
  }

}
