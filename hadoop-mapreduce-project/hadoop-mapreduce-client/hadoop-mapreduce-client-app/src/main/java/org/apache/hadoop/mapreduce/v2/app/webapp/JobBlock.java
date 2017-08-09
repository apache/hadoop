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
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
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
        p().__("Sorry, can't do anything without a JobID.").__();
      return;
    }
    JobId jobID = MRApps.toJobID(jid);
    Job job = appContext.getJob(jobID);
    if (job == null) {
      html.
        p().__("Sorry, ", jid, " not found.").__();
      return;
    }

    List<AMInfo> amInfos = job.getAMInfos();
    String amString =
        amInfos.size() == 1 ? "ApplicationMaster" : "ApplicationMasters"; 

    JobInfo jinfo = new JobInfo(job, true);
    info("Job Overview").
        __("Job Name:", jinfo.getName()).
        __("User Name:", jinfo.getUserName()).
        __("Queue Name:", jinfo.getQueueName()).
        __("State:", jinfo.getState()).
        __("Uberized:", jinfo.isUberized()).
        __("Started:", new Date(jinfo.getStartTime())).
        __("Elapsed:", StringUtils.formatTime(jinfo.getElapsedTime()));
    DIV<Hamlet> div = html.
        __(InfoBlock.class).
      div(_INFO_WRAP);

    // MRAppMasters Table
    TABLE<DIV<Hamlet>> table = div.table("#job");
    table.
      tr().
      th(amString).
        __().
      tr().
      th(_TH, "Attempt Number").
      th(_TH, "Start Time").
      th(_TH, "Node").
      th(_TH, "Logs").
        __();
    for (AMInfo amInfo : amInfos) {
      AMAttemptInfo attempt = new AMAttemptInfo(amInfo,
          jinfo.getId(), jinfo.getUserName());

      table.tr().
        td(String.valueOf(attempt.getAttemptId())).
        td(new Date(attempt.getStartTime()).toString()).
        td().a(".nodelink", url(MRWebAppUtil.getYARNWebappScheme(),
            attempt.getNodeHttpAddress()),
            attempt.getNodeHttpAddress()).__().
        td().a(".logslink", url(attempt.getLogsLink()), 
            "logs").__().
          __();
    }

    table.__();
    div.__();

    html.div(_INFO_WRAP).        
      // Tasks table
        table("#job").
          tr().
            th(_TH, "Task Type").
            th(_TH, "Progress").
            th(_TH, "Total").
            th(_TH, "Pending").
            th(_TH, "Running").
            th(_TH, "Complete").__().
          tr(_ODD).
            th("Map").
            td().
              div(_PROGRESSBAR).
                $title(join(jinfo.getMapProgressPercent(), '%')). // tooltip
                div(_PROGRESSBAR_VALUE).
                  $style(join("width:", jinfo.getMapProgressPercent(), '%')).__().__().__().
            td().a(url("tasks", jid, "m", "ALL"), String.valueOf(jinfo.getMapsTotal())).__().
            td().a(url("tasks", jid, "m", "PENDING"), String.valueOf(jinfo.getMapsPending())).__().
            td().a(url("tasks", jid, "m", "RUNNING"), String.valueOf(jinfo.getMapsRunning())).__().
            td().a(url("tasks", jid, "m", "COMPLETED"), String.valueOf(jinfo.getMapsCompleted())).__().__().
          tr(_EVEN).
            th("Reduce").
            td().
              div(_PROGRESSBAR).
                $title(join(jinfo.getReduceProgressPercent(), '%')). // tooltip
                div(_PROGRESSBAR_VALUE).
                  $style(join("width:", jinfo.getReduceProgressPercent(), '%')).__().__().__().
            td().a(url("tasks", jid, "r", "ALL"), String.valueOf(jinfo.getReducesTotal())).__().
            td().a(url("tasks", jid, "r", "PENDING"), String.valueOf(jinfo.getReducesPending())).__().
            td().a(url("tasks", jid, "r", "RUNNING"), String.valueOf(jinfo.getReducesRunning())).__().
            td().a(url("tasks", jid, "r", "COMPLETED"), String.valueOf(jinfo.getReducesCompleted())).__().__()
          .__().
        // Attempts table
        table("#job").
        tr().
          th(_TH, "Attempt Type").
          th(_TH, "New").
          th(_TH, "Running").
          th(_TH, "Failed").
          th(_TH, "Killed").
          th(_TH, "Successful").__().
        tr(_ODD).
          th("Maps").
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.NEW.toString()),
              String.valueOf(jinfo.getNewMapAttempts())).__().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.RUNNING.toString()),
              String.valueOf(jinfo.getRunningMapAttempts())).__().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.FAILED.toString()),
              String.valueOf(jinfo.getFailedMapAttempts())).__().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.KILLED.toString()),
              String.valueOf(jinfo.getKilledMapAttempts())).__().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.SUCCESSFUL.toString()),
              String.valueOf(jinfo.getSuccessfulMapAttempts())).__().
        __().
        tr(_EVEN).
          th("Reduces").
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.NEW.toString()),
              String.valueOf(jinfo.getNewReduceAttempts())).__().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.RUNNING.toString()),
              String.valueOf(jinfo.getRunningReduceAttempts())).__().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.FAILED.toString()),
              String.valueOf(jinfo.getFailedReduceAttempts())).__().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.KILLED.toString()),
              String.valueOf(jinfo.getKilledReduceAttempts())).__().
          td().a(url("attempts", jid, "r",
              TaskAttemptStateUI.SUCCESSFUL.toString()),
              String.valueOf(jinfo.getSuccessfulReduceAttempts())).__().
        __().
        __().
        __();
  }

}
