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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMWebApp.*;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

/**
 * Render a block of HTML for a give job.
 */
public class HsJobBlock extends HtmlBlock {
  final AppContext appContext;

  int killedMapAttempts = 0;
  int failedMapAttempts = 0;
  int successfulMapAttempts = 0;
  int killedReduceAttempts = 0;
  int failedReduceAttempts = 0;
  int successfulReduceAttempts = 0;
  long avgMapTime = 0;
  long avgReduceTime = 0;
  long avgShuffleTime = 0;
  long avgSortTime = 0;
  int numMaps;
  int numReduces;

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
    Map<JobACL, AccessControlList> acls = job.getJobACLs();
    List<AMInfo> amInfos = job.getAMInfos();
    JobReport jobReport = job.getReport();
    int mapTasks = job.getTotalMaps();
    int mapTasksComplete = job.getCompletedMaps();
    int reduceTasks = job.getTotalReduces();
    int reducesTasksComplete = job.getCompletedReduces();
    long startTime = jobReport.getStartTime();
    long finishTime = jobReport.getFinishTime();
    countTasksAndAttempts(job);
    ResponseInfo infoBlock = info("Job Overview").
        _("Job Name:", job.getName()).
        _("User Name:", job.getUserName()).
        _("Queue:", job.getQueueName()).
        _("State:", job.getState()).
        _("Uberized:", job.isUber()).
        _("Started:", new Date(startTime)).
        _("Finished:", new Date(finishTime)).
        _("Elapsed:", StringUtils.formatTime(
            Times.elapsed(startTime, finishTime, false)));
    
    String amString =
        amInfos.size() == 1 ? "ApplicationMaster" : "ApplicationMasters"; 
    
    List<String> diagnostics = job.getDiagnostics();
    if(diagnostics != null && !diagnostics.isEmpty()) {
      StringBuffer b = new StringBuffer();
      for(String diag: diagnostics) {
        b.append(diag);
      }
      infoBlock._("Diagnostics:", b.toString());
    }

    if(numMaps > 0) {
      infoBlock._("Average Map Time", StringUtils.formatTime(avgMapTime));
    }
    if(numReduces > 0) {
      infoBlock._("Average Reduce Time", StringUtils.formatTime(avgReduceTime));
      infoBlock._("Average Shuffle Time", StringUtils.formatTime(avgShuffleTime));
      infoBlock._("Average Merge Time", StringUtils.formatTime(avgSortTime));
    }

    for(Map.Entry<JobACL, AccessControlList> entry : acls.entrySet()) {
      infoBlock._("ACL "+entry.getKey().getAclName()+":",
          entry.getValue().getAclString());
    }
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
            String nodeHttpAddress = amInfo.getNodeManagerHost() + 
                ":" + amInfo.getNodeManagerHttpPort();
            NodeId nodeId = BuilderUtils.newNodeId(
                amInfo.getNodeManagerHost(), amInfo.getNodeManagerPort());
            
            table.tr().
              td(String.valueOf(amInfo.getAppAttemptId().getAttemptId())).
              td(new Date(amInfo.getStartTime()).toString()).
              td().a(".nodelink", url("http://", nodeHttpAddress), 
                  nodeHttpAddress)._().
              td().a(".logslink", url("logs", nodeId.toString(), 
                  amInfo.getContainerId().toString(), jid, job.getUserName()), 
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
            th(_TH, "Total").
            th(_TH, "Complete")._().
          tr(_ODD).
            th().
              a(url("tasks", jid, "m"), "Map")._().
            td(String.valueOf(mapTasks)).
            td(String.valueOf(mapTasksComplete))._().
          tr(_EVEN).
            th().
              a(url("tasks", jid, "r"), "Reduce")._().
            td(String.valueOf(reduceTasks)).
            td(String.valueOf(reducesTasksComplete))._()
          ._().

        // Attempts table
        table("#job").
        tr().
          th(_TH, "Attempt Type").
          th(_TH, "Failed").
          th(_TH, "Killed").
          th(_TH, "Successful")._().
        tr(_ODD).
          th("Maps").
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
    numReduces = 0;
    numMaps = 0;
    Map<TaskId, Task> tasks = job.getTasks();
    for (Task task : tasks.values()) {
      // Attempts counts
      Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
      for (TaskAttempt attempt : attempts.values()) {

        int successful = 0, failed = 0, killed =0;

        if (TaskAttemptStateUI.NEW.correspondsTo(attempt.getState())) {
          //Do Nothing
        } else if (TaskAttemptStateUI.RUNNING.correspondsTo(attempt
            .getState())) {
          //Do Nothing
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
          successfulMapAttempts += successful;
          failedMapAttempts += failed;
          killedMapAttempts += killed;
          if(attempt.getState() == TaskAttemptState.SUCCEEDED) {
            numMaps++;
            avgMapTime += (attempt.getFinishTime() -
                attempt.getLaunchTime());
          }
          break;
        case REDUCE:
          successfulReduceAttempts += successful;
          failedReduceAttempts += failed;
          killedReduceAttempts += killed;
          if(attempt.getState() == TaskAttemptState.SUCCEEDED) {
            numReduces++;
            avgShuffleTime += (attempt.getShuffleFinishTime() - 
                attempt.getLaunchTime());
            avgSortTime += attempt.getSortFinishTime() - 
                attempt.getLaunchTime();
            avgReduceTime += (attempt.getFinishTime() -
                attempt.getShuffleFinishTime());
          }
          break;
        }
      }
    }

    if(numMaps > 0) {
      avgMapTime = avgMapTime / numMaps;
    }
    
    if(numReduces > 0) {
      avgReduceTime = avgReduceTime / numReduces;
      avgShuffleTime = avgShuffleTime / numReduces;
      avgSortTime = avgSortTime / numReduces;
    }
  }
}
