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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.COUNTER_GROUP;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.COUNTER_NAME;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class SingleCounterBlock extends HtmlBlock {
  protected TreeMap<String, Long> values = new TreeMap<String, Long>(); 
  protected Job job;
  protected Task task;
  
  @Inject SingleCounterBlock(AppContext appCtx, ViewContext ctx) {
    super(ctx);
    this.populateMembers(appCtx);
  }

  @Override protected void render(Block html) {
    if (job == null) {
      html.
        p()._("Sorry, no counters for nonexistent", $(JOB_ID, "job"))._();
      return;
    }
    if (!$(TASK_ID).isEmpty() && task == null) {
      html.
        p()._("Sorry, no counters for nonexistent", $(TASK_ID, "task"))._();
      return;
    }
    
    String columnType = task == null ? "Task" : "Task Attempt";
    
    TBODY<TABLE<DIV<Hamlet>>> tbody = html.
      div(_INFO_WRAP).
      table("#singleCounter").
        thead().
          tr().
            th(".ui-state-default", columnType).
            th(".ui-state-default", "Value")._()._().
          tbody();
    for (Map.Entry<String, Long> entry : values.entrySet()) {
      TR<TBODY<TABLE<DIV<Hamlet>>>> row = tbody.tr();
      String id = entry.getKey();
      String val = entry.getValue().toString();
      if(task != null) {
        row.td(id);
        row.td().br().$title(val)._()._(val)._();
      } else {
        row.td().a(url("singletaskcounter",entry.getKey(),
            $(COUNTER_GROUP), $(COUNTER_NAME)), id)._();
        row.td().br().$title(val)._().a(url("singletaskcounter",entry.getKey(),
            $(COUNTER_GROUP), $(COUNTER_NAME)), val)._();
      }
      row._();
    }
    tbody._()._()._();
  }

  private void populateMembers(AppContext ctx) {
    JobId jobID = null;
    TaskId taskID = null;
    String tid = $(TASK_ID);
    if (!tid.isEmpty()) {
      taskID = MRApps.toTaskID(tid);
      jobID = taskID.getJobId();
    } else {
      String jid = $(JOB_ID);
      if (!jid.isEmpty()) {
        jobID = MRApps.toJobID(jid);
      }
    }
    if (jobID == null) {
      return;
    }
    job = ctx.getJob(jobID);
    if (job == null) {
      return;
    }
    if (taskID != null) {
      task = job.getTask(taskID);
      if (task == null) {
        return;
      }
      for(Map.Entry<TaskAttemptId, TaskAttempt> entry : 
        task.getAttempts().entrySet()) {
        long value = 0;
        Counters counters = entry.getValue().getCounters();
        CounterGroup group = (counters != null) ? counters
          .getGroup($(COUNTER_GROUP)) : null;
        if(group != null)  {
          Counter c = group.findCounter($(COUNTER_NAME));
          if(c != null) {
            value = c.getValue();
          }
        }
        values.put(MRApps.toString(entry.getKey()), value);
      }
      
      return;
    }
    // Get all types of counters
    Map<TaskId, Task> tasks = job.getTasks();
    for(Map.Entry<TaskId, Task> entry : tasks.entrySet()) {
      long value = 0;
      Counters counters = entry.getValue().getCounters();
      CounterGroup group = (counters != null) ? counters
        .getGroup($(COUNTER_GROUP)) : null;
      if(group != null)  {
        Counter c = group.findCounter($(COUNTER_NAME));
        if(c != null) {
          value = c.getValue();
        }
      }
      values.put(MRApps.toString(entry.getKey()), value);
    }
  }
}
