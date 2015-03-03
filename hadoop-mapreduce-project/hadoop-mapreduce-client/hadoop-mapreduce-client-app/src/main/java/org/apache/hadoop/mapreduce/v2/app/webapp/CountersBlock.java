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
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_TABLE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;

import java.util.Map;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TD;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class CountersBlock extends HtmlBlock {
  Job job;
  Task task;
  Counters total;
  Counters map;
  Counters reduce;

  @Inject CountersBlock(AppContext appCtx, ViewContext ctx) {
    super(ctx);
    getCounters(appCtx);
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
    
    if(total == null || total.getGroupNames() == null || total.countCounters() == 0) {
      String type = $(TASK_ID);
      if(type == null || type.isEmpty()) {
        type = $(JOB_ID, "the job");
      }
      html.
        p()._("Sorry it looks like ",type," has no counters.")._();
      return;
    }
    
    String urlBase;
    String urlId;
    if(task != null) {
      urlBase = "singletaskcounter";
      urlId = MRApps.toString(task.getID());
    } else {
      urlBase = "singlejobcounter";
      urlId = MRApps.toString(job.getID());
    }
    
    
    int numGroups = 0;
    TBODY<TABLE<DIV<Hamlet>>> tbody = html.
      div(_INFO_WRAP).
      table("#counters").
        thead().
          tr().
            th(".group.ui-state-default", "Counter Group").
            th(".ui-state-default", "Counters")._()._().
        tbody();
    for (CounterGroup g : total) {
      CounterGroup mg = map == null ? null : map.getGroup(g.getName());
      CounterGroup rg = reduce == null ? null : reduce.getGroup(g.getName());
      ++numGroups;
      // This is mostly for demonstration :) Typically we'd introduced
      // a CounterGroup block to reduce the verbosity. OTOH, this
      // serves as an indicator of where we're in the tag hierarchy.
      TR<THEAD<TABLE<TD<TR<TBODY<TABLE<DIV<Hamlet>>>>>>>> groupHeadRow = tbody.
        tr().
          th().$title(g.getName()).$class("ui-state-default").
            _(fixGroupDisplayName(g.getDisplayName()))._().
          td().$class(C_TABLE).
            table(".dt-counters").$id(job.getID()+"."+g.getName()).
              thead().
                tr().th(".name", "Name");

      if (map != null) {
        groupHeadRow.th("Map").th("Reduce");
      }
      // Ditto
      TBODY<TABLE<TD<TR<TBODY<TABLE<DIV<Hamlet>>>>>>> group = groupHeadRow.
            th(map == null ? "Value" : "Total")._()._().
        tbody();
      for (Counter counter : g) {
        // Ditto
        TR<TBODY<TABLE<TD<TR<TBODY<TABLE<DIV<Hamlet>>>>>>>> groupRow = group.
          tr();
          if (task == null && mg == null && rg == null) {
            groupRow.td().$title(counter.getName())._(counter.getDisplayName()).
            _();
          } else {
            groupRow.td().$title(counter.getName()).
              a(url(urlBase,urlId,g.getName(), 
                  counter.getName()), counter.getDisplayName()).
            _();
          }
        if (map != null) {
          Counter mc = mg == null ? null : mg.findCounter(counter.getName());
          Counter rc = rg == null ? null : rg.findCounter(counter.getName());
          groupRow.
            td(mc == null ? "0" : String.format("%,d", mc.getValue())).
            td(rc == null ? "0" : String.format("%,d", rc.getValue()));
        }
        groupRow.td(String.format("%,d", counter.getValue()))._();
      }
      group._()._()._()._();
    }
    tbody._()._()._();
  }

  private void getCounters(AppContext ctx) {
    JobId jobID = null;
    TaskId taskID = null;
    String tid = $(TASK_ID);
    if (!tid.isEmpty()) {
      taskID = MRApps.toTaskID(tid);
      jobID = taskID.getJobId();
    } else {
      String jid = $(JOB_ID);
      if (jid != null && !jid.isEmpty()) {
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
      total = task.getCounters();
      return;
    }
    // Get all types of counters
    Map<TaskId, Task> tasks = job.getTasks();
    total = job.getAllCounters();
    boolean needTotalCounters = false;
    if (total == null) {
      total = new Counters();
      needTotalCounters = true;
    }
    map = new Counters();
    reduce = new Counters();
    for (Task t : tasks.values()) {
      Counters counters = t.getCounters();
      if (counters == null) {
        continue;
      }
      switch (t.getType()) {
        case MAP:     map.incrAllCounters(counters);     break;
        case REDUCE:  reduce.incrAllCounters(counters);  break;
      }
      if (needTotalCounters) {
        total.incrAllCounters(counters);
      }
    }
  }

  private String fixGroupDisplayName(CharSequence name) {
    return name.toString().replace(".", ".\u200B").replace("$", "\u200B$");
  }
}