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

import com.google.inject.Inject;

import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.*;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import static org.apache.hadoop.yarn.util.StringHelper.*;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

public class JobsBlock extends HtmlBlock {
  final AppContext appContext;

  @Inject JobsBlock(AppContext appCtx) {
    appContext = appCtx;
  }

  @Override protected void render(Block html) {
    TBODY<TABLE<Hamlet>> tbody = html.
      h2("Active Jobs").
      table("#jobs").
        thead().
          tr().
            th(".id", "Job ID").
            th(".name", "Name").
            th(".state", "State").
            th("Map Progress").
            th("Maps Total").
            th("Maps Completed").
            th("Reduce Progress").
            th("Reduces Total").
            th("Reduces Completed")._()._().
        tbody();
    for (Job job : appContext.getAllJobs().values()) {
      String jobID = MRApps.toString(job.getID());
      JobReport report = job.getReport();
      String mapPct = percent(report.getMapProgress());
      String mapsTotal = String.valueOf(job.getTotalMaps());
      String mapsCompleted = String.valueOf(job.getCompletedMaps());
      String reducePct = percent(report.getReduceProgress());
      String reduceTotal = String.valueOf(job.getTotalReduces());
      String reduceCompleted = String.valueOf(job.getCompletedReduces());
      tbody.
        tr().
          td().
            span().$title(String.valueOf(job.getID().getId()))._(). // for sorting
            a(url("job", jobID), jobID)._().
          td(job.getName().toString()).
          td(job.getState().toString()).
          td().
            span().$title(mapPct)._(). // for sorting
            div(_PROGRESSBAR).
              $title(join(mapPct, '%')). // tooltip
              div(_PROGRESSBAR_VALUE).
                $style(join("width:", mapPct, '%'))._()._()._().
          td(mapsTotal).
          td(mapsCompleted).
          td().
            span().$title(reducePct)._(). // for sorting
            div(_PROGRESSBAR).
              $title(join(reducePct, '%')). // tooltip
              div(_PROGRESSBAR_VALUE).
                $style(join("width:", reducePct, '%'))._()._()._().
          td(reduceTotal).
          td(reduceCompleted)._();
    }
    tbody._()._();
  }
}
