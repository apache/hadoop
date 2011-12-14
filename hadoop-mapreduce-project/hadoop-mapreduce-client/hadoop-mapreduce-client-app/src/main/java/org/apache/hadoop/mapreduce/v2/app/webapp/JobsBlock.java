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

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR_VALUE;

import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

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
    for (Job j : appContext.getAllJobs().values()) {
      JobInfo job = new JobInfo(j, false);
      tbody.
        tr().
          td().
            span().$title(String.valueOf(job.getId()))._(). // for sorting
            a(url("job", job.getId()), job.getId())._().
          td(job.getName()).
          td(job.getState()).
          td().
            span().$title(job.getMapProgressPercent())._(). // for sorting
            div(_PROGRESSBAR).
              $title(join(job.getMapProgressPercent(), '%')). // tooltip
              div(_PROGRESSBAR_VALUE).
                $style(join("width:", job.getMapProgressPercent(), '%'))._()._()._().
          td(String.valueOf(job.getMapsTotal())).
          td(String.valueOf(job.getMapsCompleted())).
          td().
            span().$title(job.getReduceProgressPercent())._(). // for sorting
            div(_PROGRESSBAR).
              $title(join(job.getReduceProgressPercent(), '%')). // tooltip
              div(_PROGRESSBAR_VALUE).
                $style(join("width:", job.getReduceProgressPercent(), '%'))._()._()._().
          td(String.valueOf(job.getReducesTotal())).
          td(String.valueOf(job.getReducesCompleted()))._();
    }
    tbody._()._();
  }
}
