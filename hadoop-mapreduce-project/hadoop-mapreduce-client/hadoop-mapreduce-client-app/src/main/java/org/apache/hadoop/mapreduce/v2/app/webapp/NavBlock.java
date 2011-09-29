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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMWebApp.*;

import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.*;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

public class NavBlock extends HtmlBlock {
  final App app;

  @Inject NavBlock(App app) { this.app = app; }

  @Override protected void render(Block html) {
    String rmweb = $(RM_WEB);
    DIV<Hamlet> nav = html.
      div("#nav").
        h3("Cluster").
        ul().
          li().a(url(rmweb, prefix(), "cluster"), "About")._().
          li().a(url(rmweb, prefix(), "apps"), "Applications")._().
          li().a(url(rmweb, prefix(), "scheduler"), "Scheduler")._()._().
        h3("Application").
        ul().
          li().a(url("app/info"), "About")._().
          li().a(url("app"), "Jobs")._()._();
    if (app.getJob() != null) {
      String jobid = MRApps.toString(app.getJob().getID());
      nav.
        h3("Job").
        ul().
          li().a(url("job", jobid), "Overview")._().
          li().a(url("jobcounters", jobid), "Counters")._().
          li().a(url("conf", jobid), "Configuration")._().
          li().a(url("tasks", jobid, "m"), "Map tasks")._().
          li().a(url("tasks", jobid, "r"), "Reduce tasks")._()._();
    }
    nav.
      h3("Tools").
      ul().
        li().a("/conf", "Configuration")._().
        li().a("/logs", "Local logs")._().
        li().a("/stacks", "Server stacks")._().
        li().a("/metrics", "Server metrics")._()._()._().
    div("#themeswitcher")._();
  }
}
