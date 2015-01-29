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

import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * The navigation block for the history server
 */
public class HsNavBlock extends HtmlBlock {
  final App app;

  @Inject HsNavBlock(App app) { this.app = app; }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
   */
  @Override protected void render(Block html) {
    DIV<Hamlet> nav = html.
      div("#nav").
      h3("Application").
        ul().
          li().a(url("about"), "About")._().
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
      if (app.getTask() != null) {
        String taskid = MRApps.toString(app.getTask().getID());
        nav.
          h3("Task").
          ul().
            li().a(url("task", taskid), "Task Overview")._().
            li().a(url("taskcounters", taskid), "Counters")._()._();
      }
    }
    nav.
      h3("Tools").
        ul().
          li().a("/conf", "Configuration")._().
          li().a("/logs", "Local logs")._().
          li().a("/stacks", "Server stacks")._().
          li().a("/jmx?qry=Hadoop:*", "Server metrics")._()._()._();
  }
}
