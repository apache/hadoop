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

import org.apache.hadoop.yarn.webapp.SubView;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

public class TasksPage extends AppView {

  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    set(DATATABLES_ID, "tasks");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:2}");
    set(initID(DATATABLES, "tasks"), tasksTableInit());
    setTableStyles(html, "tasks");
  }

  @Override protected Class<? extends SubView> content() {
    return TasksBlock.class;
  }

  private String tasksTableInit() {
    return tableInit()
      .append(", 'aaData': tasksTableData")
      .append(", bDeferRender: true")
      .append(", bProcessing: true")

      .append("\n, aoColumnDefs: [\n")
      .append("{'sType':'numeric', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")

      .append("\n, {'sType':'numeric', bSearchable:false, 'aTargets': [1]")
      .append(", 'mRender': parseHadoopProgress }")


      .append("\n, {'sType':'numeric', 'aTargets': [4, 5]")
      .append(", 'mRender': renderHadoopDate }")

      .append("\n, {'sType':'numeric', 'aTargets': [6]")
      .append(", 'mRender': renderHadoopElapsedTime }]")

      // Sort by id upon page load
      .append(", aaSorting: [[0, 'asc']] }").toString();
  }
}
