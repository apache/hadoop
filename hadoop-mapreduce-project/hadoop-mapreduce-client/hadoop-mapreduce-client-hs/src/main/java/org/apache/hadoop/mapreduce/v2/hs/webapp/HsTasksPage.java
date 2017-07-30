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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_SELECTOR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initSelector;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.postInitID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.SubView;

/**
 * A page showing the tasks for a given application.
 */
public class HsTasksPage extends HsView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    set(DATATABLES_ID, "tasks");
    set(DATATABLES_SELECTOR, ".dt-tasks" );
    set(initSelector(DATATABLES), tasksTableInit());
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:1}");
    set(initID(DATATABLES, "tasks"), tasksTableInit());
    set(postInitID(DATATABLES, "tasks"), jobsPostTableInit());
    setTableStyles(html, "tasks");
  }
  
  /**
   * The content of this page is the TasksBlock
   * @return HsTasksBlock.class
   */
  @Override protected Class<? extends SubView> content() {
    return HsTasksBlock.class;
  }

  /**
   * @return the end of the JS map that is the jquery datatable configuration
   * for the tasks table.
   */
  private String tasksTableInit() {
    TaskType type = null;
    String symbol = $(TASK_TYPE);
    if (!symbol.isEmpty()) {
      type = MRApps.taskType(symbol);
    }
    StringBuilder b = tableInit().
    append(", 'aaData': tasksTableData")
    .append(", bDeferRender: true")
    .append(", bProcessing: true")

    .append("\n, aoColumnDefs: [\n")
    .append("{'sType':'natural', 'aTargets': [ 0 ]")
    .append(", 'mRender': parseHadoopID }")

    .append(", {'sType':'numeric', 'aTargets': [ 4")
    .append(type == TaskType.REDUCE ? ", 9, 10, 11, 12" : ", 7")
    .append(" ], 'mRender': renderHadoopElapsedTime }")

    .append("\n, {'sType':'numeric', 'aTargets': [ 2, 3, 5")
    .append(type == TaskType.REDUCE ? ", 6, 7, 8" : ", 6")
    .append(" ], 'mRender': renderHadoopDate }]")

    // Sort by id upon page load
    .append("\n, aaSorting: [[0, 'asc']]")
    .append("}");
    return b.toString();
  }
  
  private String jobsPostTableInit() {
    return "var asInitVals = new Array();\n" +
           "$('tfoot input').keyup( function () \n{"+
           "  $('.dt-tasks').dataTable().fnFilter("+
           " this.value, $('tfoot input').index(this) );\n"+
           "} );\n"+
           "$('tfoot input').each( function (i) {\n"+
           "  asInitVals[i] = this.value;\n"+
           "} );\n"+
           "$('tfoot input').focus( function () {\n"+
           "  if ( this.className == 'search_init' )\n"+
           "  {\n"+
           "    this.className = '';\n"+
           "    this.value = '';\n"+
           "  }\n"+
           "} );\n"+
           "$('tfoot input').blur( function (i) {\n"+
           "  if ( this.value == '' )\n"+
           "  {\n"+
           "    this.className = 'search_init';\n"+
           "    this.value = asInitVals[$('tfoot input').index(this)];\n"+
           "  }\n"+
           "} );\n";
  }
}
