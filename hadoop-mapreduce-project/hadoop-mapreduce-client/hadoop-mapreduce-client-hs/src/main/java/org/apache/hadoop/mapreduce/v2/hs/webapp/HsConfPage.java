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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.postInitID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.mapreduce.v2.app.webapp.ConfBlock;
import org.apache.hadoop.yarn.webapp.SubView;

/**
 * Render a page with the configuration for a give job in it.
 */
public class HsConfPage extends HsView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  @Override protected void preHead(Page.HTML<_> html) {
    String jobID = $(JOB_ID);
    set(TITLE, jobID.isEmpty() ? "Bad request: missing job ID"
        : join("Configuration for MapReduce Job ", $(JOB_ID)));
    commonPreHead(html);
    set(DATATABLES_ID, "conf");
    set(initID(DATATABLES, "conf"), confTableInit());
    set(postInitID(DATATABLES, "conf"), confPostTableInit());
    setTableStyles(html, "conf");

    //Override the default nav config
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:1}");
  }

  /**
   * The body of this block is the configuration block.
   * @return HsConfBlock.class
   */
  @Override protected Class<? extends SubView> content() {
    return ConfBlock.class;
  }

  /**
   * @return the end of the JS map that is the jquery datatable config for the
   * conf table.
   */
  private String confTableInit() {
    return tableInit().append("}").toString();
  }

  /**
   * @return the java script code to allow the jquery conf datatable to filter
   * by column.
   */
  private String confPostTableInit() {
    return "var confInitVals = new Array();\n" +
    "$('tfoot input').keyup( function () \n{"+
    "  confDataTable.fnFilter( this.value, $('tfoot input').index(this) );\n"+
    "} );\n"+
    "$('tfoot input').each( function (i) {\n"+
    "  confInitVals[i] = this.value;\n"+
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
    "    this.value = confInitVals[$('tfoot input').index(this)];\n"+
    "  }\n"+
    "} );\n";
  }
}
