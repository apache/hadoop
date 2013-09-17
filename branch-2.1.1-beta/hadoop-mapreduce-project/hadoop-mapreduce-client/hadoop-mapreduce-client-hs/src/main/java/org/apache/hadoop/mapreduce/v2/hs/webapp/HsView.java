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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.postInitID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;


/**
 * A view that should be used as the base class for all history server pages.
 */
public class HsView extends TwoColumnLayout {
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    set(DATATABLES_ID, "jobs");
    set(initID(DATATABLES, "jobs"), jobsTableInit());
    set(postInitID(DATATABLES, "jobs"), jobsPostTableInit());
    setTableStyles(html, "jobs");
  }

  /**
   * The prehead that should be common to all subclasses.
   * @param html used to render.
   */
  protected void commonPreHead(Page.HTML<_> html) {
    set(ACCORDION_ID, "nav");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#nav()
   */
  @Override
  protected Class<? extends SubView> nav() {
    return HsNavBlock.class;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#content()
   */
  @Override
  protected Class<? extends SubView> content() {
    return HsJobsBlock.class;
  }
  
  //TODO We need a way to move all of the javascript/CSS that is for a subview
  // into that subview.
  /**
   * @return The end of a javascript map that is the jquery datatable 
   * configuration for the jobs table.  the Jobs table is assumed to be
   * rendered by the class returned from {@link #content()} 
   */
  private String jobsTableInit() {
    return tableInit().
        append(", 'aaData': jobsTableData").
        append(", bDeferRender: true").
        append(", bProcessing: true").

        // Sort by id upon page load
        append(", aaSorting: [[2, 'desc']]").
        append(", aoColumnDefs:[").
        // Maps Total, Maps Completed, Reduces Total and Reduces Completed
        append("{'sType':'numeric', 'bSearchable': false, 'aTargets': [ 7, 8, 9, 10 ] }").
        append("]}").
        toString();
  }
  
  /**
   * @return javascript to add into the jquery block after the table has
   *  been initialized. This code adds in per field filtering.
   */
  private String jobsPostTableInit() {
    return "var asInitVals = new Array();\n" +
    		   "$('tfoot input').keyup( function () \n{"+
           "  jobsDataTable.fnFilter( this.value, $('tfoot input').index(this) );\n"+
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
