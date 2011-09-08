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

import org.apache.hadoop.mapreduce.v2.app.webapp.JobsBlock;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

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
    setTableStyles(html, "jobs");
  }

  /**
   * The prehead that should be common to all subclasses.
   * @param html used to render.
   */
  protected void commonPreHead(Page.HTML<_> html) {
    set(ACCORDION_ID, "nav");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
    set(THEMESWITCHER_ID, "themeswitcher");
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
    return JobsBlock.class;
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
        append(",aoColumns:[{sType:'title-numeric'},").
        append("null,null,{sType:'title-numeric', bSearchable:false},null,").
        append("null,{sType:'title-numeric',bSearchable:false}, null, null]}").
        toString();
  }
}
