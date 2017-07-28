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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

import org.apache.hadoop.mapreduce.v2.app.webapp.CountersBlock;
import org.apache.hadoop.yarn.webapp.SubView;

/**
 * Render the counters page
 */
public class HsCountersPage extends HsView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    setActiveNavColumnForTask();
    set(DATATABLES_SELECTOR, "#counters .dt-counters");
    set(initSelector(DATATABLES),
        "{bJQueryUI:true, sDom:'t', iDisplayLength:-1}");
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#postHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  @Override protected void postHead(Page.HTML<__> html) {
    html.
      style("#counters, .dt-counters { table-layout: fixed }",
            "#counters th { overflow: hidden; vertical-align: middle }",
            "#counters .dataTables_wrapper { min-height: 1em }",
            "#counters .group { width: 15em }",
            "#counters .name { width: 30em }");
  }

  /**
   * The content of this page is the CountersBlock now.
   * @return CountersBlock.class
   */
  @Override protected Class<? extends SubView> content() {
    return CountersBlock.class;
  }
}
