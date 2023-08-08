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

package org.apache.hadoop.yarn.server.router.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.yarn.webapp.SubView;

/**
 * Renders a block for the applications with metrics information.
 */
class FederationPage extends RouterView {

  @Override
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    setTitle("About The YARN Federation");
    set(DATATABLES_ID, "rms");
    set("ui.div.id", "div_id");
    set(initID(DATATABLES, "rms"), rmsTableInit());
    setTableStyles(html, "rms", ".healthStatus {width:10em}",
        ".healthReport {width:10em}");
  }

  @Override
  protected Class<? extends SubView> content() {
    return FederationBlock.class;
  }

  private String rmsTableInit() {
    StringBuilder builder = tableInit().append(", aoColumnDefs: [");
    builder
        .append("{'sName':'State', 'sType':'string', 'bSearchable':false, 'aTargets':[1]},")
        .append("{'sName':'LastStartTime', 'sType':'string', 'bSearchable':false, 'aTargets':[2]},")
        .append("{'sName':'lastHeartBeat', 'sType':'string', 'bSearchable':false, 'aTargets':[3]},")
        .append("{'sName':'resource', 'sType':'string', 'bSearchable':false, 'aTargets':[4]},")
        .append("{'sName':'nodes', 'sType':'string', 'bSearchable':false, 'aTargets':[5]}")
        .append("]}");
    return builder.toString();
  }
}
