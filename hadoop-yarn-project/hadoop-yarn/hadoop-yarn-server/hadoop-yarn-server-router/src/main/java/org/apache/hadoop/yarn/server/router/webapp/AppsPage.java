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

import static org.apache.hadoop.yarn.util.StringHelper.sjoin;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.yarn.webapp.SubView;

class AppsPage extends RouterView {

  @Override
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    set(DATATABLES_ID, "apps");
    set(initID(DATATABLES, "apps"), appsTableInit());
    setTableStyles(html, "apps", ".queue {width:6em}", ".ui {width:8em}");

    // Set the correct title.
    String reqState = $(APP_STATE);
    reqState = (reqState == null || reqState.isEmpty() ? "All" : reqState);
    setTitle(sjoin(reqState, "Applications"));
  }

  private String appsTableInit() {
    // id, user, name, queue, starttime, finishtime, state, status, progress, ui
    return tableInit()
      .append(", 'aaData': appsTableData")
      .append(", bDeferRender: true")
      .append(", bProcessing: true")

      .append("\n, aoColumnDefs: ")
      .append(getAppsTableColumnDefs())

      // Sort by id upon page load
      .append(", aaSorting: [[0, 'desc']]}").toString();
  }

  protected String getAppsTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb
      .append("[\n")
      .append("{'sType':'string', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")

      .append("\n, {'sType':'numeric', 'aTargets': [6, 7]")
      .append(", 'mRender': renderHadoopDate }")

      .append("\n, {'sType':'numeric', bSearchable:false, 'aTargets': [10]")
      .append(", 'mRender': parseHadoopProgress }]").toString();
  }

  @Override
  protected Class<? extends SubView> content() {
    return AppsBlock.class;
  }
}