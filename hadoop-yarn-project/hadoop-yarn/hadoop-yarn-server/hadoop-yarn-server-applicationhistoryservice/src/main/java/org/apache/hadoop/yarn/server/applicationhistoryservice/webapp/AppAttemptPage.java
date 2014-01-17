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
package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.yarn.server.webapp.AppAttemptBlock;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

public class AppAttemptPage extends AHSView {

  @Override
  protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);

    String appAttemptId = $(YarnWebParams.APPLICATION_ATTEMPT_ID);
    set(
      TITLE,
      appAttemptId.isEmpty() ? "Bad request: missing application attempt ID"
          : join("Application Attempt ",
            $(YarnWebParams.APPLICATION_ATTEMPT_ID)));

    set(DATATABLES_ID, "containers");
    set(initID(DATATABLES, "containers"), containersTableInit());
    setTableStyles(html, "containers", ".queue {width:6em}", ".ui {width:8em}");
  }

  @Override
  protected Class<? extends SubView> content() {
    return AppAttemptBlock.class;
  }

  private String containersTableInit() {
    return tableInit().append(", 'aaData': containersTableData")
      .append(", bDeferRender: true").append(", bProcessing: true")

      .append("\n, aoColumnDefs: ").append(getContainersTableColumnDefs())

      // Sort by id upon page load
      .append(", aaSorting: [[0, 'desc']]}").toString();
  }

  protected String getContainersTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("[\n").append("{'sType':'numeric', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }]").toString();
  }

}