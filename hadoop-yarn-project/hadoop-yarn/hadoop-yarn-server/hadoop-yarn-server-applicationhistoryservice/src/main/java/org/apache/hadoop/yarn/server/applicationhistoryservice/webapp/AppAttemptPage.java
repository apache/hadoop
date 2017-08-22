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
import org.apache.hadoop.yarn.server.webapp.AppAttemptBlock;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

public class AppAttemptPage extends AHSView {

  @Override
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);

    String appAttemptId = $(YarnWebParams.APPLICATION_ATTEMPT_ID);
    set(
      TITLE,
      appAttemptId.isEmpty() ? "Bad request: missing application attempt ID"
          : join("Application Attempt ",
            $(YarnWebParams.APPLICATION_ATTEMPT_ID)));

    set(DATATABLES_ID, "containers");
    set(initID(DATATABLES, "containers"), WebPageUtils.containersTableInit());
    setTableStyles(html, "containers", ".queue {width:6em}", ".ui {width:8em}");

    set(YarnWebParams.WEB_UI_TYPE, YarnWebParams.APP_HISTORY_WEB_UI);
  }

  @Override
  protected Class<? extends SubView> content() {
    return AppAttemptBlock.class;
  }

  protected String getContainersTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("[\n").append("{'sType':'natural', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }]").toString();
  }

}
