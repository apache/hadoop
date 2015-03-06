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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import static org.apache.hadoop.yarn.util.StringHelper.sjoin;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

// Do NOT rename/refactor this to RMView as it will wreak havoc
// on Mac OS HFS
public class RmView extends TwoColumnLayout {
  static final int MAX_DISPLAY_ROWS = 100;  // direct table rendering
  static final int MAX_FAST_ROWS = 1000;    // inline js array

  @Override
  protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    set(DATATABLES_ID, "apps");
    set(initID(DATATABLES, "apps"), initAppsTable());
    setTableStyles(html, "apps", ".queue {width:6em}", ".ui {width:8em}");

    // Set the correct title.
    String reqState = $(APP_STATE);
    reqState = (reqState == null || reqState.isEmpty() ? "All" : reqState);
    setTitle(sjoin(reqState, "Applications"));
  }

  protected void commonPreHead(Page.HTML<_> html) {
    set(ACCORDION_ID, "nav");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  @Override
  protected Class<? extends SubView> nav() {
    return NavBlock.class;
  }

  @Override
  protected Class<? extends SubView> content() {
    return AppsBlockWithMetrics.class;
  }

  protected String initAppsTable() {
    return WebPageUtils.appsTableInit();
  }
}
