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

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

// Do NOT rename/refactor this to RMView as it will wreak havoc
// on Mac OS HFS
public class RmView extends TwoColumnLayout {
  static final int MAX_DISPLAY_ROWS = 100;  // direct table rendering
  static final int MAX_FAST_ROWS = 1000;    // inline js array
  static final int MAX_INLINE_ROWS = 2000;  // ajax load

  @Override
  protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    set(DATATABLES_ID, "apps");
    set(initID(DATATABLES, "apps"), appsTableInit());
    setTableStyles(html, "apps", ".queue {width:6em}", ".ui {width:8em}");
  }

  protected void commonPreHead(Page.HTML<_> html) {
    //html.meta_http("refresh", "20");
    set(ACCORDION_ID, "nav");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
    set(THEMESWITCHER_ID, "themeswitcher");
  }

  @Override
  protected Class<? extends SubView> nav() {
    return NavBlock.class;
  }

  @Override
  protected Class<? extends SubView> content() {
    return AppsBlockWithMetrics.class;
  }

  private String appsTableInit() {
    AppsList list = getInstance(AppsList.class);
    // id, user, name, queue, state, progress, ui, note
    StringBuilder init = tableInit().
        append(", aoColumns:[{sType:'title-numeric'}, null, null, null, null,").
        append("null,{sType:'title-numeric', bSearchable:false}, null, null]");
    String rows = $("rowlimit");
    int rowLimit = rows.isEmpty() ? MAX_DISPLAY_ROWS : Integer.parseInt(rows);
    if (list.apps.size() < rowLimit) {
      list.rendering = Render.HTML;
      return init.append('}').toString();
    }
    if (list.apps.size() > MAX_FAST_ROWS) {
      tableInitProgress(init, list.apps.size() * 6);
    }
    if (list.apps.size() > MAX_INLINE_ROWS) {
      list.rendering = Render.JS_LOAD;
      return init.append(", sAjaxSource:'").append(url("apps", "json")).
          append("'}").toString();
    }
    list.rendering = Render.JS_ARRAY;
    return init.append(", aaData:appsData}").toString();
  }
}
