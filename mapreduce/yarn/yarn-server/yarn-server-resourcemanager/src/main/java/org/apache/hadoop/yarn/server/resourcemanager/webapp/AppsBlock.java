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

import com.google.inject.Inject;

import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.AppContext;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.*;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import static org.apache.hadoop.yarn.util.StringHelper.*;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

class AppsBlock extends HtmlBlock {
  final AppsList list;

  @Inject AppsBlock(AppsList list, ViewContext ctx) {
    super(ctx);
    this.list = list;
  }

  @Override public void render(Block html) {
    TBODY<TABLE<Hamlet>> tbody = html.
      table("#apps").
        thead().
          tr().
            th(".id", "ID").
            th(".user", "User").
            th(".name", "Name").
            th(".queue", "Queue").
            th(".state", "State").
            th(".progress", "Progress").
            th(".ui", "Tracking UI").
            th(".note", "Note")._()._().
        tbody();
    int i = 0;
    for (AppContext app : list.apps) {
      String appId = Apps.toString(app.getApplicationID());
      String trackingUrl = app.getMaster().getTrackingUrl();
      String ui = trackingUrl == null || trackingUrl.isEmpty() ? "UNASSIGNED" :
          (app.getFinishTime() == 0 ? "ApplicationMaster" : "JobHistory");
      String percent = String.format("%.1f", app.getStatus().getProgress() * 100);
      tbody.
        tr().
          td().
            br().$title(String.valueOf(app.getApplicationID().getId()))._(). // for sorting
            a(url("app", appId), appId)._().
          td(app.getUser().toString()).
          td(app.getName().toString()).
          td(app.getQueue().toString()).
          td(app.getMaster().getState().toString()).
          td().
            br().$title(percent)._(). // for sorting
            div(_PROGRESSBAR).
              $title(join(percent, '%')). // tooltip
              div(_PROGRESSBAR_VALUE).
                $style(join("width:", percent, '%'))._()._()._().
          td().
            a(trackingUrl == null ? "#" : join("http://", trackingUrl), ui)._().
          td(app.getMaster().getDiagnostics())._();
      if (list.rendering != Render.HTML && ++i >= 20) break;
    }
    tbody._()._();

    if (list.rendering == Render.JS_ARRAY) {
      echo("<script type='text/javascript'>\n",
           "var appsData=");
      list.toDataTableArrays(writer());
      echo("\n</script>\n");
    }
  }
}
