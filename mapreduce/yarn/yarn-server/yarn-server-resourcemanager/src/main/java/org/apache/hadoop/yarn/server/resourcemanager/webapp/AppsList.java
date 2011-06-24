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
import com.google.inject.servlet.RequestScoped;

import java.io.PrintWriter;
import java.util.List;

import static org.apache.commons.lang.StringEscapeUtils.*;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;
import static org.apache.hadoop.yarn.webapp.view.Jsons.*;

import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.AppContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationsManager;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.ToJSON;
import org.apache.hadoop.yarn.webapp.Controller.RequestContext;

// So we only need to do asm.getApplications once in a request
@RequestScoped
class AppsList implements ToJSON {
  final RequestContext rc;
  final List<AppContext> apps;
  Render rendering;

  @Inject AppsList(RequestContext ctx, ApplicationsManager asm) {
    rc = ctx;
    apps = asm.getAllAppContexts();
  }

  void toDataTableArrays(PrintWriter out) {
    out.append('[');
    boolean first = true;
    for (AppContext app : apps) {
      if (first) {
        first = false;
      } else {
        out.append(",\n");
      }
      String appID = Apps.toString(app.getApplicationID());
      String trackingUrl = app.getMaster().getTrackingUrl();
      String ui = trackingUrl == null ? "UNASSIGNED" :
          (app.getFinishTime() == 0 ? "ApplicationMaster" : "JobHistory");
      out.append("[\"");
      appendSortable(out, app.getApplicationID().getId());
      appendLink(out, appID, rc.prefix(), "app", appID).append(_SEP).
          append(escapeHtml(app.getUser().toString())).append(_SEP).
          append(escapeHtml(app.getName().toString())).append(_SEP).
          append(escapeHtml(app.getQueue())).append(_SEP).
          append(app.getMaster().getState().toString()).append(_SEP);
      appendProgressBar(out, app.getStatus().getProgress()).append(_SEP);
      appendLink(out, ui, rc.prefix(),
                 trackingUrl == null ? "#" : "http://", trackingUrl).
          append(_SEP).append(escapeJavaScript(escapeHtml(
                              app.getMaster().getDiagnostics()))).
          append("\"]");
    }
    out.append(']');
  }

  @Override
  public void toJSON(PrintWriter out) {
    out.print("{\"aaData\":");
    toDataTableArrays(out);
    out.print("}\n");
  }
}
